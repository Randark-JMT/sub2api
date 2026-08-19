package service

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/alitto/pond/v2"
)

// AccountWindowUsageRecorder 账号滚动窗口用量记录器（opt-in）。
//
// 对 accounts.window_tracking_enabled = true 的账号：
//   - 常规探测：每 baselineProbeInterval（±jitter）经渠道监控配额抓取链路
//     （ChannelMonitorQuotaFetcher，自带 TTL 缓存 + singleflight）采样一次
//     各滚动窗口的使用率/重置时间，更新开放行
//   - 决断探测：开放行临近 window_end（≤ decisiveProbeLead）时，绕过缓存
//     force 抓取（每窗口最多 decisiveProbeMaxCount 次、间隔
//     decisiveProbeRetryGap），确保拿到窗口关闭前的最终用量
//   - finalize：window_end 过后 finalizeGrace 仍未被新观测推进的开放行，
//     用 usage_logs 在 [window_start, window_end) 内聚合回填 token 明细并关闭
//
// 调度形态：单 goroutine 循环（recorderTickInterval）+ pond 池。与渠道监控的
// per-monitor goroutine 不同，所有启用账号共享统一节奏，决断探测的时机完全由
// 开放行的 window_end（DB 状态）推导，没有需要常驻的 per-account 定时器；
// 服务重启后从 DB 行恢复，无需额外状态。
//
// 多副本部署下每个副本都会运行本记录器：开放行的原子 upsert（GREATEST/单调
// 计数）与 finalize 的 finalized_at IS NULL 守卫使重复写入幂等，最坏情况是
// tick 重叠窗口内的上游查询翻倍——与渠道监控 runner 的取舍一致。
type AccountWindowUsageRecorder struct {
	windowRepo   AccountWindowUsageRepository
	usageLogRepo UsageLogRepository
	fetcher      windowQuotaSource

	pool         pond.Pool
	parentCtx    context.Context
	parentCancel context.CancelFunc

	wg      sync.WaitGroup
	started bool
	stopped bool
	mu      sync.Mutex

	// lastBaseline 每账号上次常规探测提交时间（内存态；重启后丢失仅导致
	// 多一次探测，上游由抓取器缓存去重）
	lastBaseline   map[int64]time.Time
	lastBaselineMu sync.Mutex

	// inFlight 正在探测的账号集合，避免同一账号的常规/决断探测并发交错
	inFlight   map[int64]struct{}
	inFlightMu sync.Mutex
}

// windowQuotaSource 记录器的配额来源（*ChannelMonitorQuotaFetcher 天然满足）。
// 独立窄接口便于单元测试注入 stub。
type windowQuotaSource interface {
	Fetch(ctx context.Context, accountID int64, force ...bool) *domain.MonitorQuotaSnapshot
}

// 记录器节奏与守卫常量。
const (
	// recorderTickInterval 循环粒度：驱动 finalize 扫描与两类探测的到期计算
	recorderTickInterval = 15 * time.Second
	// recorderBaselineInterval 常规探测节奏（每账号）
	recorderBaselineInterval = 15 * time.Minute
	// recorderBaselineJitter 常规探测的随机延迟上限，打散多账号并发
	recorderBaselineJitter = 2 * time.Minute
	// recorderDecisiveLead window_end 前多久触发决断探测
	recorderDecisiveLead = 2 * time.Minute
	// recorderDecisiveRetryGap 决断探测最小间隔。与 lead/maxCount=2 配合，
	// 两次探测约落在 T-120s 与 T-30s：间隔小于 lead-30s 会让第二次探测
	// 提前到期（如 45s 时两次都挤在窗口关闭前 ~72s 以上），丢失窗口末尾用量
	recorderDecisiveRetryGap = 90 * time.Second
	// recorderDecisiveMaxCount 每窗口决断探测次数上限
	recorderDecisiveMaxCount = 2
	// recorderFinalizeGrace window_end 过后的收敛等待（容忍迟到 usage_logs 写入）
	recorderFinalizeGrace = 5 * time.Minute
	// recorderWorkerConcurrency 探测任务池容量（约束上游并发）
	recorderWorkerConcurrency = 4
	// recorderRunOneTimeout 单次探测任务超时（抓取器自身另有 45s 上限）
	recorderRunOneTimeout = 60 * time.Second
	// recorderSweepLimit 单轮 finalize/探测扫描的行数上限
	recorderSweepLimit = 200
	// recorderResetEpsilon 两次观测的 reset_at 视为同一窗口的容差
	// （供应商时间戳存在秒级抖动）
	recorderResetEpsilon = 2 * time.Second
	// windowHistoryRetentionDays 已关闭窗口历史的保留天数
	windowHistoryRetentionDays = 90
	// windowStaleOpenRetentionDays 僵尸开放行的保留天数（账号软删/关追踪兜底）
	windowStaleOpenRetentionDays = 14
)

// NewAccountWindowUsageRecorder 构造记录器。参数取具体服务类型以便 wire 直连
// （fetcher 即 *ChannelMonitorQuotaFetcher）；单元测试在同包内用 struct 字面量注入 stub。
func NewAccountWindowUsageRecorder(
	windowRepo AccountWindowUsageRepository,
	usageLogRepo UsageLogRepository,
	fetcher *ChannelMonitorQuotaFetcher,
) *AccountWindowUsageRecorder {
	ctx, cancel := context.WithCancel(context.Background())
	return &AccountWindowUsageRecorder{
		windowRepo:   windowRepo,
		usageLogRepo: usageLogRepo,
		fetcher:      fetcher,
		pool:         pond.NewPool(recorderWorkerConcurrency),
		parentCtx:    ctx,
		parentCancel: cancel,
		lastBaseline: make(map[int64]time.Time),
		inFlight:     make(map[int64]struct{}),
	}
}

// Start 启动记录器循环。调用方需保证只调一次（wire provider 内调用）。
func (r *AccountWindowUsageRecorder) Start() {
	if r == nil || r.windowRepo == nil || r.fetcher == nil {
		return
	}
	r.mu.Lock()
	if r.started || r.stopped {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()

	r.wg.Add(1)
	go r.runLoop()
	slog.Info("account_window_usage: recorder started")
}

// Stop 优雅停止：取消循环并等待在飞任务结束。
func (r *AccountWindowUsageRecorder) Stop() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.parentCancel()
	r.mu.Unlock()

	r.wg.Wait()
	r.pool.StopAndWait()
}

// RunDailyMaintenance 每日维护：保留期清理（OpsCleanupService cron 驱动，
// 复用 leader lock）。
func (r *AccountWindowUsageRecorder) RunDailyMaintenance(ctx context.Context) {
	if r == nil || r.windowRepo == nil {
		return
	}

	finalizedCutoff := time.Now().AddDate(0, 0, -windowHistoryRetentionDays)
	if deleted, err := r.windowRepo.PruneFinalizedBefore(ctx, finalizedCutoff); err != nil {
		slog.Warn("account_window_usage: prune finalized failed", "error", err)
	} else if deleted > 0 {
		slog.Info("account_window_usage: pruned finalized rows", "deleted", deleted)
	}

	staleCutoff := time.Now().AddDate(0, 0, -windowStaleOpenRetentionDays)
	if deleted, err := r.windowRepo.PruneStaleOpenBefore(ctx, staleCutoff); err != nil {
		slog.Warn("account_window_usage: prune stale open rows failed", "error", err)
	} else if deleted > 0 {
		slog.Info("account_window_usage: pruned stale open rows", "deleted", deleted)
	}
}

// runLoop 主循环：每 tick 先做 finalize 扫描，再为到期账号提交常规/决断探测。
func (r *AccountWindowUsageRecorder) runLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(recorderTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.parentCtx.Done():
			return
		case <-ticker.C:
			r.runOnce(r.parentCtx)
		}
	}
}

// runOnce 单轮调度。errors 只记日志：单轮失败不影响下一轮。
func (r *AccountWindowUsageRecorder) runOnce(ctx context.Context) {
	now := time.Now()

	r.finalizeExpired(ctx, now)
	r.scheduleBaselineProbes(ctx, now)
	r.scheduleDecisiveProbes(ctx, now)
}

// finalizeExpired 关闭已过期（window_end + grace 已过）的开放行并回填 token 明细。
// 不限启用账号：中途关闭追踪的账号也要把遗留行收尾。
func (r *AccountWindowUsageRecorder) finalizeExpired(ctx context.Context, now time.Time) {
	cutoff := now.Add(-recorderFinalizeGrace)
	rows, err := r.windowRepo.ListExpiredOpenWindows(ctx, cutoff, recorderSweepLimit)
	if err != nil {
		slog.Warn("account_window_usage: list expired open windows failed", "error", err)
		return
	}
	for _, rec := range rows {
		if _, err := r.finalizeRecord(ctx, rec, now); err != nil {
			slog.Warn("account_window_usage: finalize window failed",
				"account_id", rec.AccountID, "window_type", rec.WindowType, "error", err)
		}
	}
}

// finalizeRecord 聚合 [window_start, window_end) 的本地用量并幂等关闭窗口。
func (r *AccountWindowUsageRecorder) finalizeRecord(ctx context.Context, rec *AccountWindowUsageRecord, now time.Time) (bool, error) {
	stats, err := r.usageLogRepo.GetAccountWindowStatsRange(ctx, rec.AccountID, rec.WindowStart, rec.WindowEnd)
	if err != nil {
		return false, err
	}
	return r.windowRepo.FinalizeWindow(ctx, rec.ID, stats, now)
}

// scheduleBaselineProbes 为启用账号按节奏提交常规探测。
func (r *AccountWindowUsageRecorder) scheduleBaselineProbes(ctx context.Context, now time.Time) {
	accountIDs, err := r.windowRepo.ListWindowTrackingEnabled(ctx)
	if err != nil {
		slog.Warn("account_window_usage: list tracking enabled accounts failed", "error", err)
		return
	}

	// 常规探测的到期判定在提交时做（含 jitter），提交成功即记录时间戳
	for _, accountID := range accountIDs {
		if !r.baselineDue(accountID, now) {
			continue
		}
		if !r.tryAcquireInFlight(accountID) {
			continue
		}
		if _, ok := r.pool.TrySubmit(func() {
			r.probeAccount(accountID, false)
		}); !ok {
			r.releaseInFlight(accountID)
			slog.Warn("account_window_usage: worker pool full, skip baseline probe",
				"account_id", accountID)
			continue
		}
		r.markBaseline(accountID, now)
	}
}

// baselineDue 判定账号常规探测是否到期。阈值带每次评估随机化的抖动
// （interval ~ interval+jitter 之间的随机期限），用于打散多账号的并发节奏。
func (r *AccountWindowUsageRecorder) baselineDue(accountID int64, now time.Time) bool {
	r.lastBaselineMu.Lock()
	last, ok := r.lastBaseline[accountID]
	r.lastBaselineMu.Unlock()
	if !ok {
		return true
	}
	jitter := time.Duration(rand.Int64N(int64(recorderBaselineJitter) + 1))
	return now.Sub(last) >= recorderBaselineInterval+jitter
}

// markBaseline 记录账号常规探测的提交时间。
func (r *AccountWindowUsageRecorder) markBaseline(accountID int64, now time.Time) {
	r.lastBaselineMu.Lock()
	r.lastBaseline[accountID] = now
	r.lastBaselineMu.Unlock()
}

// scheduleDecisiveProbes 为临近 window_end 的开放行提交决断（force）探测。
//
// 到期行按账号分组：单次 force 抓取返回账号全部窗口 tier，一组一次上游调用
// 即覆盖组内所有行。认领（次数上限 + 最小间隔）在任务内进行——只有真正会
// 执行的探测才消耗决断预算，提交失败/inFlight 冲突不再空烧认领次数。
func (r *AccountWindowUsageRecorder) scheduleDecisiveProbes(ctx context.Context, now time.Time) {
	rows, err := r.windowRepo.ListWindowsDueForDecisiveProbe(
		ctx, now, recorderDecisiveLead, recorderDecisiveRetryGap, recorderDecisiveMaxCount, recorderSweepLimit)
	if err != nil {
		slog.Warn("account_window_usage: list windows due for decisive probe failed", "error", err)
		return
	}

	groups := make(map[int64][]*AccountWindowUsageRecord)
	order := make([]int64, 0, len(rows))
	for _, rec := range rows {
		if _, seen := groups[rec.AccountID]; !seen {
			order = append(order, rec.AccountID)
		}
		groups[rec.AccountID] = append(groups[rec.AccountID], rec)
	}

	for _, accountID := range order {
		groupRows := groups[accountID]
		if !r.tryAcquireInFlight(accountID) {
			continue
		}
		if _, ok := r.pool.TrySubmit(func() {
			r.probeDecisive(groupRows)
		}); !ok {
			r.releaseInFlight(accountID)
			slog.Warn("account_window_usage: worker pool full, skip decisive probe",
				"account_id", accountID)
		}
	}
}

// probeDecisive 决断探测任务：逐行认领（任一行认领失败仅跳过该行），组内
// 至少一行认领成功才执行 force 抓取。
func (r *AccountWindowUsageRecorder) probeDecisive(rows []*AccountWindowUsageRecord) {
	accountID := rows[0].AccountID

	ctx, cancel := context.WithTimeout(r.parentCtx, recorderRunOneTimeout)
	defer cancel()

	defer r.releaseInFlight(accountID)

	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("account_window_usage: decisive probe panic", "account_id", accountID, "panic", rec)
		}
	}()

	claimed := false
	for _, rec := range rows {
		ok, err := r.windowRepo.ClaimDecisiveProbe(ctx, rec.ID, time.Now(), recorderDecisiveRetryGap, recorderDecisiveMaxCount)
		if err != nil {
			slog.Warn("account_window_usage: claim decisive probe failed",
				"account_id", rec.AccountID, "window_type", rec.WindowType, "error", err)
			continue
		}
		if ok {
			claimed = true
		}
	}
	if !claimed {
		return
	}

	r.fetchAndApply(ctx, accountID, true)
}

// probeAccount 执行单次常规探测并应用快照。
func (r *AccountWindowUsageRecorder) probeAccount(accountID int64, force bool) {
	// 探测 ctx 挂靠 parentCtx：Stop 取消时在飞探测立即中止，不拖慢优雅停机
	ctx, cancel := context.WithTimeout(r.parentCtx, recorderRunOneTimeout)
	defer cancel()

	defer r.releaseInFlight(accountID)

	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("account_window_usage: probe panic", "account_id", accountID, "panic", rec)
		}
	}()

	r.fetchAndApply(ctx, accountID, force)
}

// fetchAndApply 抓取账号配额快照并应用。force=true 用于决断探测（绕过
// 抓取器缓存）；决断失败留痕，便于排查窗口末尾用量缺失。
func (r *AccountWindowUsageRecorder) fetchAndApply(ctx context.Context, accountID int64, force bool) {
	snapshot := r.fetcher.Fetch(ctx, accountID, force)
	if snapshot == nil {
		return
	}
	if force && !snapshot.Success {
		slog.Warn("account_window_usage: decisive probe unsuccessful",
			"account_id", accountID, "error", snapshot.Error)
	}
	if err := r.ApplySnapshot(ctx, accountID, snapshot); err != nil {
		slog.Warn("account_window_usage: apply snapshot failed", "account_id", accountID, "error", err)
	}
}

// ApplySnapshot 把一次配额快照的各窗口 tier 合并进开放行（状态机核心，
// 独立导出便于单元测试与未来的被动采样挂点复用）。
//
// 单个 tier 的迁移：
//
//	无开放行                → 插入（start = reset - duration）
//	|reset - windowEnd| ≤ ε → 同窗口：peak=max(peak, used%)、last=used%、计数+1
//	reset 前移 && 旧 end>now → 滚动窗口滑动：更新指标 + 重算 start/end
//	旧 windowEnd ≤ now      → 旧窗口关闭（回填 token）+ 插入新窗口
//	reset 后移（上游抖动）   → 只更新指标，绝不回退 window_end
func (r *AccountWindowUsageRecorder) ApplySnapshot(ctx context.Context, accountID int64, snapshot *domain.MonitorQuotaSnapshot) error {
	if snapshot == nil || !snapshot.Success {
		return nil
	}
	now := time.Now()

	// 单个 tier 失败不阻断其余窗口：记录首个错误，处理完所有 tier 后返回
	var firstErr error
	for _, tier := range snapshot.Tiers {
		if err := r.applyTier(ctx, accountID, tier, now); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// applyTier 处理单个窗口 tier 的状态迁移。
func (r *AccountWindowUsageRecorder) applyTier(ctx context.Context, accountID int64, tier domain.MonitorQuotaTier, now time.Time) error {
	if !recordedWindow(tier.Window) || tier.ResetAt == "" {
		return nil
	}
	resetAt, err := time.Parse(time.RFC3339, tier.ResetAt)
	if err != nil {
		return nil // 未知格式的时间戳跳过，不阻断其他 tier
	}
	windowType := tier.Window
	duration := windowTypeDuration[windowType]

	open, err := r.windowRepo.GetOpenWindow(ctx, accountID, windowType)
	if err != nil {
		return err
	}

	// 构造本次采样的指标增量（sample_count 语义：插入时为 1，合并时累加 1）
	buildRow := func(start, end time.Time) *AccountWindowUsageRecord {
		return &AccountWindowUsageRecord{
			AccountID:       accountID,
			WindowType:      windowType,
			WindowStart:     start,
			WindowEnd:       end,
			PeakUsedPercent: tier.UsedPercent,
			LastUsedPercent: tier.UsedPercent,
			UsedAbsolute:    absPtr(tier.Used),
			LimitAbsolute:   absPtr(tier.Limit),
			SampleCount:     1,
		}
	}

	// 分支 1：无开放行 → 直接插入。reset_at 已过的快照是陈旧数据（或旧行
	// 已被并发 finalize）：此时新开的行 window_end 在过去，finalize 扫描会
	// 再关一次，产生同一窗口的重复历史行——仅容忍秒级时钟偏差
	if open == nil {
		if !resetAt.After(now.Add(-recorderResetEpsilon)) {
			return nil
		}
		return r.windowRepo.UpsertOpenWindow(ctx, buildRow(resetAt.Add(-duration), resetAt))
	}

	sameWindow := resetAt.Sub(open.WindowEnd) <= recorderResetEpsilon &&
		resetAt.Sub(open.WindowEnd) >= -recorderResetEpsilon
	windowExpired := !open.WindowEnd.After(now)

	switch {
	// 分支 2：同一窗口（reset 抖动在容差内）→ 合并指标
	case sameWindow:
		return r.windowRepo.UpsertOpenWindow(ctx, buildRow(open.WindowStart, open.WindowEnd))

	// 分支 3：旧窗口已过期 → 关闭旧行（回填 token）+ 写入新窗口行
	case windowExpired:
		stats, err := r.usageLogRepo.GetAccountWindowStatsRange(ctx, accountID, open.WindowStart, open.WindowEnd)
		if err != nil {
			return err
		}
		return r.windowRepo.ReplaceOpenWindow(ctx, open.ID, stats, buildRow(resetAt.Add(-duration), resetAt), now)

	// 分支 4：reset 前移且旧 end 仍在未来 → 滚动窗口滑动，整体前移
	case resetAt.After(open.WindowEnd):
		return r.windowRepo.UpsertOpenWindow(ctx, buildRow(resetAt.Add(-duration), resetAt))

	// 分支 5：reset 后移（上游抖动）→ 只更新指标，保留原窗口边界
	default:
		return r.windowRepo.UpsertOpenWindow(ctx, buildRow(open.WindowStart, open.WindowEnd))
	}
}

// absPtr 把 tier 的绝对用量/限额转为可选指针（0 视为未上报）。
func absPtr(v float64) *float64 {
	if v <= 0 {
		return nil
	}
	return &v
}

func (r *AccountWindowUsageRecorder) tryAcquireInFlight(accountID int64) bool {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()
	if _, exists := r.inFlight[accountID]; exists {
		return false
	}
	r.inFlight[accountID] = struct{}{}
	return true
}

func (r *AccountWindowUsageRecorder) releaseInFlight(accountID int64) {
	r.inFlightMu.Lock()
	delete(r.inFlight, accountID)
	r.inFlightMu.Unlock()
}
