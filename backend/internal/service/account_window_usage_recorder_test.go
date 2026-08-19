//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/alitto/pond/v2"
	"github.com/stretchr/testify/require"
)

// --- 记录器依赖 stub ---

// windowKey 开放行的内存索引键。
func windowKey(accountID int64, windowType string) string {
	return fmt.Sprintf("%d|%s", accountID, windowType)
}

// stubWindowUsageRepo 内存版仓储，模拟 SQL 层的合并/守卫语义
// （GREATEST、单调计数、finalized_at IS NULL 守卫、条件认领）。
type stubWindowUsageRepo struct {
	AccountWindowUsageRepository

	mu        sync.Mutex
	open      map[string]*AccountWindowUsageRecord
	finalized []*AccountWindowUsageRecord
	nextID    int64

	// 行为注入
	trackingEnabled []int64
	dueRows         []*AccountWindowUsageRecord
	claimResults    map[int64]bool // id → 认领结果（缺省 true）

	// 调用观测
	upsertCalls        int
	replaceCalls       int
	finalizeCalls      int
	lastUpsertRow      *AccountWindowUsageRecord
	lastReplaceStats   *usagestats.WindowTokenStats
	lastReplaceOldRow  int64
	pruneFinalizedAt   time.Time
	pruneStaleOpenFrom time.Time
}

func newStubWindowUsageRepo() *stubWindowUsageRepo {
	return &stubWindowUsageRepo{
		open:         make(map[string]*AccountWindowUsageRecord),
		claimResults: make(map[int64]bool),
	}
}

func (r *stubWindowUsageRepo) GetOpenWindow(_ context.Context, accountID int64, windowType string) (*AccountWindowUsageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if row, ok := r.open[windowKey(accountID, windowType)]; ok {
		cp := *row
		return &cp, nil
	}
	return nil, nil
}

// UpsertOpenWindow 复刻 SQL 合并语义：peak 取 GREATEST、last 覆盖、绝对值
// 保留最后非空、sample_count 累加、window_end 只前移不回退。
func (r *stubWindowUsageRepo) UpsertOpenWindow(_ context.Context, row *AccountWindowUsageRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upsertCalls++
	cp := *row
	r.lastUpsertRow = &cp

	key := windowKey(row.AccountID, row.WindowType)
	if existing, ok := r.open[key]; !ok {
		r.nextID++
		cp.ID = r.nextID
		r.open[key] = &cp
	} else {
		if row.PeakUsedPercent > existing.PeakUsedPercent {
			existing.PeakUsedPercent = row.PeakUsedPercent
		}
		existing.LastUsedPercent = row.LastUsedPercent
		existing.SampleCount += row.SampleCount
		if row.WindowEnd.After(existing.WindowEnd) {
			existing.WindowStart = row.WindowStart
			existing.WindowEnd = row.WindowEnd
			// 决断预算跟随窗口实例（对齐 SQL CASE 语义）
			existing.DecisiveProbeCount = 0
			existing.LastProbeAt = nil
		}
	}
	return nil
}

func (r *stubWindowUsageRepo) FinalizeWindow(_ context.Context, id int64, stats *usagestats.WindowTokenStats, now time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finalizeCalls++
	for key, row := range r.open {
		if row.ID != id {
			continue
		}
		delete(r.open, key)
		applyFinalize(row, stats, now)
		r.finalized = append(r.finalized, row)
		return true, nil
	}
	return false, nil // 幂等守卫：已关闭/不存在 no-op
}

func (r *stubWindowUsageRepo) ReplaceOpenWindow(ctx context.Context, oldID int64, stats *usagestats.WindowTokenStats, newRow *AccountWindowUsageRecord, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replaceCalls++
	r.lastReplaceStats = stats
	r.lastReplaceOldRow = oldID

	for key, row := range r.open {
		if row.ID == oldID {
			delete(r.open, key)
			applyFinalize(row, stats, now)
			r.finalized = append(r.finalized, row)
			break
		}
	}
	// 复用 upsert 合并逻辑写入新行（去掉锁重入，手动内联）
	key := windowKey(newRow.AccountID, newRow.WindowType)
	if existing, ok := r.open[key]; !ok {
		cp := *newRow
		r.nextID++
		cp.ID = r.nextID
		r.open[key] = &cp
	} else {
		if newRow.PeakUsedPercent > existing.PeakUsedPercent {
			existing.PeakUsedPercent = newRow.PeakUsedPercent
		}
		existing.LastUsedPercent = newRow.LastUsedPercent
		existing.SampleCount += newRow.SampleCount
		if newRow.WindowEnd.After(existing.WindowEnd) {
			existing.WindowStart = newRow.WindowStart
			existing.WindowEnd = newRow.WindowEnd
		}
	}
	return nil
}

func applyFinalize(row *AccountWindowUsageRecord, stats *usagestats.WindowTokenStats, now time.Time) {
	if stats == nil {
		stats = &usagestats.WindowTokenStats{}
	}
	row.Requests = &stats.Requests
	row.TokensTotal = &stats.TokensTotal
	row.TokensInput = &stats.TokensInput
	row.TokensOutput = &stats.TokensOutput
	row.TokensCacheCreation = &stats.TokensCacheCreation
	row.TokensCacheRead = &stats.TokensCacheRead
	row.FinalizedAt = &now
}

func (r *stubWindowUsageRepo) ListExpiredOpenWindows(_ context.Context, cutoff time.Time, _ int) ([]*AccountWindowUsageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := make([]*AccountWindowUsageRecord, 0)
	for _, row := range r.open {
		if row.WindowEnd.Before(cutoff) {
			cp := *row
			rows = append(rows, &cp)
		}
	}
	return rows, nil
}

func (r *stubWindowUsageRepo) ListWindowsDueForDecisiveProbe(_ context.Context, now time.Time, _, _ time.Duration, _, _ int) ([]*AccountWindowUsageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := make([]*AccountWindowUsageRecord, 0, len(r.dueRows))
	for _, row := range r.dueRows {
		cp := *row
		rows = append(rows, &cp)
	}
	return rows, nil
}

func (r *stubWindowUsageRepo) ClaimDecisiveProbe(_ context.Context, id int64, _ time.Time, _ time.Duration, _ int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if result, ok := r.claimResults[id]; ok {
		return result, nil
	}
	return true, nil
}

func (r *stubWindowUsageRepo) ListHistorySince(_ context.Context, accountID int64, since time.Time) ([]*AccountWindowUsageRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	records := make([]*AccountWindowUsageRecord, 0)
	for _, row := range r.open {
		if row.AccountID == accountID && !row.WindowEnd.Before(since) {
			cp := *row
			records = append(records, &cp)
		}
	}
	records = append(records, r.finalized...)
	return records, nil
}

func (r *stubWindowUsageRepo) PruneFinalizedBefore(_ context.Context, cutoff time.Time) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneFinalizedAt = cutoff
	return 0, nil
}

func (r *stubWindowUsageRepo) PruneStaleOpenBefore(_ context.Context, cutoff time.Time) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneStaleOpenFrom = cutoff
	return 0, nil
}

func (r *stubWindowUsageRepo) ListWindowTrackingEnabled(_ context.Context) ([]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.trackingEnabled, nil
}

// openRow 快照当前开放行（测试断言用）。
func (r *stubWindowUsageRepo) openRow(accountID int64, windowType string) *AccountWindowUsageRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	if row, ok := r.open[windowKey(accountID, windowType)]; ok {
		cp := *row
		return &cp
	}
	return nil
}

// stubWindowUsageLogRepo 记录聚合请求并返回可配置统计。
type stubWindowUsageLogRepo struct {
	UsageLogRepository

	mu         sync.Mutex
	rangeCalls int
	lastStart  time.Time
	lastEnd    time.Time
	rangeStats *usagestats.WindowTokenStats
	rangeErr   error
}

func (s *stubWindowUsageLogRepo) GetAccountWindowStatsRange(_ context.Context, _ int64, start, end time.Time) (*usagestats.WindowTokenStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rangeCalls++
	s.lastStart = start
	s.lastEnd = end
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	if s.rangeStats != nil {
		return s.rangeStats, nil
	}
	return &usagestats.WindowTokenStats{}, nil
}

// stubWindowQuotaSource 记录 force 参数的配额源 stub。
type stubWindowQuotaSource struct {
	mu     sync.Mutex
	snap   *domain.MonitorQuotaSnapshot
	calls  int
	forces []bool
}

func (s *stubWindowQuotaSource) Fetch(_ context.Context, _ int64, force ...bool) *domain.MonitorQuotaSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.forces = append(s.forces, len(force) > 0 && force[0])
	return s.snap
}

func (s *stubWindowQuotaSource) snapshot() (int, []bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, append([]bool(nil), s.forces...)
}

// newRecorderForTest 构造仅填充状态机所需字段的记录器（跳过 pool/生命周期）。
func newRecorderForTest(windowRepo AccountWindowUsageRepository, usageLogRepo UsageLogRepository, fetcher windowQuotaSource) *AccountWindowUsageRecorder {
	ctx, cancel := context.WithCancel(context.Background())
	return &AccountWindowUsageRecorder{
		windowRepo:   windowRepo,
		usageLogRepo: usageLogRepo,
		fetcher:      fetcher,
		parentCtx:    ctx,
		parentCancel: cancel,
		lastBaseline: make(map[int64]time.Time),
		inFlight:     make(map[int64]struct{}),
	}
}

// newRunningRecorderForTest 构造带工作池的记录器（探测调度测试用），
// 返回值之二用于测试结束后停池。
func newRunningRecorderForTest(windowRepo *stubWindowUsageRepo, usageLogRepo *stubWindowUsageLogRepo, fetcher *stubWindowQuotaSource) (*AccountWindowUsageRecorder, func()) {
	r := newRecorderForTest(windowRepo, usageLogRepo, fetcher)
	pool := pond.NewPool(recorderWorkerConcurrency)
	r.pool = pool
	return r, func() {
		r.parentCancel()
		pool.StopAndWait()
	}
}

// --- ApplySnapshot 状态机 ---

func tier5h(used float64, resetAt time.Time) domain.MonitorQuotaTier {
	return domain.MonitorQuotaTier{Window: "5h", UsedPercent: used, ResetAt: resetAt.UTC().Format(time.RFC3339)}
}

func snapshotWithTiers(tiers ...domain.MonitorQuotaTier) *domain.MonitorQuotaSnapshot {
	// FetchedAt 必须是当前时刻：陈旧快照守卫会丢弃 fetchedAt 早于开放行
	// window_start 的观测（多副本防污染），零值会被整批拒绝
	return &domain.MonitorQuotaSnapshot{Success: true, Tiers: tiers, FetchedAt: time.Now()}
}

func TestRecorder_ApplySnapshot_FirstInsertCreatesOpenRow(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	resetAt := time.Now().Add(4 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(42.5, resetAt))))

	row := repo.openRow(7, "5h")
	require.NotNil(t, row)
	require.Equal(t, 1, row.SampleCount)
	require.InDelta(t, 42.5, row.PeakUsedPercent, 0.001)
	require.InDelta(t, 42.5, row.LastUsedPercent, 0.001)
	require.Equal(t, resetAt, row.WindowEnd)
	require.Equal(t, resetAt.Add(-5*time.Hour), row.WindowStart)
	require.Nil(t, row.FinalizedAt, "fresh row must stay open")
}

func TestRecorder_ApplySnapshot_SameWindowMergesMetrics(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	resetAt := time.Now().Add(4 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(30, resetAt))))
	// 同一窗口（reset 抖动 1 秒内）的第二次采样
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(55, resetAt.Add(1*time.Second)))))

	row := repo.openRow(7, "5h")
	require.NotNil(t, row)
	require.Equal(t, 2, row.SampleCount)
	require.InDelta(t, 55.0, row.PeakUsedPercent, 0.001, "peak should keep the max sample")
	require.InDelta(t, 55.0, row.LastUsedPercent, 0.001)
	require.Equal(t, resetAt, row.WindowEnd, "same-window jitter must not move bounds")
}

func TestRecorder_ApplySnapshot_SlidingWindowMovesBounds(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	oldReset := time.Now().Add(1 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(30, oldReset))))

	// reset 前移（滚动窗口滑动），旧 end 仍在未来 → 边界整体前移
	newReset := oldReset.Add(30 * time.Minute)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(35, newReset))))

	row := repo.openRow(7, "5h")
	require.NotNil(t, row)
	require.Equal(t, newReset, row.WindowEnd)
	require.Equal(t, newReset.Add(-5*time.Hour), row.WindowStart)
	require.Equal(t, 2, row.SampleCount)
}

func TestRecorder_ApplySnapshot_ExpiredWindowFinalizesAndReopens(t *testing.T) {
	repo := newStubWindowUsageRepo()
	usageRepo := &stubWindowUsageLogRepo{
		rangeStats: &usagestats.WindowTokenStats{Requests: 12, TokensTotal: 34567},
	}
	r := newRecorderForTest(repo, usageRepo, &stubWindowQuotaSource{})

	// 预置一个已过期的开放行（window_end 在过去）
	expiredEnd := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	expiredStart := expiredEnd.Add(-5 * time.Hour)
	require.NoError(t, repo.UpsertOpenWindow(context.Background(), &AccountWindowUsageRecord{
		AccountID: 7, WindowType: "5h",
		WindowStart: expiredStart, WindowEnd: expiredEnd,
		PeakUsedPercent: 80, LastUsedPercent: 78, SampleCount: 3,
	}))
	oldRow := repo.openRow(7, "5h")

	// 新窗口观测（reset 在未来）→ 关闭旧行 + 开新行
	newReset := time.Now().Add(5 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(5, newReset))))

	require.Equal(t, 1, repo.replaceCalls)
	require.Equal(t, oldRow.ID, repo.lastReplaceOldRow, "expired row should be replaced")
	require.Equal(t, int64(12), repo.lastReplaceStats.Requests)
	require.Equal(t, int64(34567), repo.lastReplaceStats.TokensTotal)
	require.Equal(t, 1, usageRepo.rangeCalls)
	require.Equal(t, expiredStart, usageRepo.lastStart, "token aggregation must use old window bounds")
	require.Equal(t, expiredEnd, usageRepo.lastEnd)

	// 旧行已关闭且带 token 明细
	require.Len(t, repo.finalized, 1)
	require.NotNil(t, repo.finalized[0].FinalizedAt)
	require.NotNil(t, repo.finalized[0].TokensTotal)
	require.Equal(t, int64(34567), *repo.finalized[0].TokensTotal)

	// 新开放行边界正确
	newRow := repo.openRow(7, "5h")
	require.NotNil(t, newRow)
	require.Equal(t, newReset, newRow.WindowEnd)
	require.Nil(t, newRow.FinalizedAt)
}

func TestRecorder_ApplySnapshot_ResetJitterBackwardNeverRegresses(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	forwardReset := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(30, forwardReset))))

	// reset 后移（上游抖动）→ 只更新指标，绝不回退 window_end。
	// 同时断言传给仓储的行边界：仓储的 SQL 合并（GREATEST）本身也能兜住
	// 回退，这里确保记录器就没把回退值传下去（防御纵深可被检测）
	backwardReset := forwardReset.Add(-20 * time.Minute)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(40, backwardReset))))

	require.NotNil(t, repo.lastUpsertRow)
	require.Equal(t, forwardReset, repo.lastUpsertRow.WindowEnd, "recorder must pass the existing bounds on backward jitter")

	row := repo.openRow(7, "5h")
	require.NotNil(t, row)
	require.Equal(t, forwardReset, row.WindowEnd, "window_end must never regress")
	require.Equal(t, forwardReset.Add(-5*time.Hour), row.WindowStart)
	require.Equal(t, 2, row.SampleCount)
	require.InDelta(t, 40.0, row.LastUsedPercent, 0.001)
}

func TestRecorder_ApplySnapshot_FiltersNonRecordedAndInvalidTiers(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	reset := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	snapshot := snapshotWithTiers(
		domain.MonitorQuotaTier{Window: "5h", UsedPercent: 50, ResetAt: reset},
		domain.MonitorQuotaTier{Window: "daily", UsedPercent: 50, ResetAt: reset},         // 不记录
		domain.MonitorQuotaTier{Window: "30d", UsedPercent: 50, ResetAt: reset},           // 不记录
		domain.MonitorQuotaTier{Window: "7d", UsedPercent: 60},                            // 无 ResetAt
		domain.MonitorQuotaTier{Window: "weekly", UsedPercent: 70, ResetAt: "not-a-time"}, // 非法时间戳
	)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshot))

	require.NotNil(t, repo.openRow(7, "5h"))
	require.Nil(t, repo.openRow(7, "daily"))
	require.Nil(t, repo.openRow(7, "30d"))
	require.Nil(t, repo.openRow(7, "7d"))
	require.Nil(t, repo.openRow(7, "weekly"))

	// 失败快照整体跳过
	failed := &domain.MonitorQuotaSnapshot{Success: false, Tiers: []domain.MonitorQuotaTier{tier5h(10, time.Now().Add(time.Hour))}}
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, failed))
	require.Nil(t, r.ApplySnapshot(context.Background(), 7, nil))
	require.Equal(t, 1, repo.upsertCalls)
}

// 陈旧快照（reset_at 已过）且无开放行 → 不得新开行：旧行已被并发 finalize
// 或数据本身滞后，此时插入会产生 window_end 在过去的开放行，finalize 扫描
// 会再关一次，形成同一窗口的重复历史
func TestRecorder_ApplySnapshot_StalePastResetDoesNotOpenRow(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	staleReset := time.Now().Add(-10 * time.Minute)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(50, staleReset))))

	require.Equal(t, 0, repo.upsertCalls)
	require.Nil(t, repo.openRow(7, "5h"))

	// 仅秒级偏差（时钟抖动容差内）仍应正常开行
	skewedReset := time.Now().Add(-1 * time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(50, skewedReset))))
	require.Equal(t, 1, repo.upsertCalls)
	require.NotNil(t, repo.openRow(7, "5h"))
}

// 多副本陈旧快照守卫：其他副本已把窗口推进到新实例（window_start 前移到
// 边界 T），本副本进程内缓存仍持有 T 之前抓的旧窗口观测（reset_at 落在
// 新行 window_end 之前 → 判为 reset 后移）。不守卫的话上一窗口的峰值会经
// GREATEST 永久写进新窗口。fetchedAt 早于新行 window_start → 整 tier 丢弃。
func TestRecorder_ApplySnapshot_StaleFetchedAtDoesNotMergeIntoNewWindow(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	// 副本 A：新窗口已开启（边界在前方 3h，起点 = 边界 - 5h ≈ 2h 前）
	newReset := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(20, newReset))))
	callsAfterOpen := repo.upsertCalls

	// 副本 B：陈旧快照（reset_at 属于上一窗口 → 相对新行是「后移」路径），
	// 抓取时间早于新行 window_start → 必须被丢弃，不产生任何写入
	staleSnapshot := &domain.MonitorQuotaSnapshot{
		Success:   true,
		Tiers:     []domain.MonitorQuotaTier{tier5h(98, newReset.Add(-5*time.Hour))},
		FetchedAt: newReset.Add(-5 * time.Hour).Add(-2 * time.Minute), // 上一窗口期间抓取
	}
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, staleSnapshot))
	require.Equal(t, callsAfterOpen, repo.upsertCalls, "stale snapshot must be dropped, not merged")

	row := repo.openRow(7, "5h")
	require.NotNil(t, row)
	require.InDelta(t, 20.0, row.PeakUsedPercent, 0.001, "peak must not inherit the previous window's 98%")
	require.Equal(t, 1, row.SampleCount)
}

func TestRecorder_ApplySnapshot_MultipleTiersIndependent(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	reset := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	weeklyReset := time.Now().Add(72 * time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(
		tier5h(50, reset),
		domain.MonitorQuotaTier{Window: "weekly", UsedPercent: 20, ResetAt: weeklyReset.Format(time.RFC3339)},
	)))

	fiveHour := repo.openRow(7, "5h")
	weekly := repo.openRow(7, "weekly")
	require.NotNil(t, fiveHour)
	require.NotNil(t, weekly)
	require.Equal(t, reset, fiveHour.WindowEnd)
	require.Equal(t, weeklyReset, weekly.WindowEnd)
	require.Equal(t, weeklyReset.Add(-7*24*time.Hour), weekly.WindowStart)
}

// --- finalize 扫描 ---

func TestRecorder_FinalizeExpiredAggregatesLocalUsage(t *testing.T) {
	repo := newStubWindowUsageRepo()
	usageRepo := &stubWindowUsageLogRepo{
		rangeStats: &usagestats.WindowTokenStats{Requests: 5, TokensTotal: 1000, TokensInput: 600, TokensOutput: 400},
	}
	r := newRecorderForTest(repo, usageRepo, &stubWindowQuotaSource{})

	// 过期超过 grace 的开放行
	end := time.Now().Add(-(recorderFinalizeGrace + time.Minute))
	require.NoError(t, repo.UpsertOpenWindow(context.Background(), &AccountWindowUsageRecord{
		AccountID: 7, WindowType: "5h",
		WindowStart: end.Add(-5 * time.Hour), WindowEnd: end,
		PeakUsedPercent: 90, LastUsedPercent: 88, SampleCount: 4,
	}))
	// 未过 grace 的行（window_end 刚过）→ 不应被 finalize
	freshEnd := time.Now().Add(-time.Minute)
	require.NoError(t, repo.UpsertOpenWindow(context.Background(), &AccountWindowUsageRecord{
		AccountID: 8, WindowType: "5h",
		WindowStart: freshEnd.Add(-5 * time.Hour), WindowEnd: freshEnd,
		PeakUsedPercent: 10, SampleCount: 1,
	}))

	r.finalizeExpired(context.Background(), time.Now())

	require.Equal(t, 1, repo.finalizeCalls)
	require.Len(t, repo.finalized, 1)
	require.Equal(t, int64(7), repo.finalized[0].AccountID)
	require.NotNil(t, repo.finalized[0].TokensTotal)
	require.Equal(t, int64(1000), *repo.finalized[0].TokensTotal)
	require.Equal(t, end.Add(-5*time.Hour), usageRepo.lastStart)
	require.Equal(t, end, usageRepo.lastEnd)
	require.NotNil(t, repo.openRow(8, "5h"), "row within grace must stay open")
}

// --- 探测调度 ---

func TestRecorder_ScheduleBaselineProbesSubmitsNonForceFetch(t *testing.T) {
	repo := newStubWindowUsageRepo()
	repo.trackingEnabled = []int64{7}
	fetcher := &stubWindowQuotaSource{}
	r, stop := newRunningRecorderForTest(repo, &stubWindowUsageLogRepo{}, fetcher)
	defer stop()

	r.scheduleBaselineProbes(context.Background(), time.Now())

	require.Eventually(t, func() bool {
		calls, _ := fetcher.snapshot()
		return calls == 1
	}, 5*time.Second, 20*time.Millisecond)
	_, forces := fetcher.snapshot()
	require.Equal(t, []bool{false}, forces, "baseline probe must not force")

	// 未到节奏（刚提交过）→ 不再探测
	r.scheduleBaselineProbes(context.Background(), time.Now())
	calls, _ := fetcher.snapshot()
	require.Equal(t, 1, calls, "baseline probe should respect interval")
}

func TestRecorder_ScheduleDecisiveProbesRespectsClaimGuard(t *testing.T) {
	repo := newStubWindowUsageRepo()
	dueRow := &AccountWindowUsageRecord{ID: 99, AccountID: 7, WindowType: "5h", WindowEnd: time.Now().Add(time.Minute)}
	repo.dueRows = []*AccountWindowUsageRecord{dueRow}
	fetcher := &stubWindowQuotaSource{}
	r, stop := newRunningRecorderForTest(repo, &stubWindowUsageLogRepo{}, fetcher)
	defer stop()

	// 认领失败（他副本已认领/次数耗尽）→ 不打上游
	repo.claimResults[99] = false
	r.scheduleDecisiveProbes(context.Background(), time.Now())
	calls, _ := fetcher.snapshot()
	require.Equal(t, 0, calls)

	// 认领成功 → force 探测
	repo.claimResults[99] = true
	r.scheduleDecisiveProbes(context.Background(), time.Now())
	require.Eventually(t, func() bool {
		calls, _ := fetcher.snapshot()
		return calls == 1
	}, 5*time.Second, 20*time.Millisecond)
	_, forces := fetcher.snapshot()
	require.Equal(t, []bool{true}, forces, "decisive probe must force-refresh")
}

func TestRecorder_RunDailyMaintenancePrunesRetentionWindows(t *testing.T) {
	repo := newStubWindowUsageRepo()
	r := newRecorderForTest(repo, &stubWindowUsageLogRepo{}, &stubWindowQuotaSource{})

	now := time.Now()
	r.RunDailyMaintenance(context.Background())

	require.WithinDuration(t, now.AddDate(0, 0, -windowHistoryRetentionDays), repo.pruneFinalizedAt, time.Minute)
	require.WithinDuration(t, now.AddDate(0, 0, -windowStaleOpenRetentionDays), repo.pruneStaleOpenFrom, time.Minute)
}

// --- 查询服务 ---

type stubWindowAccountRepo struct {
	stubOpenAIAccountRepo
}

// GetByID 缺省账号返回类型化 ErrAccountNotFound（对齐真实仓储，
// 查询服务据此走宽松分支）。
func (r stubWindowAccountRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	account, err := r.stubOpenAIAccountRepo.GetByID(ctx, id)
	if err != nil {
		return nil, ErrAccountNotFound
	}
	return account, nil
}

func TestWindowHistoryService_GroupsByWindowTypeAndFlagsTracking(t *testing.T) {
	windowRepo := newStubWindowUsageRepo()
	end := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	finalizedAt := time.Now()
	row := &AccountWindowUsageRecord{
		ID: 1, AccountID: 7, WindowType: "5h",
		WindowStart: end.Add(-5 * time.Hour), WindowEnd: end,
		PeakUsedPercent: 66, LastUsedPercent: 60, SampleCount: 3,
		FinalizedAt: &finalizedAt,
	}
	windowRepo.finalized = []*AccountWindowUsageRecord{row}

	svc := NewAccountWindowUsageHistoryService(windowRepo, stubWindowAccountRepo{stubOpenAIAccountRepo{accounts: []Account{
		{ID: 7, Platform: domain.PlatformAnthropic, WindowTrackingEnabled: true},
	}}})

	resp, err := svc.GetWindowHistory(context.Background(), 7, 30)
	require.NoError(t, err)
	require.True(t, resp.Tracked)
	require.Len(t, resp.Windows["5h"], 1)

	entry := resp.Windows["5h"][0]
	require.True(t, entry.Finalized)
	require.NotNil(t, entry.FinalUsedPercent)
	require.InDelta(t, 60.0, *entry.FinalUsedPercent, 0.001)
	require.InDelta(t, 66.0, entry.PeakUsedPercent, 0.001)
}

func TestWindowHistoryService_MissingAccountYieldsEmptyResponse(t *testing.T) {
	svc := NewAccountWindowUsageHistoryService(newStubWindowUsageRepo(), stubWindowAccountRepo{stubOpenAIAccountRepo{accounts: nil}})

	resp, err := svc.GetWindowHistory(context.Background(), 404, 30)
	require.NoError(t, err)
	require.False(t, resp.Tracked)
	require.Empty(t, resp.Windows)
}

func TestWindowHistoryService_FiltersNonRecordedWindowRows(t *testing.T) {
	windowRepo := newStubWindowUsageRepo()
	finalizedAt := time.Now()
	end := time.Now().Add(-time.Hour)
	windowRepo.finalized = []*AccountWindowUsageRecord{
		{ID: 1, AccountID: 7, WindowType: "5h", WindowStart: end.Add(-5 * time.Hour), WindowEnd: end, FinalizedAt: &finalizedAt},
		{ID: 2, AccountID: 7, WindowType: "daily", WindowStart: end.Add(-24 * time.Hour), WindowEnd: end, FinalizedAt: &finalizedAt},
	}

	svc := NewAccountWindowUsageHistoryService(windowRepo, stubWindowAccountRepo{stubOpenAIAccountRepo{accounts: []Account{
		{ID: 7, WindowTrackingEnabled: false},
	}}})

	resp, err := svc.GetWindowHistory(context.Background(), 7, 30)
	require.NoError(t, err)
	require.False(t, resp.Tracked, "tracking flag comes from the account")
	require.Len(t, resp.Windows["5h"], 1)
	require.NotContains(t, resp.Windows, "daily")
}

// windowQuotaSource 接口契约（防止接口与 fetcher 实现漂移）。
var _ windowQuotaSource = (*ChannelMonitorQuotaFetcher)(nil)

// stubWindowUsageLogRepo 必须满足 UsageLogRepository（嵌入即可）。
var _ UsageLogRepository = (*stubWindowUsageLogRepo)(nil)

// 保险：错误路径透传
func TestRecorder_ApplySnapshot_RepoErrorPropagates(t *testing.T) {
	repo := newStubWindowUsageRepo()
	usageRepo := &stubWindowUsageLogRepo{rangeErr: errors.New("db down")}
	r := newRecorderForTest(repo, usageRepo, &stubWindowQuotaSource{})

	end := time.Now().Add(-10 * time.Minute)
	require.NoError(t, repo.UpsertOpenWindow(context.Background(), &AccountWindowUsageRecord{
		AccountID: 7, WindowType: "5h", WindowStart: end.Add(-5 * time.Hour), WindowEnd: end, SampleCount: 1,
	}))

	err := r.ApplySnapshot(context.Background(), 7, snapshotWithTiers(tier5h(5, time.Now().Add(5*time.Hour))))
	require.Error(t, err, "expired-window finalize failure must surface")
}
