package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
)

// 账号滚动窗口用量历史（opt-in，accounts.window_tracking_enabled 启用）。
//
// 数据分两块：
//   - 窗口限额（使用率/重置时间）：由 AccountWindowUsageRecorder
//     复用渠道监控的配额抓取链路（ChannelMonitorQuotaFetcher）定时探测，
//     并在窗口重置前做决断性 force 探测，捕获窗口最终用量
//   - token 明细：窗口关闭后由 usage_logs 在 [window_start, window_end) 内
//     聚合重建（GetAccountWindowStatsRange），不依赖上游
//
// 管理端在「账号管理 → 查看统计」弹窗按窗口类型展示两者叠加的图表，
// 推算限额（token ÷ 最终使用率）随时间下降即为限额缩水信号。

// 滚动窗口类型 token（与 domain.MonitorQuotaTier.Window 取值一致）。
// 仅记录带滚动窗口语义的类型；Gemini/Grok 的 daily、30d 与 Antigravity 的
// total 不在本功能语义内（未来需要时可扩展 duration 映射）。
const (
	windowTypeFiveHour       = "5h"
	windowTypeSevenDay       = "7d"
	windowTypeSevenDaySonnet = "7d-sonnet"
	windowTypeSevenDayFable  = "7d-fable"
	windowTypeWeekly         = "weekly"
)

// windowTypeDuration 窗口类型的窗口时长（用于推导 window_start 与 token 聚合边界）。
var windowTypeDuration = map[string]time.Duration{
	windowTypeFiveHour:       5 * time.Hour,
	windowTypeSevenDay:       7 * 24 * time.Hour,
	windowTypeSevenDaySonnet: 7 * 24 * time.Hour,
	windowTypeSevenDayFable:  7 * 24 * time.Hour,
	windowTypeWeekly:         7 * 24 * time.Hour,
}

// recordedWindow 判断 tier 窗口是否纳入记录。
func recordedWindow(windowType string) bool {
	_, ok := windowTypeDuration[windowType]
	return ok
}

// DecisiveBudgetSlideWindow 决断预算的重置阈值：window_end 前移超过该值才视为
// 新窗口实例并重置认领预算；秒级 reset 抖动不重置，避免亚分钟滑动反复补充
// 探测预算（repo upsert 以 SQL 参数消费该常量）。
const DecisiveBudgetSlideWindow = 60 * time.Second

// WindowTrackable 判断账号平台是否适用窗口追踪。
//
// 只有具备「只读用量 API」的平台才能被主动探测：
//   - anthropic：console usage API，纯查询不消耗账号配额
//   - kimi/zhipu/deepseek coding plan：配额端点，纯查询
//
// OpenAI（Codex）的用量只能经真实推理请求的响应头推导，主动探测既消耗账号
// 自身配额、又污染测量对象（探针请求计入上游 used% 但不产生本地 usage_log，
// 推算限额 = 本地 token ÷ 上游 used% 会被系统性低估）；Gemini/Grok 只有 daily
// 窗口、Antigravity 只有 total、CN payg 只有余额端点——均产不出滚动窗口数据。
func WindowTrackable(account *Account) bool {
	if account == nil {
		return false
	}
	switch account.Platform {
	case domain.PlatformAnthropic:
		return true
	case domain.PlatformKimi, domain.PlatformZhipu, domain.PlatformDeepseek:
		return account.IsCodingPlan()
	default:
		return false
	}
}

// TrackablePlatformSQLArgs 平台门控 SQL 的参数（与 ListWindowTrackingEnabled
// 的占位符一一对应；coding plan 存于 credentials["account_mode"]，明文 JSONB）。
func TrackablePlatformSQLArgs() []any {
	return []any{
		domain.PlatformAnthropic,
		domain.PlatformKimi, domain.PlatformZhipu, domain.PlatformDeepseek,
		domain.AccountModeCoding,
	}
}

// ValidateWindowTrackingEnable 校验窗口追踪开关变更的平台适用性。
//
// 只拒绝「在不适用平台上新开启」：历史遗留的 true（旧版本写入/直改库）原样
// 再提交不报错——SQL 选取层已排除不适用平台，遗留 true 是惰性值，不能让它
// 卡住该账号的其他字段编辑。返回的 error 直接面向管理端 API。
func ValidateWindowTrackingEnable(account *Account, current, next bool) error {
	if account == nil || !next || next == current || WindowTrackable(account) {
		return nil
	}
	return fmt.Errorf("window_tracking_enabled 仅适用于 Anthropic 与国产 coding plan 账号，平台 %s 不支持", account.Platform)
}

// AccountWindowUsageRecord 单个滚动窗口的用量历史记录（一行）。
//
// finalized_at 为空表示「开放行」：当前窗口仍在滑动/使用中，quota 字段随采样
// 持续更新；窗口关闭后回填 token 明细并定格（局部唯一索引保证每账号每窗口
// 类型至多一行开放行）。
type AccountWindowUsageRecord struct {
	ID          int64     `json:"id"`
	AccountID   int64     `json:"account_id"`
	WindowType  string    `json:"window_type"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	// Peak/LastUsedPercent 窗口内峰值/最新使用率（0-100+，不截断）
	PeakUsedPercent float64 `json:"peak_used_percent"`
	LastUsedPercent float64 `json:"last_used_percent"`
	SampleCount     int     `json:"sample_count"`
	// DecisiveProbeCount/LastProbeAt 决断探测认领守卫（次数上限 + 最小间隔）
	DecisiveProbeCount int        `json:"decisive_probe_count"`
	LastProbeAt        *time.Time `json:"last_probe_at"`
	// Requests/Tokens* finalize 后由 usage_logs 聚合回填；开放行为 nil
	Requests            *int64     `json:"requests"`
	TokensTotal         *int64     `json:"tokens_total"`
	TokensInput         *int64     `json:"tokens_input"`
	TokensOutput        *int64     `json:"tokens_output"`
	TokensCacheCreation *int64     `json:"tokens_cache_creation"`
	TokensCacheRead     *int64     `json:"tokens_cache_read"`
	FinalizedAt         *time.Time `json:"finalized_at"`
}

// AccountWindowUsageRepository 账号滚动窗口用量历史仓储。
//
// 刻意做成窄接口（而非往 AccountRepository 加方法）：AccountRepository 有多个
// 测试文件手写全量实现，加方法会连锁要求补 stub；本接口仅记录器与查询服务使用。
type AccountWindowUsageRepository interface {
	// GetOpenWindow 读取账号指定窗口类型的开放行；无开放行返回 (nil, nil)。
	GetOpenWindow(ctx context.Context, accountID int64, windowType string) (*AccountWindowUsageRecord, error)
	// UpsertOpenWindow 原子插入/合并开放行（ON CONFLICT 局部唯一索引）：
	// peak 取 GREATEST、sample_count 累加、window_end 只前移不回退，
	// 并发/多副本探测同一账号时天然幂等合并。
	UpsertOpenWindow(ctx context.Context, row *AccountWindowUsageRecord) error
	// FinalizeWindow 幂等关闭窗口：回填 token 明细并设置 finalized_at。
	// 行不存在或已关闭时返回 false（不报错）。
	FinalizeWindow(ctx context.Context, id int64, stats *usagestats.WindowTokenStats, now time.Time) (bool, error)
	// ReplaceOpenWindow 事务内「关闭旧开放行 + 写入新开放行」：状态机的
	// 旧窗口过期 → 新窗口路径，避免新窗口数据在并发下误并入旧窗口行。
	// 旧行已被并发关闭时静默跳过 finalize，仅写入新行。
	ReplaceOpenWindow(ctx context.Context, oldID int64, stats *usagestats.WindowTokenStats, newRow *AccountWindowUsageRecord, now time.Time) error
	// ListExpiredOpenWindows 列出 window_end < cutoff 的开放行（finalize 扫描，
	// 不限启用账号——关闭追踪也要收尾遗留行）。按 window_end 升序。
	ListExpiredOpenWindows(ctx context.Context, cutoff time.Time, limit int) ([]*AccountWindowUsageRecord, error)
	// ListWindowsDueForDecisiveProbe 列出启用追踪账号中临近 window_end 且满足
	// 决断探测间隔/次数约束的开放行。
	ListWindowsDueForDecisiveProbe(ctx context.Context, now time.Time, lead, retryGap time.Duration, maxCount, limit int) ([]*AccountWindowUsageRecord, error)
	// ClaimDecisiveProbe 条件更新认领一次决断探测：满足次数上限与最小间隔才
	// 更新（影响行数=1 即认领成功），防止多副本/tick 重复探测。
	ClaimDecisiveProbe(ctx context.Context, id int64, now time.Time, retryGap time.Duration, maxCount int) (bool, error)
	// ListHistorySince 查询账号 window_end >= since 的历史（含开放行），
	// 按 window_type、window_end 升序。
	ListHistorySince(ctx context.Context, accountID int64, since time.Time) ([]*AccountWindowUsageRecord, error)
	// PruneFinalizedBefore 删除 finalized_at < cutoff 的已关闭行（保留期清理）。
	PruneFinalizedBefore(ctx context.Context, cutoff time.Time) (int64, error)
	// PruneStaleOpenBefore 删除 window_end < cutoff 的僵尸开放行
	// （账号软删/中途关闭追踪的兜底清理）。
	PruneStaleOpenBefore(ctx context.Context, cutoff time.Time) (int64, error)
	// ListWindowTrackingEnabled 列出启用窗口追踪且未软删的账号 ID。
	ListWindowTrackingEnabled(ctx context.Context) ([]int64, error)
}

// AccountWindowUsageEntry 管理端窗口历史接口的单窗口条目。
type AccountWindowUsageEntry struct {
	WindowStart         time.Time `json:"window_start"`
	WindowEnd           time.Time `json:"window_end"`
	Requests            *int64    `json:"requests"` // finalize 前为 null
	TokensTotal         *int64    `json:"tokens_total"`
	TokensInput         *int64    `json:"tokens_input"`
	TokensOutput        *int64    `json:"tokens_output"`
	TokensCacheCreation *int64    `json:"tokens_cache_creation"`
	TokensCacheRead     *int64    `json:"tokens_cache_read"`
	PeakUsedPercent     float64   `json:"peak_used_percent"`
	FinalUsedPercent    *float64  `json:"final_used_percent"` // 开放行为 null
	SampleCount         int       `json:"sample_count"`
	Finalized           bool      `json:"finalized"`
}

// AccountWindowHistoryResponse 管理端窗口历史接口响应。
// Windows 按窗口类型分组（key = "5h"/"7d"/...），组内旧 → 新。
type AccountWindowHistoryResponse struct {
	Tracked bool                                  `json:"tracked"` // 账号是否启用追踪
	Windows map[string][]*AccountWindowUsageEntry `json:"windows"`
}

// AccountWindowUsageHistoryService 账号滚动窗口用量历史查询服务（管理端）。
type AccountWindowUsageHistoryService struct {
	windowRepo  AccountWindowUsageRepository
	accountRepo AccountRepository
}

// NewAccountWindowUsageHistoryService 构造查询服务。
func NewAccountWindowUsageHistoryService(
	windowRepo AccountWindowUsageRepository,
	accountRepo AccountRepository,
) *AccountWindowUsageHistoryService {
	return &AccountWindowUsageHistoryService{
		windowRepo:  windowRepo,
		accountRepo: accountRepo,
	}
}

// GetWindowHistory 查询账号近 days 天的滚动窗口用量历史。
// 账号不存在或未启用追踪时返回 Tracked=false 与空 Windows（宽松语义，
// 弹窗据此渲染 opt-in 提示而非报错）。
func (s *AccountWindowUsageHistoryService) GetWindowHistory(ctx context.Context, accountID int64, days int) (*AccountWindowHistoryResponse, error) {
	resp := &AccountWindowHistoryResponse{Windows: map[string][]*AccountWindowUsageEntry{}}

	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		// 账号不存在（含软删）不算错误：弹窗在账号被并发删除时静默收起本区块
		if errors.Is(err, ErrAccountNotFound) {
			return resp, nil
		}
		return nil, fmt.Errorf("get account failed: %w", err)
	}
	if account == nil {
		return resp, nil
	}
	// Tracked = 开关开启 && 平台适用：不适用平台的遗留开关不展示 opt-in 语义
	resp.Tracked = account.WindowTrackingEnabled && WindowTrackable(account)

	since := time.Now().AddDate(0, 0, -days)
	records, err := s.windowRepo.ListHistorySince(ctx, accountID, since)
	if err != nil {
		return nil, fmt.Errorf("list window history failed: %w", err)
	}

	for _, rec := range records {
		if !recordedWindow(rec.WindowType) {
			continue
		}
		resp.Windows[rec.WindowType] = append(resp.Windows[rec.WindowType], windowRecordToEntry(rec))
	}
	return resp, nil
}

func windowRecordToEntry(rec *AccountWindowUsageRecord) *AccountWindowUsageEntry {
	entry := &AccountWindowUsageEntry{
		WindowStart:         rec.WindowStart,
		WindowEnd:           rec.WindowEnd,
		PeakUsedPercent:     rec.PeakUsedPercent,
		SampleCount:         rec.SampleCount,
		Requests:            rec.Requests,
		TokensTotal:         rec.TokensTotal,
		TokensInput:         rec.TokensInput,
		TokensOutput:        rec.TokensOutput,
		TokensCacheCreation: rec.TokensCacheCreation,
		TokensCacheRead:     rec.TokensCacheRead,
		Finalized:           rec.FinalizedAt != nil,
	}
	if rec.FinalizedAt != nil {
		final := rec.LastUsedPercent
		entry.FinalUsedPercent = &final
	}
	return entry
}
