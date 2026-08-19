//go:build integration

package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// newWindowRepoForTest 构造被测仓储。
func newWindowRepoForTest(t *testing.T) (*accountWindowUsageRepository, *dbent.Client) {
	t.Helper()
	client := testEntClient(t)
	return &accountWindowUsageRepository{client: client, db: integrationDB}, client
}

// mustCreateWindowAccount 建号并按需打开窗口追踪。
func mustCreateWindowAccount(t *testing.T, client *dbent.Client, tracking bool) *service.Account {
	t.Helper()
	account := mustCreateAccount(t, client, &service.Account{Name: "win-" + uuid.NewString()})
	if tracking {
		_, err := integrationDB.ExecContext(context.Background(),
			`UPDATE accounts SET window_tracking_enabled = TRUE WHERE id = $1`, account.ID)
		require.NoError(t, err, "enable window tracking")
	}
	return account
}

func mustSeedUsageLog(t *testing.T, client *dbent.Client, accountID int64, at time.Time, in, out, cacheCreate, cacheRead int) {
	t.Helper()
	user := mustCreateUser(t, client, &service.User{Email: "win-" + uuid.NewString() + "@example.com"})
	apiKey := mustCreateApiKey(t, client, &service.APIKey{UserID: user.ID, Key: "sk-win-" + uuid.NewString(), Name: "k"})
	repo := newUsageLogRepositoryWithSQL(client, integrationDB)
	_, err := repo.Create(context.Background(), &service.UsageLog{
		UserID:              user.ID,
		APIKeyID:            apiKey.ID,
		AccountID:           accountID,
		RequestID:           uuid.NewString(),
		Model:               "claude-3",
		InputTokens:         in,
		OutputTokens:        out,
		CacheCreationTokens: cacheCreate,
		CacheReadTokens:     cacheRead,
		TotalCost:           0.1,
		ActualCost:          0.1,
		CreatedAt:           at,
	})
	require.NoError(t, err, "seed usage log")
}

func TestAccountWindowUsage_UpsertMergeSemantics(t *testing.T) {
	ctx := context.Background()
	repo, client := newWindowRepoForTest(t)
	account := mustCreateWindowAccount(t, client, true)

	start := time.Now().Add(-5 * time.Hour).UTC().Truncate(time.Second)
	end := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second)

	// 首次插入
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h", WindowStart: start, WindowEnd: end,
		PeakUsedPercent: 30, LastUsedPercent: 30, SampleCount: 1,
	}))
	row, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, 1, row.SampleCount)
	require.InDelta(t, 30.0, row.PeakUsedPercent, 0.001)

	// 合并：peak 取 GREATEST、绝对值 COALESCE、sample 累加
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h", WindowStart: start, WindowEnd: end,
		PeakUsedPercent: 25, LastUsedPercent: 45, SampleCount: 1,
		UsedAbsolute: floatPtr(120), LimitAbsolute: floatPtr(400),
	}))
	row, err = repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.Equal(t, 2, row.SampleCount)
	require.InDelta(t, 30.0, row.PeakUsedPercent, 0.001, "peak must keep the max")
	require.InDelta(t, 45.0, row.LastUsedPercent, 0.001)
	require.NotNil(t, row.UsedAbsolute)
	require.InDelta(t, 120.0, *row.UsedAbsolute, 0.001)

	// window_end 只前移不回退（reset 抖动安全）
	newEnd := end.Add(10 * time.Minute)
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h", WindowStart: end, WindowEnd: newEnd,
		PeakUsedPercent: 50, LastUsedPercent: 50, SampleCount: 1,
	}))
	row, err = repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.Equal(t, newEnd, row.WindowEnd)
	require.Equal(t, end, row.WindowStart, "start must move with a forward end")

	// 回退观测：边界不动，指标照常合并
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h", WindowStart: start, WindowEnd: end,
		PeakUsedPercent: 60, LastUsedPercent: 60, SampleCount: 1,
	}))
	row, err = repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.Equal(t, newEnd, row.WindowEnd, "window_end must never regress")
	require.InDelta(t, 60.0, row.PeakUsedPercent, 0.001)
	require.Equal(t, 4, row.SampleCount)
}

func TestAccountWindowUsage_ConcurrentUpsertsConverge(t *testing.T) {
	ctx := context.Background()
	repo, client := newWindowRepoForTest(t)
	account := mustCreateWindowAccount(t, client, true)

	start := time.Now().Add(-5 * time.Hour).UTC()
	end := time.Now().Add(time.Hour).UTC()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
				AccountID: account.ID, WindowType: "5h", WindowStart: start, WindowEnd: end,
				PeakUsedPercent: float64(10 + i), LastUsedPercent: float64(10 + i), SampleCount: 1,
			})
		}(i)
	}
	wg.Wait()

	row, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, 8, row.SampleCount, "every sample must be counted exactly once")
	require.InDelta(t, 17.0, row.PeakUsedPercent, 0.001)
}

func TestAccountWindowUsage_FinalizeAndReplace(t *testing.T) {
	ctx := context.Background()
	repo, client := newWindowRepoForTest(t)
	account := mustCreateWindowAccount(t, client, true)

	start := time.Now().Add(-5 * time.Hour).UTC().Truncate(time.Second)
	end := time.Now().Add(-10 * time.Minute).UTC().Truncate(time.Second)
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h", WindowStart: start, WindowEnd: end,
		PeakUsedPercent: 80, LastUsedPercent: 70, SampleCount: 3,
	}))
	open, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.NotNil(t, open)

	stats := &usagestats.WindowTokenStats{Requests: 9, TokensTotal: 12345}
	ok, err := repo.FinalizeWindow(ctx, open.ID, stats, time.Now())
	require.NoError(t, err)
	require.True(t, ok, "first finalize should win")

	// 幂等守卫：重复关闭 no-op
	ok, err = repo.FinalizeWindow(ctx, open.ID, stats, time.Now())
	require.NoError(t, err)
	require.False(t, ok)

	// 关闭后不再是开放行；token 明细已回填
	after, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.Nil(t, after)

	// Replace：旧行（已关闭）+ 新开放行
	newStart := time.Now().UTC().Truncate(time.Second)
	newEnd := newStart.Add(5 * time.Hour)
	require.NoError(t, repo.ReplaceOpenWindow(ctx, open.ID, stats, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h", WindowStart: newStart, WindowEnd: newEnd,
		PeakUsedPercent: 5, LastUsedPercent: 5, SampleCount: 1,
	}, time.Now()))
	fresh, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.NotNil(t, fresh)
	require.Equal(t, newEnd, fresh.WindowEnd)
	require.Nil(t, fresh.FinalizedAt)
}

func TestAccountWindowUsage_DecisiveProbeClaimGuard(t *testing.T) {
	ctx := context.Background()
	repo, client := newWindowRepoForTest(t)
	tracked := mustCreateWindowAccount(t, client, true)
	untracked := mustCreateWindowAccount(t, client, false)

	now := time.Now()
	// 截断到秒：PG timestamptz 只有微秒精度，未截断的 time.Now() 往返后不等
	end := now.Add(time.Minute).UTC().Truncate(time.Second)
	for _, accountID := range []int64{tracked.ID, untracked.ID} {
		require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
			AccountID: accountID, WindowType: "5h",
			WindowStart: end.Add(-5 * time.Hour), WindowEnd: end,
			PeakUsedPercent: 10, LastUsedPercent: 10, SampleCount: 1,
		}))
	}

	// 只有启用追踪且临近 window_end 的行会列出
	due, err := repo.ListWindowsDueForDecisiveProbe(ctx, now, 2*time.Minute, 45*time.Second, 2, 50)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, tracked.ID, due[0].AccountID)
	rowID := due[0].ID

	// 认领 1：成功
	claimed, err := repo.ClaimDecisiveProbe(ctx, rowID, now, 45*time.Second, 2)
	require.NoError(t, err)
	require.True(t, claimed)

	// 认领 2（间隔未到）：拒绝
	claimed, err = repo.ClaimDecisiveProbe(ctx, rowID, now.Add(10*time.Second), 45*time.Second, 2)
	require.NoError(t, err)
	require.False(t, claimed)

	// 间隔已过：成功（第 2 次）
	claimed, err = repo.ClaimDecisiveProbe(ctx, rowID, now.Add(time.Minute), 45*time.Second, 2)
	require.NoError(t, err)
	require.True(t, claimed)

	// 次数耗尽：拒绝
	claimed, err = repo.ClaimDecisiveProbe(ctx, rowID, now.Add(10*time.Minute), 45*time.Second, 2)
	require.NoError(t, err)
	require.False(t, claimed)

	// 决断预算跟随窗口实例：window_end 前移（滚动窗口滑动到下一窗口）后，
	// 认领计数与 last_probe_at 重置，新窗口恢复完整预算
	slidedEnd := end.Add(30 * time.Minute)
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: tracked.ID, WindowType: "5h",
		WindowStart: slidedEnd.Add(-5 * time.Hour), WindowEnd: slidedEnd,
		PeakUsedPercent: 20, LastUsedPercent: 20, SampleCount: 1,
	}))
	open, err := repo.GetOpenWindow(ctx, tracked.ID, "5h")
	require.NoError(t, err)
	require.Equal(t, slidedEnd, open.WindowEnd)
	require.Zero(t, open.DecisiveProbeCount, "probe budget must reset when the window slides forward")
	require.Nil(t, open.LastProbeAt)

	// 预算重置后立即可再认领
	claimed, err = repo.ClaimDecisiveProbe(ctx, rowID, now.Add(11*time.Minute), 45*time.Second, 2)
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestAccountWindowUsage_HistoryAndPrune(t *testing.T) {
	ctx := context.Background()
	repo, client := newWindowRepoForTest(t)
	account := mustCreateWindowAccount(t, client, true)

	now := time.Now().UTC()
	// 旧窗口（已关闭，window_end/finalized_at 都在 40 天前——生产中 finalize
	// 紧跟窗口结束，保留期语义按 finalized_at 计算）
	oldEnd := now.AddDate(0, 0, -40)
	oldRow := &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h",
		WindowStart: oldEnd.Add(-5 * time.Hour), WindowEnd: oldEnd,
		PeakUsedPercent: 10, LastUsedPercent: 10, SampleCount: 1,
	}
	require.NoError(t, repo.UpsertOpenWindow(ctx, oldRow))
	openOld, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	_, err = repo.FinalizeWindow(ctx, openOld.ID, &usagestats.WindowTokenStats{}, oldEnd.Add(time.Minute))
	require.NoError(t, err)

	// 当前开放行
	require.NoError(t, repo.UpsertOpenWindow(ctx, &service.AccountWindowUsageRecord{
		AccountID: account.ID, WindowType: "5h",
		WindowStart: now.Add(-5 * time.Hour), WindowEnd: now.Add(time.Hour),
		PeakUsedPercent: 20, LastUsedPercent: 20, SampleCount: 1,
	}))

	// since=30d：只有当前行；since=60d：两行
	recent, err := repo.ListHistorySince(ctx, account.ID, now.AddDate(0, 0, -30))
	require.NoError(t, err)
	require.Len(t, recent, 1)
	wider, err := repo.ListHistorySince(ctx, account.ID, now.AddDate(0, 0, -60))
	require.NoError(t, err)
	require.Len(t, wider, 2)

	// 保留清理按 finalized_at 计算，且是全局删除：断言用「本账号视角」
	// 验证（ListHistorySince），deleted 只验证方向，避免历史遗留行干扰。
	var deleted int64
	_, err = repo.PruneFinalizedBefore(ctx, now.AddDate(0, 0, -90))
	require.NoError(t, err)
	after90d, err := repo.ListHistorySince(ctx, account.ID, now.AddDate(0, 0, -60))
	require.NoError(t, err)
	require.Len(t, after90d, 2, "90d retention must keep the 40-day-old window")

	deleted, err = repo.PruneFinalizedBefore(ctx, now.AddDate(0, 0, -30))
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, int64(1), "30d cutoff must drop the 40-day-old window")
	after30d, err := repo.ListHistorySince(ctx, account.ID, now.AddDate(0, 0, -60))
	require.NoError(t, err)
	require.Len(t, after30d, 1, "only the open row should remain")

	// 开放行不受 finalized 清理影响，但受僵尸清理影响
	staleRows, err := repo.ListExpiredOpenWindows(ctx, now.Add(2*time.Hour), 100)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(staleRows), 1)
	deleted, err = repo.PruneStaleOpenBefore(ctx, now.Add(2*time.Hour))
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, int64(1))
	gone, err := repo.GetOpenWindow(ctx, account.ID, "5h")
	require.NoError(t, err)
	require.Nil(t, gone, "stale open row must be pruned")
}

func TestAccountWindowUsage_TrackingEnabledListing(t *testing.T) {
	ctx := context.Background()
	repo, client := newWindowRepoForTest(t)
	on := mustCreateWindowAccount(t, client, true)
	mustCreateWindowAccount(t, client, false)

	ids, err := repo.ListWindowTrackingEnabled(ctx)
	require.NoError(t, err)
	require.Contains(t, ids, on.ID)
	// 软删后不再列出
	_, err = integrationDB.ExecContext(ctx, `UPDATE accounts SET deleted_at = NOW() WHERE id = $1`, on.ID)
	require.NoError(t, err)
	ids, err = repo.ListWindowTrackingEnabled(ctx)
	require.NoError(t, err)
	require.NotContains(t, ids, on.ID)
}

func TestGetAccountWindowStatsRange_AggregatesOnlyWithinBounds(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	account := mustCreateWindowAccount(t, client, false)
	repo := newUsageLogRepositoryWithSQL(client, integrationDB)

	windowStart := time.Now().Add(-5 * time.Hour).UTC().Truncate(time.Second)
	windowEnd := time.Now().UTC().Truncate(time.Second)

	// 窗口内 3 条
	mustSeedUsageLog(t, client, account.ID, windowStart.Add(10*time.Minute), 100, 50, 10, 5)
	mustSeedUsageLog(t, client, account.ID, windowStart.Add(1*time.Hour), 200, 100, 20, 10)
	mustSeedUsageLog(t, client, account.ID, windowEnd.Add(-time.Second), 1, 1, 1, 1)
	// 边界精确钉死半开语义：恰在 start（计入，>=）与恰在 end（不计入，<）
	mustSeedUsageLog(t, client, account.ID, windowStart, 2, 1, 0, 0)
	mustSeedUsageLog(t, client, account.ID, windowEnd, 4, 2, 0, 0)
	// 窗口外 2 条（起点前 + 终点后）
	mustSeedUsageLog(t, client, account.ID, windowStart.Add(-time.Minute), 999, 999, 999, 999)
	mustSeedUsageLog(t, client, account.ID, windowEnd.Add(time.Minute), 999, 999, 999, 999)

	stats, err := repo.GetAccountWindowStatsRange(ctx, account.ID, windowStart, windowEnd)
	require.NoError(t, err)
	require.Equal(t, int64(4), stats.Requests)
	// 165 + 330 + 4 + 3 = 502；恰在 end 的行不计入（半开区间），窗口外两条亦然
	require.Equal(t, int64(502), stats.TokensTotal)
	require.Equal(t, int64(303), stats.TokensInput)        // 100+200+1+2
	require.Equal(t, int64(152), stats.TokensOutput)       // 50+100+1+1
	require.Equal(t, int64(31), stats.TokensCacheCreation) // 10+20+1+0
	require.Equal(t, int64(16), stats.TokensCacheRead)     // 5+10+1+0
}

// floatPtr 测试辅助。
func floatPtr(v float64) *float64 {
	return &v
}
