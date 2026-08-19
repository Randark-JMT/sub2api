//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/stretchr/testify/require"
)

// --- Anthropic OAuth usage 缓存的 force 语义 ---

// stubClaudeUsageFetcher 计数调用的 ClaudeUsageFetcher stub。
type stubClaudeUsageFetcher struct {
	mu    sync.Mutex
	resp  *ClaudeUsageResponse
	err   error
	calls int
}

func (f *stubClaudeUsageFetcher) FetchUsage(context.Context, string, string) (*ClaudeUsageResponse, error) {
	return f.fetch()
}

func (f *stubClaudeUsageFetcher) FetchUsageWithOptions(context.Context, *ClaudeUsageFetchOptions) (*ClaudeUsageResponse, error) {
	return f.fetch()
}

func (f *stubClaudeUsageFetcher) fetch() (*ClaudeUsageResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *stubClaudeUsageFetcher) getCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type anthropicForceAccountRepo struct {
	stubOpenAIAccountRepo
}

func (r anthropicForceAccountRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

// skipWindowStatsUsageLogRepo 窗口统计查询直接报错：addWindowStats 只记日志，
// 不影响 usage 主链路，测试无需构造完整统计。
type skipWindowStatsUsageLogRepo struct {
	UsageLogRepository
}

func (skipWindowStatsUsageLogRepo) GetAccountWindowStats(context.Context, int64, time.Time) (*usagestats.AccountStats, error) {
	return nil, errors.New("skip window stats")
}

func newAnthropicForceService(fetcher *stubClaudeUsageFetcher) *AccountUsageService {
	account := Account{
		ID:          21,
		Platform:    PlatformAnthropic,
		Type:        AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "tok"},
	}
	return &AccountUsageService{
		accountRepo:  anthropicForceAccountRepo{stubOpenAIAccountRepo{accounts: []Account{account}}},
		usageLogRepo: skipWindowStatsUsageLogRepo{},
		usageFetcher: fetcher,
		cache:        NewUsageCache(),
	}
}

func TestAccountUsageService_AnthropicForceBypassesPositiveCache(t *testing.T) {
	fetcher := &stubClaudeUsageFetcher{resp: &ClaudeUsageResponse{}}
	svc := newAnthropicForceService(fetcher)
	ctx := context.Background()

	_, err := svc.GetUsage(ctx, 21)
	require.NoError(t, err)
	require.Equal(t, 1, fetcher.getCalls())

	// 常规查询命中正缓存。
	_, err = svc.GetUsage(ctx, 21)
	require.NoError(t, err)
	require.Equal(t, 1, fetcher.getCalls(), "positive cache should serve repeat queries")

	// force 探测跳过正缓存，直接打上游（决断探测拿到窗口关闭前的最终用量）。
	_, err = svc.GetUsage(ctx, 21, true)
	require.NoError(t, err)
	require.Equal(t, 2, fetcher.getCalls(), "force must bypass the positive cache")

	// force 的结果回填缓存：后续常规查询继续命中。
	_, err = svc.GetUsage(ctx, 21)
	require.NoError(t, err)
	require.Equal(t, 2, fetcher.getCalls())
}

func TestAccountUsageService_AnthropicForceKeepsNegativeCache(t *testing.T) {
	fetcher := &stubClaudeUsageFetcher{err: errors.New("usage API error: HTTP 429")}
	svc := newAnthropicForceService(fetcher)
	ctx := context.Background()

	_, err := svc.GetUsage(ctx, 21)
	require.Error(t, err)
	require.Equal(t, 1, fetcher.getCalls())

	// 上游 429/故障期间，force 也命中负缓存短路——强制刷新只会雪上加霜。
	_, err = svc.GetUsage(ctx, 21, true)
	require.Error(t, err)
	require.Equal(t, 1, fetcher.getCalls(), "force must respect the negative cache")
}
