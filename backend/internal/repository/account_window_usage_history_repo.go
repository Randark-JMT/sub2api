package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/accountwindowusagehistory"
	"github.com/Wei-Shaw/sub2api/internal/pkg/usagestats"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// accountWindowUsageRepository 实现 service.AccountWindowUsageRepository。
//
// 选型说明（同 channelMonitorRepository）：
//   - 简单读取走 ent，复用项目的事务上下文支持
//   - upsert/认领/清理等集合语义走原生 SQL——局部唯一索引的 ON CONFLICT、
//     GREATEST/单调计数与条件 UPDATE 在 SQL 里表达最直接，也保证多副本并发安全
type accountWindowUsageRepository struct {
	client *dbent.Client
	db     *sql.DB
}

// NewAccountWindowUsageRepository 创建仓储实例。
func NewAccountWindowUsageRepository(client *dbent.Client, db *sql.DB) service.AccountWindowUsageRepository {
	return &accountWindowUsageRepository{client: client, db: db}
}

// accountWindowColumns 读取行的公共列清单（与 scanAccountWindowRows 配套）；
// accountWindowColumnsH 为 JOIN accounts 查询用的 h. 前缀消歧版本。
const (
	accountWindowColumns = `id, account_id, window_type, window_start, window_end,
	peak_used_percent, last_used_percent, used_absolute, limit_absolute,
	sample_count, decisive_probe_count, last_probe_at,
	requests, tokens_total, tokens_input, tokens_output, tokens_cache_creation, tokens_cache_read,
	finalized_at`

	accountWindowColumnsH = `h.id, h.account_id, h.window_type, h.window_start, h.window_end,
	h.peak_used_percent, h.last_used_percent, h.used_absolute, h.limit_absolute,
	h.sample_count, h.decisive_probe_count, h.last_probe_at,
	h.requests, h.tokens_total, h.tokens_input, h.tokens_output, h.tokens_cache_creation, h.tokens_cache_read,
	h.finalized_at`
)

// GetOpenWindow 读取账号指定窗口类型的开放行；无开放行返回 (nil, nil)。
func (r *accountWindowUsageRepository) GetOpenWindow(ctx context.Context, accountID int64, windowType string) (*service.AccountWindowUsageRecord, error) {
	client := clientFromContext(ctx, r.client)
	row, err := client.AccountWindowUsageHistory.Query().
		Where(
			accountwindowusagehistory.AccountIDEQ(accountID),
			accountwindowusagehistory.WindowTypeEQ(windowType),
			accountwindowusagehistory.FinalizedAtIsNil(),
		).
		Only(ctx)
	if dbent.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get open window failed: %w", err)
	}
	return entToAccountWindowRecord(row), nil
}

// ListHistorySince 查询账号 window_end >= since 的历史（含开放行）。
func (r *accountWindowUsageRepository) ListHistorySince(ctx context.Context, accountID int64, since time.Time) ([]*service.AccountWindowUsageRecord, error) {
	client := clientFromContext(ctx, r.client)
	rows, err := client.AccountWindowUsageHistory.Query().
		Where(
			accountwindowusagehistory.AccountIDEQ(accountID),
			accountwindowusagehistory.WindowEndGTE(since),
		).
		Order(dbent.Asc(accountwindowusagehistory.FieldWindowType), dbent.Asc(accountwindowusagehistory.FieldWindowEnd)).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list window history failed: %w", err)
	}
	records := make([]*service.AccountWindowUsageRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, entToAccountWindowRecord(row))
	}
	return records, nil
}

// UpsertOpenWindow 原子插入/合并开放行。
//
// 冲突目标为局部唯一索引 (account_id, window_type) WHERE finalized_at IS NULL。
// 合并语义：peak 取 GREATEST、last 直接覆盖、绝对值 COALESCE（保留最后非空）、
// sample_count 累加；window_end 只前移不回退（上游 reset 抖动/并发乱序均安全），
// 随之前移时 window_start 一并重算、决断认领预算重置（预算归属窗口实例），
// 保证 [start, end) 恰为最终窗口。
func (r *accountWindowUsageRepository) UpsertOpenWindow(ctx context.Context, row *service.AccountWindowUsageRecord) error {
	query := `
		INSERT INTO account_window_usage_histories
			(account_id, window_type, window_start, window_end,
			 peak_used_percent, last_used_percent, used_absolute, limit_absolute,
			 sample_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (account_id, window_type) WHERE finalized_at IS NULL
		DO UPDATE SET
			peak_used_percent = GREATEST(
				account_window_usage_histories.peak_used_percent, EXCLUDED.peak_used_percent),
			last_used_percent = EXCLUDED.last_used_percent,
			used_absolute = COALESCE(EXCLUDED.used_absolute, account_window_usage_histories.used_absolute),
			limit_absolute = COALESCE(EXCLUDED.limit_absolute, account_window_usage_histories.limit_absolute),
			sample_count = account_window_usage_histories.sample_count + EXCLUDED.sample_count,
			-- 决断预算跟随窗口实例：window_end 前移即视为新窗口，重置认领计数
			decisive_probe_count = CASE
				WHEN EXCLUDED.window_end > account_window_usage_histories.window_end THEN 0
				ELSE account_window_usage_histories.decisive_probe_count END,
			last_probe_at = CASE
				WHEN EXCLUDED.window_end > account_window_usage_histories.window_end THEN NULL
				ELSE account_window_usage_histories.last_probe_at END,
			window_end = GREATEST(account_window_usage_histories.window_end, EXCLUDED.window_end),
			window_start = CASE
				WHEN EXCLUDED.window_end > account_window_usage_histories.window_end
				THEN EXCLUDED.window_start
				ELSE account_window_usage_histories.window_start END,
			updated_at = NOW()
	`
	_, err := r.db.ExecContext(ctx, query,
		row.AccountID, row.WindowType, row.WindowStart, row.WindowEnd,
		row.PeakUsedPercent, row.LastUsedPercent, row.UsedAbsolute, row.LimitAbsolute,
		row.SampleCount,
	)
	if err != nil {
		return fmt.Errorf("upsert open window failed: %w", err)
	}
	return nil
}

// FinalizeWindow 幂等关闭窗口并回填 token 明细。
// finalized_at IS NULL 守卫使重复执行/多副本并发关闭均安全（后到者 no-op）。
func (r *accountWindowUsageRepository) FinalizeWindow(ctx context.Context, id int64, stats *usagestats.WindowTokenStats, now time.Time) (bool, error) {
	if stats == nil {
		stats = &usagestats.WindowTokenStats{}
	}
	query := `
		UPDATE account_window_usage_histories
		SET requests = $2, tokens_total = $3, tokens_input = $4, tokens_output = $5,
		    tokens_cache_creation = $6, tokens_cache_read = $7,
		    finalized_at = $8, updated_at = NOW()
		WHERE id = $1 AND finalized_at IS NULL
	`
	res, err := r.db.ExecContext(ctx, query,
		id, stats.Requests, stats.TokensTotal, stats.TokensInput, stats.TokensOutput,
		stats.TokensCacheCreation, stats.TokensCacheRead, now,
	)
	if err != nil {
		return false, fmt.Errorf("finalize window failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("finalize window rows affected: %w", err)
	}
	return affected == 1, nil
}

// ReplaceOpenWindow 事务内关闭旧开放行 + 写入新开放行（状态机的旧窗口过期路径）。
// 旧行已被并发关闭时 finalize no-op，仅写入新行。
func (r *accountWindowUsageRepository) ReplaceOpenWindow(ctx context.Context, oldID int64, stats *usagestats.WindowTokenStats, newRow *service.AccountWindowUsageRecord, now time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("replace open window begin tx failed: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if stats == nil {
		stats = &usagestats.WindowTokenStats{}
	}
	finalizeQuery := `
		UPDATE account_window_usage_histories
		SET requests = $2, tokens_total = $3, tokens_input = $4, tokens_output = $5,
		    tokens_cache_creation = $6, tokens_cache_read = $7,
		    finalized_at = $8, updated_at = NOW()
		WHERE id = $1 AND finalized_at IS NULL
	`
	if _, err := tx.ExecContext(ctx, finalizeQuery,
		oldID, stats.Requests, stats.TokensTotal, stats.TokensInput, stats.TokensOutput,
		stats.TokensCacheCreation, stats.TokensCacheRead, now,
	); err != nil {
		return fmt.Errorf("replace open window finalize failed: %w", err)
	}

	insertQuery := `
		INSERT INTO account_window_usage_histories
			(account_id, window_type, window_start, window_end,
			 peak_used_percent, last_used_percent, used_absolute, limit_absolute,
			 sample_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (account_id, window_type) WHERE finalized_at IS NULL
		DO UPDATE SET
			peak_used_percent = GREATEST(
				account_window_usage_histories.peak_used_percent, EXCLUDED.peak_used_percent),
			last_used_percent = EXCLUDED.last_used_percent,
			used_absolute = COALESCE(EXCLUDED.used_absolute, account_window_usage_histories.used_absolute),
			limit_absolute = COALESCE(EXCLUDED.limit_absolute, account_window_usage_histories.limit_absolute),
			sample_count = account_window_usage_histories.sample_count + EXCLUDED.sample_count,
			-- 决断预算跟随窗口实例：window_end 前移即视为新窗口，重置认领计数
			decisive_probe_count = CASE
				WHEN EXCLUDED.window_end > account_window_usage_histories.window_end THEN 0
				ELSE account_window_usage_histories.decisive_probe_count END,
			last_probe_at = CASE
				WHEN EXCLUDED.window_end > account_window_usage_histories.window_end THEN NULL
				ELSE account_window_usage_histories.last_probe_at END,
			window_end = GREATEST(account_window_usage_histories.window_end, EXCLUDED.window_end),
			window_start = CASE
				WHEN EXCLUDED.window_end > account_window_usage_histories.window_end
				THEN EXCLUDED.window_start
				ELSE account_window_usage_histories.window_start END,
			updated_at = NOW()
	`
	if _, err := tx.ExecContext(ctx, insertQuery,
		newRow.AccountID, newRow.WindowType, newRow.WindowStart, newRow.WindowEnd,
		newRow.PeakUsedPercent, newRow.LastUsedPercent, newRow.UsedAbsolute, newRow.LimitAbsolute,
		newRow.SampleCount,
	); err != nil {
		return fmt.Errorf("replace open window insert failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("replace open window commit failed: %w", err)
	}
	return nil
}

// ListExpiredOpenWindows 列出 window_end < cutoff 的开放行（finalize 扫描）。
func (r *accountWindowUsageRepository) ListExpiredOpenWindows(ctx context.Context, cutoff time.Time, limit int) ([]*service.AccountWindowUsageRecord, error) {
	query := fmt.Sprintf(`
		SELECT %s FROM account_window_usage_histories
		WHERE finalized_at IS NULL AND window_end < $1
		ORDER BY window_end ASC
		LIMIT $2
	`, accountWindowColumns)
	rows, err := r.db.QueryContext(ctx, query, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired open windows failed: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanAccountWindowRows(rows)
}

// ListWindowsDueForDecisiveProbe 列出启用追踪账号中临近 window_end 且满足
// 决断探测间隔/次数约束的开放行。间隔参数显式 ::double precision，避免
// pq 未类型化参数被推断为 interval（timestamptz <= interval 报错）。
func (r *accountWindowUsageRepository) ListWindowsDueForDecisiveProbe(ctx context.Context, now time.Time, lead, retryGap time.Duration, maxCount, limit int) ([]*service.AccountWindowUsageRecord, error) {
	query := fmt.Sprintf(`
		SELECT %s
		FROM account_window_usage_histories h
		JOIN accounts a ON a.id = h.account_id
		WHERE h.finalized_at IS NULL
		  AND a.window_tracking_enabled = TRUE
		  AND a.deleted_at IS NULL
		  AND h.window_end <= $1::timestamptz + make_interval(secs => $2::double precision)
		  AND h.window_end > $1::timestamptz
		  AND h.decisive_probe_count < $3
		  AND (h.last_probe_at IS NULL OR h.last_probe_at <= $1::timestamptz - make_interval(secs => $4::double precision))
		ORDER BY h.window_end ASC
		LIMIT $5
	`, accountWindowColumnsH)
	rows, err := r.db.QueryContext(ctx, query,
		now, lead.Seconds(), maxCount, retryGap.Seconds(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list windows due for decisive probe failed: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanAccountWindowRows(rows)
}

// ClaimDecisiveProbe 条件认领一次决断探测（次数上限 + 最小间隔）。
func (r *accountWindowUsageRepository) ClaimDecisiveProbe(ctx context.Context, id int64, now time.Time, retryGap time.Duration, maxCount int) (bool, error) {
	query := `
		UPDATE account_window_usage_histories
		SET decisive_probe_count = decisive_probe_count + 1,
		    last_probe_at = $2,
		    updated_at = NOW()
		WHERE id = $1 AND finalized_at IS NULL
		  AND decisive_probe_count < $3
		  AND (last_probe_at IS NULL OR last_probe_at <= $2::timestamptz - make_interval(secs => $4::double precision))
	`
	res, err := r.db.ExecContext(ctx, query, id, now, maxCount, retryGap.Seconds())
	if err != nil {
		return false, fmt.Errorf("claim decisive probe failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim decisive probe rows affected: %w", err)
	}
	return affected == 1, nil
}

// PruneFinalizedBefore 删除保留期外的已关闭行。
func (r *accountWindowUsageRepository) PruneFinalizedBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM account_window_usage_histories WHERE finalized_at IS NOT NULL AND finalized_at < $1`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("prune finalized windows failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune finalized windows rows affected: %w", err)
	}
	return affected, nil
}

// PruneStaleOpenBefore 删除僵尸开放行（账号软删/中途关闭追踪的兜底清理）。
func (r *accountWindowUsageRepository) PruneStaleOpenBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM account_window_usage_histories WHERE finalized_at IS NULL AND window_end < $1`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("prune stale open windows failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune stale open windows rows affected: %w", err)
	}
	return affected, nil
}

// ListWindowTrackingEnabled 列出启用窗口追踪且未软删的账号 ID。
func (r *accountWindowUsageRepository) ListWindowTrackingEnabled(ctx context.Context) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT id FROM accounts WHERE window_tracking_enabled = TRUE AND deleted_at IS NULL ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("list window tracking enabled accounts failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	ids := make([]int64, 0, 16)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan window tracking account id failed: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate window tracking accounts failed: %w", err)
	}
	return ids, nil
}

// ---------- helpers ----------

// scanAccountWindowRows 扫描多行查询结果。
func scanAccountWindowRows(rows *sql.Rows) ([]*service.AccountWindowUsageRecord, error) {
	records := make([]*service.AccountWindowUsageRecord, 0, 16)
	for rows.Next() {
		rec := &service.AccountWindowUsageRecord{}
		if err := rows.Scan(
			&rec.ID, &rec.AccountID, &rec.WindowType, &rec.WindowStart, &rec.WindowEnd,
			&rec.PeakUsedPercent, &rec.LastUsedPercent, &rec.UsedAbsolute, &rec.LimitAbsolute,
			&rec.SampleCount, &rec.DecisiveProbeCount, &rec.LastProbeAt,
			&rec.Requests, &rec.TokensTotal, &rec.TokensInput, &rec.TokensOutput,
			&rec.TokensCacheCreation, &rec.TokensCacheRead,
			&rec.FinalizedAt,
		); err != nil {
			return nil, fmt.Errorf("scan account window row failed: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate account window rows failed: %w", err)
	}
	return records, nil
}

// entToAccountWindowRecord ent 行 → service 记录。
func entToAccountWindowRecord(row *dbent.AccountWindowUsageHistory) *service.AccountWindowUsageRecord {
	return &service.AccountWindowUsageRecord{
		ID:                  row.ID,
		AccountID:           row.AccountID,
		WindowType:          row.WindowType,
		WindowStart:         row.WindowStart,
		WindowEnd:           row.WindowEnd,
		PeakUsedPercent:     row.PeakUsedPercent,
		LastUsedPercent:     row.LastUsedPercent,
		UsedAbsolute:        row.UsedAbsolute,
		LimitAbsolute:       row.LimitAbsolute,
		SampleCount:         row.SampleCount,
		DecisiveProbeCount:  row.DecisiveProbeCount,
		LastProbeAt:         row.LastProbeAt,
		Requests:            row.Requests,
		TokensTotal:         row.TokensTotal,
		TokensInput:         row.TokensInput,
		TokensOutput:        row.TokensOutput,
		TokensCacheCreation: row.TokensCacheCreation,
		TokensCacheRead:     row.TokensCacheRead,
		FinalizedAt:         row.FinalizedAt,
	}
}
