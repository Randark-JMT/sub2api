-- Migration: 227_account_window_usage_history
-- 账号滚动窗口用量历史：
--   1. 新表 account_window_usage_histories：每账号每滚动窗口类型（5h/7d/7d-sonnet/
--      7d-fable/weekly）一行开放记录，窗口关闭（finalized_at 非空）后由 usage_logs
--      聚合回填 token 明细；局部唯一索引保证「每账号每窗口类型至多一行开放记录」
--      （记录器原子 upsert 的冲突目标）
--   2. accounts.window_tracking_enabled：opt-in 开关（默认关闭）。启用后后台记录器
--      会定时探测账号各滚动窗口用量，并在窗口重置前做决断性 force 探测——
--      会增加上游用量 API 调用，故按账号显式启用
--   3. 保留期 90 天，由每日维护任务物理删除（日志类表，不用软删除）

CREATE TABLE IF NOT EXISTS account_window_usage_histories (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    window_type VARCHAR(32) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    peak_used_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_used_percent DOUBLE PRECISION NOT NULL DEFAULT 0,
    used_absolute DOUBLE PRECISION,
    limit_absolute DOUBLE PRECISION,
    sample_count INT NOT NULL DEFAULT 0,
    decisive_probe_count INT NOT NULL DEFAULT 0,
    last_probe_at TIMESTAMPTZ,
    requests BIGINT,
    tokens_total BIGINT,
    tokens_input BIGINT,
    tokens_output BIGINT,
    tokens_cache_creation BIGINT,
    tokens_cache_read BIGINT,
    finalized_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_account_window_usage_range CHECK (window_end > window_start)
);

COMMENT ON TABLE account_window_usage_histories IS
    '账号滚动窗口用量历史（opt-in，accounts.window_tracking_enabled 启用后记录）';

-- 每账号每窗口类型至多一行开放记录：记录器 upsert 的冲突目标
CREATE UNIQUE INDEX IF NOT EXISTS uq_account_window_usage_open
    ON account_window_usage_histories(account_id, window_type)
    WHERE finalized_at IS NULL;

-- 管理端统计弹窗的历史查询
CREATE INDEX IF NOT EXISTS idx_account_window_usage_history
    ON account_window_usage_histories(account_id, window_type, window_end DESC);

-- finalize 扫描 / 决断探测扫描
CREATE INDEX IF NOT EXISTS idx_account_window_usage_open_scan
    ON account_window_usage_histories(window_end) WHERE finalized_at IS NULL;

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS window_tracking_enabled BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN accounts.window_tracking_enabled IS
    'opt-in 滚动窗口用量历史记录（启用会增加上游用量 API 调用）';

CREATE INDEX IF NOT EXISTS idx_accounts_window_tracking_enabled
    ON accounts(window_tracking_enabled) WHERE window_tracking_enabled;
