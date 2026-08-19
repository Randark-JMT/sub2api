//go:build unit

package service

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

func TestWindowTrackable(t *testing.T) {
	cases := []struct {
		name     string
		platform string
		mode     string // credentials["account_mode"]；空 = 未设置
		want     bool
	}{
		{"anthropic always trackable", domain.PlatformAnthropic, "", true},
		{"kimi coding plan", domain.PlatformKimi, "coding", true},
		{"zhipu coding plan", domain.PlatformZhipu, "coding", true},
		{"kimi payg", domain.PlatformKimi, "payg", false},
		{"deepseek no mode set", domain.PlatformDeepseek, "", false},
		// OpenAI 的用量查询是真实推理请求，绝不探测
		{"openai excluded", domain.PlatformOpenAI, "", false},
		{"gemini excluded", domain.PlatformGemini, "", false},
		{"grok excluded", domain.PlatformGrok, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{Platform: tc.platform}
			if tc.mode != "" {
				account.Credentials = map[string]any{"account_mode": tc.mode}
			}
			if got := WindowTrackable(account); got != tc.want {
				t.Fatalf("WindowTrackable(%s, mode=%q) = %v, want %v", tc.platform, tc.mode, got, tc.want)
			}
		})
	}
	if WindowTrackable(nil) {
		t.Fatal("nil account must not be trackable")
	}
}

func TestValidateWindowTrackingEnable(t *testing.T) {
	openai := &Account{Platform: domain.PlatformOpenAI}

	// 不适用平台上「新开启」必须拒绝
	if err := ValidateWindowTrackingEnable(openai, false, true); err == nil {
		t.Fatal("enabling on a non-trackable platform must be rejected")
	}
	// 关闭永远允许（遗留 true 也要能关掉）
	if err := ValidateWindowTrackingEnable(openai, true, false); err != nil {
		t.Fatalf("disabling must always be allowed: %v", err)
	}
	// 遗留 true 原样再提交不报错：不能卡住该账号的其他字段编辑
	if err := ValidateWindowTrackingEnable(openai, true, true); err != nil {
		t.Fatalf("resubmitting a legacy true must not block edits: %v", err)
	}

	anthropic := &Account{Platform: domain.PlatformAnthropic}
	if err := ValidateWindowTrackingEnable(anthropic, false, true); err != nil {
		t.Fatalf("enabling on anthropic must be allowed: %v", err)
	}
	if err := ValidateWindowTrackingEnable(nil, false, true); err != nil {
		t.Fatalf("nil account is a no-op: %v", err)
	}
}
