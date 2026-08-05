# Memory System

This directory is Claude's learning infrastructure. It captures observations, corrections, and graduated rules across sessions.

## How It Works

Session start
    |
    v
VERIFICATION SWEEP   <-- Runs every rule's verify: check
    |
    v
Session activity
    |
    v
observations.jsonl   <-- Verified discoveries (not guesses)
corrections.jsonl    <-- User corrections (with auto-generated checks)
violations.jsonl     <-- Rule violations caught by verification
sessions.jsonl       <-- Session scorecards and trend data
    |
    v
/project:evolve      <-- Periodic review (run manually)
    |
    v
learned-rules.md     <-- Graduated patterns WITH verify: checks
    |
    v
CLAUDE.md / rules/   <-- Promoted to permanent config

## File Purposes

### observations.jsonl
Append-only log. One JSON object per line. Claude writes here when it discovers something non-obvious.

Example entries:
{"timestamp": "2026-03-28T14:30:00Z", "type": "convention", "observation": "All service functions return Promise<Result<T>>", "file_context": "src/services/payment.ts", "confidence": "high"}
{"timestamp": "2026-03-28T15:10:00Z", "type": "gotcha", "observation": "The Stripe SDK timeout is 5s, not the documented 30s", "file_context": "src/services/stripe.ts", "confidence": "confirmed"}

Types: convention, gotcha, dependency, architecture, performance, pattern
Confidence: low (inferred), medium (observed once), high (observed multiple times), confirmed (user validated)

### corrections.jsonl
Append-only log. Claude writes here when the user corrects its behavior.

Example:
{"timestamp": "2026-03-28T16:00:00Z", "correction": "Don't use ternary operators in this project", "context": "Was writing a ternary in a handler", "category": "style", "times_corrected": 1}

Categories: style, architecture, security, testing, naming, process, behavior

###violations.jsonl
Append-only log. Records every rule violation caught by the verification sweep. Used by /project:evolve to identify rules that need escalation (recurring violations mean the rule should graduate to CLAUDE.md or become a linter rule).

###sessions.jsonl
Session scorecards. One entry per session. Tracks corrections received, rules checked/passed/failed, observations made. Used for trend detection: are corrections decreasing over time? If not, the rules aren't working.

The times_corrected field tracks repeat corrections. When this reaches 2 for the same pattern, it auto-promotes to learned-rules.md without waiting for /project:evolve.

### learned-rules.md
Curated rules that graduated from observations and corrections. Claude reads this file at the start of complex tasks. Rules here have been validated by repetition (corrected 2+ times) or explicit approval during /project:evolve.

### evolution-log.md
Audit trail of every /project:evolve run. Records what was proposed, approved, rejected, and why. Prevents the system from re-proposing rejected rules.

## Rules for Writing to Memory

1. Observations are cheap. Log liberally. Low-confidence observations are fine.
2. Corrections are gold. Every correction gets logged. No exceptions.
3. Learned rules are expensive. They load into context every session. Each must be actionable, testable, and non-redundant.
4. Never delete correction logs. They're provenance.
5. Learned rules max at 50 lines. Forces graduation or pruning.

## Promotion Ladder

| Signal | Destination |
|--------|------------|
| Corrected once | corrections.jsonl (logged) |
| Corrected twice, same pattern | learned-rules.md (auto-promoted) |
| Observed 3+ times, same pattern | learned-rules.md (via /project:evolve) |
| In learned-rules 10+ sessions, always followed | Candidate for CLAUDE.md or rules/ |
| Rejected during evolve | evolution-log.md (never re-proposed) |

## 提升到 rules/ 的判断标准

从 `learned-rules.md` 提升到 `.claude/rules/` 时，选择正确的目标文件：

| 规则类型 | 目标文件 | 加载时机 |
|---------|---------|---------|
| Rust 编码约束（use 语句、错误处理、重构规范） | `rules/rust-patterns.md` | 始终 |
| E2E 测试执行规范 | `tests/lab/README.md` | 修改 Nightly lab 时 |
| Web API 设计规范（axum handler 模式） | `rules/api-design.md` | 接触 web/src/ 文件时 |
| 每次会话都必须知道的铁律 | `CLAUDE.md` | 每次（谨慎，保持 <200 行）|

rules/ 文件格式：
- 无 frontmatter = 始终加载
- 有 `paths:` frontmatter = path-scoped，只在接触指定路径时加载

提升流程：
1. 在 learned-rules.md 中观察规则稳定 10+ session
2. 运行 `/evolve` 审查，确认规则无争议
3. 移入对应 rules/ 文件（或更新现有文件）
4. 从 learned-rules.md 中删除（避免重复）
