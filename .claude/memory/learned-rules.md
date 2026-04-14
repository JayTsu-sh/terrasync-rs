# Learned Rules

Rules that graduated from observations and corrections. Loaded at session start.
Max 50 lines. Rules beyond that should be promoted to CLAUDE.md or rules/ files.
Each rule includes a source annotation AND a machine-checkable verify line.

---

<!-- Example format:
- Never use the spread pattern to merge options in fetchJSON.
    verify: Grep("\.\.\.options", path="src/api/client.js") → 0 matches
    [source: corrected 2x, 2026-03-28]

- All service functions must return Result<T>.
  verify: Grep("export.*function.*Promise<(?!Result)", path="src/services/") → 0 matches
  [source: verified observation, 2026-04-01]
-->