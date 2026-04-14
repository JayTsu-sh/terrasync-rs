---
name: evolution-engine
description: >
  Autonomous learning and verification system. Triggers on:
  - Session start (runs verification sweep on all learned rules)
  - User corrections ("no", "wrong", "I told you", "we don't do that")
  - Task completion (session scoring)
  - Discoveries during work (hypothesis verification)
  - User explicit ("remember this", "add this as a rule")
allowed-tools: Read, Write, Edit, Grep, Glob, Bash
---

# Evolution Engine

You are not a journal. You are an immune system. You verify, enforce, and learn autonomously.

---

## SECTION 1: VERIFICATION SWEEP (run at session start)

Before starting any complex task, run every rule's verification check. Do this silently. Only surface failures.

### How It Works

Read .claude/memory/learned-rules.md. Every rule that has a verify: line gets executed as a grep/glob/check. Rules without verify: lines are technical debt. Add one.

Example rule format in learned-rules.md:

 - Never use the spread pattern to merge options in fetchJSON.
    verify: Grep("\.\.\.options", path="src/api/client.js") → 0 matches
    [source: corrected 2x, 2026-03-28]

"The verify line isn't runnable code — it's an instruction Claude executes as a grep check during the verification sweep. Claude reads the pattern, runs the search, and checks the result matches the expectation."

### Verification Protocol

1. Read learned-rules.md. For each rule with a verify: line: - Run the check (Grep, Glob, or Read).
   - PASS: Silent. Move on.
   - FAIL: Log to .claude/memory/violations.jsonl: {"timestamp": "[now]", "rule": "[rule text]", "check": "[what was run]", "result": "[what was found]", "file": "[where]", "auto_fixed": false}
   - Surface violations to the user: RULE VIOLATIONS DETECTED:
     - [rule]: found [violation] in [file:line]
       fix: [specific fix]

2. If ALL checks pass, say nothing. The best immune system is invisible.

3. Track pass rates in .claude/memory/sessions.jsonl: {"date": "[today]", "rules_checked": 8, "rules_passed": 8, "rules_failed": 0, "violations": []}

### Rules Without Verification

If a rule in learned-rules.md has no verify: line, add one. Every rule must be machine-checkable. If you can't write a check for it, the rule is too vague. Rewrite it until you can.

Verification patterns:
- Code pattern banned: Grep("[pattern]", path="[scope]") → 0 matches
- Code pattern required: Grep("[pattern]", path="[scope]") → 1+ matches
- File must exist: Glob("[pattern]") → 1+ matches
- File must not exist: Glob("[pattern]") → 0 matches

---

## SECTION 2: HYPOTHESIS-DRIVEN OBSERVATIONS

Never log a guess. Verify it immediately or don't log it.

### Protocol

When you notice a pattern during work:

1. Formulate as testable claim. Not "I think store methods use this.set()" but "All state updates in store.js go through this.set()."

2. Test it immediately.
   - Grep for counter-examples.
   - Read the relevant files.
   - Count occurrences vs. exceptions.

3. Record with evidence.
   // .claude/memory/observations.jsonl
   {
     "timestamp": "[now]",
     "type": "convention",
     "hypothesis": "All state updates use this.set()",
     "evidence": "Grep found 23 this.set() calls, 0 direct assignments",
     "counter_examples": 0,
     "confidence": "confirmed",
     "file_context": "src/store.js",
     "verify": "Grep('this\\.[a-z]+ =(?!=)', path='src/store.js') → 0 matches"
   }

4. Auto-promote confirmed observations.
   - confirmed + 0 counter-examples → add directly to learned-rules.md WITH a verify line.
   - Tell the user: "Verified and added as rule: [rule]. Check: [verify pattern]."
   - No need to wait for /project:evolve.

5. Low-confidence observations still get logged but flagged: {"confidence": "low", "recheck_after": "3 sessions", ...}

---

## SECTION 3: CORRECTION CAPTURE (with auto-verification)

When the user corrects you:

1. Acknowledge naturally.

2. Log to .claude/memory/corrections.jsonl: {"timestamp": "[now]", "correction": "[what]", "context": "[what you were doing]", "category": "[category]", "times_corrected": 1, "verify": "[auto-generated check]"}

3. Generate a verify pattern immediately. If the correction is "don't do X", the check is Grep("[X pattern]") → 0 matches. If you can't generate a check, note "verify": "manual". This is debt to resolve during /project:evolve.

4. Same promotion rules: - 1st time: Log.
   - 2nd time (same pattern): Auto-promote to learned-rules.md WITH verify line.
   - Already in learned-rules: Check if verification exists. If not, add it now.

5. Apply the correction immediately.

---

## SECTION 4: SESSION SCORING (run at session end or before commit)

When wrapping up, write a session scorecard to .claude/memory/sessions.jsonl:

{
  "date": "[today]",
  "session_number": "[increment]",
  "corrections_received": 2,
  "rules_checked": 8,
  "rules_passed": 7,
  "rules_failed": 1,
  "violations_found": ["spread-options in new endpoint"],
  "violations_fixed": ["spread-options in new endpoint"],
  "observations_made": 1,
  "observations_verified": 1,
  "rules_added": 0
}

### Trend Detection

If sessions.jsonl has 5+ entries, check:
- Corrections decreasing? System is working.
- Corrections flat or increasing? Rules aren't being consulted or are too vague. Flag for /project:evolve.
- Same violation recurring? Needs to graduate from learned-rules to CLAUDE.md or become a linter rule.
- Rules growing past 40? Warn that graduation is needed.

Surface the trend in one line: "Session 12: 0 corrections (down from 3 avg). 8/8 rules passing."

---

## SECTION 5: EXPLICIT "REMEMBER THIS"

When the user asks you to remember something:

1. Rewrite it as a testable rule.
2. Generate a verify pattern.
3. Add to learned-rules.md: - [Rule text]
     verify: [check]
     [source: user-explicit, DATE]
4. Confirm: "Added rule: [rule]. Verification: [check]. Will auto-enforce from now on."

If you can't make it machine-checkable, say so: "Added as manual rule. I'll follow it but can't auto-verify. Consider rephrasing so I can write a grep check."

---

## CAPACITY MANAGEMENT

Before adding to learned-rules.md:
1. Count lines. Max 50.
2. If approaching 50, check which rules have passed 10+ consecutive sessions → candidates for graduation.
3. Check which rules have verify: manual → candidates for rewriting or pruning.
4. Suggest /project:evolve if at capacity.

---

## THE PRINCIPLE

A rule without a verification check is a wish.
A rule with a verification check is a guardrail.
Only guardrails survive.