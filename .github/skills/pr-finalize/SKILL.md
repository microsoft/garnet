---
name: pr-finalize
description: Finalizes any PR for merge by verifying title/description match implementation AND performing code review for best practices. Use when asked to "finalize PR", "check PR description", "review commit message", before merging any PR, or when PR implementation changed during review. Do NOT use for extracting lessons or investigating build failures.
---

# PR Finalize

Ensures PR title and description accurately reflect the implementation, and performs a **code review** for Garnet best practices before merge.

**Standalone skill** — Can be used on any PR.

## Two-Phase Workflow

1. **Title & Description Review** — Verify PR metadata matches implementation
2. **Code Review** — Review code for Garnet-specific best practices and potential issues

---

## 🚨 CRITICAL RULES

### 1. NEVER Approve or Request Changes

**AI agents must NEVER use `--approve` or `--request-changes` flags.**

| Action | Allowed? | Why |
|--------|----------|-----|
| `gh pr review --approve` | ❌ **NEVER** | Approval is a human decision |
| `gh pr review --request-changes` | ❌ **NEVER** | Blocking PRs is a human decision |

### 2. NEVER Post Comments Directly

**This skill is ANALYSIS ONLY.** Never post comments using `gh` commands.

| Action | Allowed? | Why |
|--------|----------|-----|
| `gh pr review --comment` | ❌ **NEVER** | Present findings to the user instead |
| `gh pr comment` | ❌ **NEVER** | Present findings to the user instead |
| Analyze and report findings | ✅ **YES** | This is the skill's purpose |

**Only humans control when comments are posted.** Your job is to analyze and present findings.

---

## Phase 1: Title & Description

### Core Principle: Preserve Quality

**Review existing description BEFORE suggesting changes.** Many PR authors write excellent, detailed descriptions. Your job is to:

1. **Evaluate first** — Is the existing description good? Better than a template?
2. **Preserve quality** — Don't replace a thorough description with a generic template
3. **Enhance, don't replace** — Add missing required elements (issue links, test info) without rewriting good content
4. **Only rewrite if needed** — When description is stale, inaccurate, or missing key information

## Usage

```bash
# Get current state (no local checkout required)
gh pr view XXXXX --json title,body
gh pr view XXXXX --json files --jq '.files[].path'

# Review commit messages (helpful for squash/merge commit quality)
gh pr view XXXXX --json commits --jq '.commits[].messageHeadline'

# Review actual code changes
gh pr diff XXXXX

# Optional: if the PR branch is checked out locally
git diff origin/main...HEAD
```

## Evaluation Workflow

### Step 1: Review Existing Description Quality

Before suggesting changes, evaluate the current description:

| Quality Indicator | Look For |
|-------------------|----------|
| **Structure** | Clear sections, headers, organized flow |
| **Technical depth** | File-by-file changes, specific code references |
| **Scannability** | Easy to find what changed and where |
| **Accuracy** | Matches actual diff — not stale or incorrect |
| **Completeness** | Breaking changes, performance impact, testing info |

### Step 2: Compare to Template

Ask: "Is the existing description better than what my template would produce?"

- **If YES**: Keep existing, only add missing required elements
- **If NO**: Suggest improvements or replacement

### Step 3: Produce Output

- Recommended PR title (if change needed)
- Assessment of existing description
- Specific additions needed (e.g., "Add issue link", "Mention breaking change")
- Only full replacement if description is inadequate

## Title Requirements

**The title becomes the commit message headline.** Make it searchable and informative.

| Requirement | Good | Bad |
|-------------|------|-----|
| Component prefix (if specific) | `[Cluster] Fix gossip protocol timeout` | `Fix timeout` |
| Describes behavior, not issue | `[RESP] ZADD: Support GT/LT flags` | `Fix #123` |
| Captures the "what" | `[Tsavorite] Reduce lock contention in RMW` | `Fix perf bug` |
| Notes breaking change if applicable | `(breaking)` | (omitted) |
| No noise prefixes | `[Storage] Fix...` | `[PR agent] Fix...` |

### Title Formula

```
[Component] What changed (breaking if applicable)
```

Component prefixes (use when change is scoped):
- `[RESP]` — RESP command parsing/dispatch (`libs/server/Resp/`)
- `[Storage]` — Storage session/functions (`libs/server/Storage/`)
- `[Tsavorite]` — Tsavorite engine (`libs/storage/Tsavorite/`)
- `[Cluster]` — Cluster/replication/sharding (`libs/cluster/`)
- `[Objects]` — Object types: Hash, List, Set, SortedSet (`libs/server/Objects/`)
- `[API]` — Garnet API surface (`libs/server/API/`)
- `[Network]` — Networking/TLS (`libs/common/Networking/`)
- `[Config]` — Configuration/options (`libs/host/Configuration/`)
- `[Tests]` — Test-only changes
- `[Docs]` — Documentation-only changes
- Omit prefix for cross-cutting changes

Examples:
- `[RESP] ZADD: Support GT/LT flags for conditional updates`
- `[Tsavorite] Reduce epoch protection overhead in hot-path RMW`
- `[Cluster] Fix replication lag during key migration`
- `Add multi-database support for standalone mode`

## Description Requirements

PR description should:
1. Link to the GitHub Issue
2. Describe what changed and why
3. Match the actual implementation

```markdown
### Description of Change

[Must match actual implementation]

### Issues Fixed

Fixes #XXXXX
```

## Content for Future Agents

**The title and description become the commit message.** Future agents searching git history will use this to understand:
- What changed and why
- What patterns to follow or avoid
- How this change affects related code

### Required Elements for Agent Success

| Element | Purpose | Example |
|---------|---------|---------|
| Component in title | Scoped search | `[Tsavorite] ...` |
| Root cause (bug fixes) | Understand failure mode | "Epoch was not released on error path" |
| Description of change | What code does now | "Added GT/LT flag parsing in ZADD handler" |
| Key types/interfaces | API surface awareness | `IGarnetApi`, `StorageSession`, `CustomRawStringFunctions` |
| What NOT to do | Prevent repeat mistakes | "Don't allocate on RMW hot path" |

### Recommended Elements

| Element | When to Include |
|---------|----------------|
| **Root cause** | Bug fixes — explain why the bug occurred |
| **Key technical details** | Complex changes — list affected types and interfaces |
| **What NOT to do** | When failed approaches were attempted |
| **Edge cases** | When behavior differs across scenarios |
| **Performance impact** | When change affects hot paths or memory allocation |
| **Breaking changes** | When API or behavior changes affect consumers |
| **Migration guide** | When users/extensions need to update |

## Description Template (for Inadequate Descriptions)

Use this only when the existing description is stale, inaccurate, or missing key information:

```markdown
### Root Cause

[Why the bug occurred — be specific about the code path]

### Description of Change

[What the code now does]

**Key changes:**
- [Change 1]
- [Change 2]

### Key Technical Details

**Affected types/interfaces:**
- `TypeA` — [What it does]
- `TypeB` — [What it does]

### What NOT to Do (for future agents)

- ❌ **Don't [approach 1]** — [Why it fails]
- ❌ **Don't [approach 2]** — [Why it's wrong]

### Edge Cases

| Scenario | Risk | Mitigation |
|----------|------|------------|
| [Case 1] | Low/Medium/High | [How to handle] |

### Issues Fixed

Fixes #XXXXX
```

## Quality Comparison Examples

### Good Existing Description (KEEP)

```markdown
## Changes

### `libs/server/Resp/Objects/SortedSetCommands.cs`
- Added GT/LT flag parsing in ZADD command handler
- Flag validation against NX (mutually exclusive)

### `libs/server/Objects/SortedSet/SortedSetObjectImpl.cs`
- Implemented conditional update logic in SortedSetAdd
- GT: only update if new score > current; LT: only if new score < current

### `libs/server/Storage/Session/ObjectStore/SortedSetOps.cs`
- Passed flags through ObjectInput to the object implementation

## Tests Added
- `RespSortedSetTests.ZAddWithGTFlag` — verifies GT-only updates
- `RespSortedSetTests.ZAddWithLTFlag` — verifies LT-only updates
- `RespSortedSetTests.ZAddGTNXMutuallyExclusive` — verifies error on GT+NX
```

**Verdict:** Excellent — file-by-file breakdown, specific changes, tests listed. Keep it.

### Poor Existing Description (REWRITE)

```markdown
Fixed the issue mentioned in #456
```

**Verdict:** Inadequate — no detail on what changed. Use template.

---

## Phase 2: Code Review

After verifying title/description, perform a **code review** to catch Garnet-specific issues and general best practice violations before merge.

### Review Focus Areas

When reviewing code changes in Garnet, focus on:

1. **Performance and memory safety**
   - Unnecessary heap allocations on hot paths (prefer `Span<T>`, `SpanByte`, stack allocation)
   - Missing `[MethodImpl(MethodImplOptions.AggressiveInlining)]` on hot-path methods
   - Missing `[MethodImpl(MethodImplOptions.NoInlining)]` on cold/exception-throwing methods
   - Blocking calls or unnecessary copies in RESP command handlers

2. **Epoch management**
   - `LightEpoch` acquired but not released on error paths
   - Epoch ownership — only dispose if owned
   - Shared epochs in parallel test scenarios

3. **RESP protocol correctness**
   - Argument parsing via `parseState.GetArgSliceByRef(i)` returning `ref PinnedSpanByte`
   - Correct RESP response format (using `RespWriteUtils` helpers)
   - Proper `SendAndReset()` calls to flush response buffer
   - Command dispatch wired in `ProcessBasicCommands`/`ProcessArrayCommands`

4. **Thread safety and concurrency**
   - Proper lock usage (`TryWriteLock()` in spin loops, not `CloseLock()`)
   - Safe concurrent access to shared state
   - Session-local vs shared state boundaries

5. **Test quality**
   - `[AllureNUnit]` attribute and `AllureTestBase` inheritance on test fixtures
   - `TestUtils.OnTearDown()` called in `[TearDown]` (checks for leaked epochs)
   - `TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true)` in `[SetUp]`
   - Both `StackExchange.Redis` and `LightClient` coverage where applicable

6. **Code conventions**
   - File header: `// Copyright (c) Microsoft Corporation.` / `// Licensed under the MIT license.`
   - `TreatWarningsAsErrors` — no new warnings introduced
   - XML doc comments on public methods
   - Consistent naming (camelCase private fields, PascalCase constants/statics)

7. **Breaking changes and API surface**
   - Changes to `IGarnetApi` / `IGarnetReadApi` / `IGarnetAdvancedApi`
   - Changes to custom extension base classes (`CustomRawStringFunctions`, `CustomObjectBase`, etc.)
   - Configuration option changes in `GarnetServerOptions`

### How to Review

```bash
# Get the PR diff
gh pr diff XXXXX

# Review specific files
gh pr diff XXXXX -- path/to/file.cs

# Check CI status
gh pr view XXXXX --json statusCheckRollup
```

### Output Format

```markdown
## Code Review Findings

### 🔴 Critical Issues

**[Issue Title]**
- **File:** [path/to/file.cs]
- **Problem:** [Description]
- **Recommendation:** [Code fix or approach]

### 🟡 Suggestions

- [Suggestion 1]
- [Suggestion 2]

### ✅ Looks Good

- [Positive observation 1]
- [Positive observation 2]
```

### 🚨 CRITICAL: Do NOT Post Comments Directly

**The pr-finalize skill is ANALYSIS ONLY.** Never post comments using `gh pr review` or `gh pr comment`.

| Action | Allowed? | Why |
|--------|----------|-----|
| `gh pr review --comment` | ❌ **NEVER** | Present findings to the user instead |
| `gh pr comment` | ❌ **NEVER** | Present findings to the user instead |
| Analyze and report findings | ✅ **YES** | This is the skill's purpose |

**Workflow:**
1. **This skill**: Analyze PR, produce findings in your response
2. **User asks to post**: User decides whether and how to share feedback

The user controls when comments are posted. Your job is to analyze and present findings.

---

## Complete Example

See [references/complete-example.md](references/complete-example.md) for a full agent-optimized PR description showing all elements above applied to a real Garnet change.
