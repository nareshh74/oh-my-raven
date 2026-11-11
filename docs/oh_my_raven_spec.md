# oh-my-raven: Harness-Agnostic Specialist Orchestration & Anti-Premature-Completion Gate

## Overview

`oh-my-raven` (`raven.agent.subagent.oh_my_raven`, with backward-compatible re-exports in `raven.agent.subagent.specialist_support`) is Raven's harness-agnostic specialist orchestration layer where **Specialists (microservices)** are 100% decoupled from **Harnesses (VMs)**:

1. **Harness-Free Specialist Definition (`SpecialistSubagentConfig` / `kind = "specialist"`)**:
   - Specialists (`CTO`, `Manager`, `Reviewer`, `Researcher`, `Engineer - Superpowers`, `Engineer - ECC`, `Engineer - Matt Pocock`, `UX-Engineer`, `Creator`) are pure logical definitions in `raven/config/schema.py` and `raven/agent/subagent/registry.py`.
   - They require **no `command`** and **no hardcoded fallback `routes`**.
   - They declare `role_prompt` (or `soul_path`), portable `skill_packs` (e.g. `["superpowers", "ecc", "mattpocock", "agent-skills", "pstack", "open-code-review"]`), portable `mcp_servers` (e.g. `["consensus"]`), `required_capabilities` (physical execution needs only, e.g. `["local-files"]`), and `is_harness = False`.

2. **Dynamic Runtime Harness Pool Discovery (`SubagentTable` / `AgentRegistry` + `RoutingBackend`)**:
   - Physical execution engines (`Claude Code`, `Codex`, `GitHub Copilot`, `Hermes`, `Raven-Code`, etc.) have `is_harness = True` and form the shared **Runtime Harness Pool**.
   - When a specialist has `kind == "specialist"` (or `dynamic_harness_pool = True`) and no hardcoded `routes`, `SubagentTable._routing_entry()` automatically binds it to all enabled harnesses in the pool that satisfy `required_capabilities`.
   - Onboarding a new harness into `subagents.agents` automatically makes it available to **every specialist at runtime** without editing any specialist configuration.
   - `RoutingBackend` selects and fails over across the pool using cooldown state, capability checks, and `_workload_score()` (historical pass rate and latency from `specialist_telemetry.jsonl`).

3. **Portable Specialist Skill Packs & MCP Injection (`SpecialistDefinition` & `SkillPackResolver`)**:
   - Efficiency/methodology skills (`superpowers`, `ecc`, `mattpocock`, `agent-skills`, `pstack`, `open-code-review`) belong to the **Specialist**, never locking a task to a single harness.
   - `SkillPackResolver.render_specialist_bundle(specialist, task_prompt)` resolves the specialist's `role_prompt`/`soul_path`, portable skill pack instructions (from `skill_dirs`, `~/.raven/workspace/skills`, Hermes profile skills, or built-in portable skill pack library), and `mcp_servers`, injecting them into the task prompt and `mcps` grant on whichever harness executes the specialist at runtime.

4. **Active `CTO` Strategic Alignment & Cross-Workstream Synergy Gate (`CtoStrategicBrief` & `CtoAlignmentGate`)**:
   - Eliminates tunnel view by placing `cto_strategy` (`subagent="CTO"`, `execution_kind="plan"`) at the root of `oh-my-raven` feature delivery (`build_oh_my_raven_playbook()`).
   - The `CTO` node receives cross-workstream context via `build_context_charter()` (Obsidian Wiki `hot.md` open threads, EverOS long-term goals/decisions, and repo context) and must output a validated `CtoStrategicBrief` (`workstream_synergies`, `long_term_goal_alignment`, `cross_project_dependencies`, and a 5-dimension `ConsequenceMatrix`).
   - `DagSpecialistRuntime.after_success()` validates `CtoAlignmentGate.verify_cto_brief()`, blocks downstream `write` nodes if CTO alignment is missing, and automatically propagates `[BEGIN CTO STRATEGIC BRIEF]` into downstream `write`, `integrate`, and `review` nodes in `inject_charter()`.

5. **Worktree Isolation, Independent Review & Requirement Traceability (`WorktreeManager`, `ReviewGateValidator`, `RequirementTraceabilityGate`)**:
   - Concurrent `write` nodes run in isolated detached Git worktrees (`<repo>.worktrees/<run_id>-<node_id>`) with static `file_scope` overlap checks and deterministic cherry-pick integration (`integrate_slices`).
   - `ReviewGateValidator.verify_review_receipt()` enforces reviewer/author `ExecutionIdentity` separation, candidate SHA freshness, command evidence, and bounded remediation cycles (`ReviewCycleTracker`).
   - `RequirementTraceabilityGate.verify_acceptance_receipt()` enforces 100% requirement completion and AST-verified production entrypoint wiring (`no_test_only_wiring`).
