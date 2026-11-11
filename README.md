# oh-my-raven

`oh-my-raven` is a multi-agent orchestration plugin for Raven. It separates logical agent roles (`SpecialistDefinition`) from physical CLI and ACP execution backends (`RoutingBackend`), isolates parallel writes in detached Git worktrees (`WorktreeManager`), and blocks unreviewed or partially wired code from completing (`ReviewGateValidator` and `RequirementTraceabilityGate`).

## Why `oh-my-raven` exists: the 8 pain points in single-agent and coupled multi-agent workflows

Most multi-agent setups fail in production because they bind role prompts to specific CLI binaries, share a mutable working tree across parallel workers, and accept self-reported completion claims. `oh-my-raven` addresses eight specific failure modes.

1. **Specialists tightly coupled to a single runtime binary.** In coupled setups, a `Researcher` or `Reviewer` is hardcoded to one binary, or skill sets such as `superpowers`, `ecc`, and `mattpocock` live inside one tool profile. If that binary hits a rate limit or quota ceiling, the specialist is dead. `oh-my-raven` defines each specialist with `kind = "specialist"`, zero `command`, and zero hardcoded fallback lists. `SkillPackResolver` injects portable `skill_packs` and `mcp_servers` into whichever runtime backend takes the task.
2. **Single-agent bottleneck by default.** Standard agent loops send the entire prompt to one coding worker. That worker plans, edits files, and grades its own work. `oh-my-raven` routes feature work through a typed DAG (`build_oh_my_raven_playbook()`) with dedicated `plan`, `write`, `integrate`, and `review` nodes.
3. **Serial execution of parallelizable work, or parallel agents corrupting a shared Git checkout.** Running two write tasks in one directory mixes uncommitted edits and causes index lock failures. Running them one by one wastes wall-clock time. `oh-my-raven` validates `file_scope` globs statically before execution (`scopes_overlap()`) and provisions a detached Git worktree per write node (`<repo>.worktrees/<run_id>-<node_id>`).
4. **Task halts on worker crash or timeout.** When a CLI or ACP subagent times out or exits non-zero, standard runners abort the turn and wait for a human nudge. `RoutingBackend` catches retryable failures (`TimeoutError`, `CliAgentExitError`, `SubagentNoAnswerError`), checks whether the worktree is clean, places the failed target on a timed cooldown, and fails over to the next candidate in the runtime pool.
5. **Tunnel vision and zero ecosystem awareness.** Subagents normally receive only the local task string. They miss active workstreams, open incidents, and architectural constraints. `build_context_charter()` reads open threads (`hot.md`) and long-term project memories, redacts secrets, and prepends a `[BEGIN UNTRUSTED CONTEXT CHARTER]` block to the prompt. At the root of the DAG, the `CTO` node synthesizes a `CtoStrategicBrief` (`workstream_synergies`, `long_term_goal_alignment`, `cross_project_dependencies`) that `DagSpecialistRuntime.inject_charter()` passes to every downstream `write`, `integrate`, and `review` node.
6. **Blind implementation without second-order consequence analysis.** Agents routinely change schemas or public signatures without checking callers or rollback steps. `oh-my-raven` blocks any `write` node from starting until its upstream `plan` node produces a validated 5-dimension `ConsequenceMatrix` (`affected_files`, `blast_radius`, `failure_modes`, `rollback_plan`, `contract_changes`, `validation_plan`).
7. **Code reviews only happen when a human nudges, or the same model rubber-stamps its own diff.** `ReviewGateValidator` requires a downstream `review` node for every `write` workflow. It compares `ExecutionIdentity` across author and reviewer nodes so the same agent, binary+profile pair, or model+endpoint pair cannot approve its own code. It also verifies that `reviewed_head_sha` matches the candidate commit and caps `changes_requested` loops through `ReviewCycleTracker`.
8. **Premature "Complete" declarations with gaps, deferred milestones, or test-only unwired helpers.** Agents often write a helper class, import it only in `tests/`, or defer hard requirements to "Phase 2" while reporting 100% completion. `RequirementTraceabilityGate` inspects the `AcceptanceReceipt` at review time. It rejects any receipt with `unmet_requirements` or `deferred_requirements`, enforces a 1:1 bijection against `requirements_manifest.json`, and parses the target Python file with `ast.parse()` to confirm that `entrypoint_caller` exists in a production module outside `tests/`.

## Architecture: microservice specialists on a dynamic runtime pool

`oh-my-raven` treats specialists like stateless microservices and CLI/ACP agents (`Claude Code`, `Codex`, `GitHub Copilot`, `Hermes`, `Raven-Code`) like interchangeable worker VMs in a shared pool.

```text
+-------------------------------------------------------------------------+
|                       oh-my-raven DAG Orchestrator                      |
|                                                                         |
|  [cto_strategy] (subagent="CTO", execution_kind="plan")                 |
|        |  Validates CtoStrategicBrief + 5-dim ConsequenceMatrix         |
|        +-----------------------+-----------------------+                |
|        v                                               v                |
|  [slice_a] (kind="write")                        [slice_b] (kind="write"|
|  Worktree: .worktrees/<run>-slice_a              Worktree: <run>-slice_b|
|  Scope: raven/agent/subagent/**                  Scope: tests/**        |
|        |                                               |                |
|        +-----------------------+-----------------------+                |
|                                |  SliceReceipt (SHA + changed_paths)    |
|                                v                                        |
|                     [integrate] (kind="integrate")                      |
|                     Deterministic git cherry-pick                       |
|                                |                                        |
|                                v                                        |
|                      [review] (kind="review")                           |
|          ReviewGateValidator (ExecutionIdentity != author)              |
|          RequirementTraceabilityGate (AST symbol + 1:1 ID check)        |
+-------------------------------------------------------------------------+
                                 |
                                 | Dynamic pool resolution + SkillPackResolver
                                 v
+-------------------------------------------------------------------------+
|                   Dynamic Runtime Pool (RoutingBackend)                 |
|  Selects healthy backend by capability, cooldown, and _workload_score   |
|                                                                         |
|  +--------------+  +--------------+  +--------------+  +--------------+ |
|  | Claude Code  |  |    Codex     |  | Copilot CLI  |  |    Hermes    | |
|  | (is_harness) |  | (is_harness) |  | (is_harness) |  | (is_harness) | |
|  +--------------+  +--------------+  +--------------+  +--------------+ |
+-------------------------------------------------------------------------+
```

### 1. Define a specialist with zero binary coupling

In Raven's configuration (`SpecialistSubagentConfig`), a specialist declares what it knows (`role_prompt`, `skill_packs`, `mcp_servers`) and what physical capabilities it needs (`required_capabilities`). It does not declare a `command` or a static `routes` list.

```toml
[[subagents.agents]]
name = "Engineer - Superpowers"
kind = "specialist"
description = "TDD implementation specialist"
role_prompt = "Execute with strict TDD and verification before completion."
skill_packs = ["superpowers", "ecc", "mattpocock"]
mcp_servers = ["consensus"]
required_capabilities = ["local-files"]

[[subagents.agents]]
name = "CTO"
kind = "specialist"
description = "Cross-workstream architecture and consequence analysis"
role_prompt = "Align feature slices with active workstreams and verify blast radius."
skill_packs = ["agent-skills", "pstack"]
required_capabilities = ["local-files"]
```

When `SubagentTable.apply()` loads the agent table, it discovers every enabled backend with `is_harness = True` that satisfies `required_capabilities` and binds the specialist to that pool. Adding a new CLI or ACP backend to `subagents.agents` makes it available to every specialist on the next run without editing specialist definitions.

### 2. Portable skill pack and MCP injection (`SkillPackResolver`)

Before a task runs on the chosen backend, `SkillPackResolver.render_specialist_bundle()` resolves the specialist's `role_prompt`, `soul_path`, and `skill_packs` (`superpowers`, `ecc`, `mattpocock`, `agent-skills`, `pstack`, `open-code-review`). It searches configured `skill_dirs`, `~/.raven/workspace/skills`, local profile skill directories, and the built-in portable library, redacts any secret patterns, and prepends a `[BEGIN SPECIALIST BUNDLE: <name>]` block to the task. `RoutingBackend.run()` also merges the specialist's `mcp_servers` into the backend call's `mcps` grant.

### 3. Active CTO strategic alignment (`CtoAlignmentGate`)

When a plan node is assigned to `CTO` (or when `require_cto_alignment=True` is set on `DagSpecialistRuntime`), `after_success()` validates a `CtoStrategicBrief` payload before any `write` node can start:

```json
{
	"cto_strategic_brief": {
		"workstream_synergies": [
			"t18: Share dynamic runtime pool across all specialist roles",
			"t7: Inject portable superpowers and ecc skill packs at dispatch time"
		],
		"long_term_goal_alignment": "Decouple logical specialist definitions from physical CLI/ACP runners",
		"cross_project_dependencies": [
			"raven.config.schema",
			"raven.agent.subagent.registry"
		],
		"consequence_matrix": {
			"affected_files": [
				"raven/agent/subagent/oh_my_raven.py",
				"raven/agent/subagent/backends/routing.py"
			],
			"blast_radius": "subagent DAG runner and routing layer",
			"failure_modes": ["empty runtime pool", "dirty worktree on timeout"],
			"rollback_plan": "git revert of the integration commit",
			"contract_changes": ["Added SpecialistSubagentConfig and CtoStrategicBrief"],
			"validation_plan": ["python -X utf8 -m pytest tests/test_specialist_orchestration_e2e.py -q"]
		}
	}
}
```

### 4. Isolated Git worktrees and deterministic integration (`WorktreeManager`)

For each `write` node in the DAG:

1. `validate_and_order()` checks all parallel `write` nodes with `scopes_overlap()`. If two concurrent nodes can touch the same path, the DAG fails validation before spawning a single process.
2. `WorktreeManager.provision_worktree()` verifies a clean baseline commit and creates a detached worktree at `<repo>.worktrees/<run_id>-<node_id>`.
3. After the node exits, `WorktreeManager.build_slice_receipt()` runs `git diff` and `git status --porcelain=v1 -z -uall` to confirm that every modified or untracked file matches the node's `file_scope`. Out-of-scope edits raise `ScopeViolationError`.
4. The downstream `integrate` node calls `WorktreeManager.integrate_slices()`, which cherry-picks each `SliceReceipt.head_sha` in topological order into a clean integration worktree. If a cherry-pick conflicts, it aborts the cherry-pick and raises `IntegrationConflictError`.

### 5. Independent review and requirement traceability (`ReviewGateValidator` and `RequirementTraceabilityGate`)

A `review` node must return a `ReviewReceipt` (and, when requirements are tracked, an `AcceptanceReceipt`):

```python
from pathlib import Path
from oh_my_raven import (
	AcceptanceReceipt,
	RequirementItem,
	RequirementTraceabilityGate,
)

receipt = AcceptanceReceipt(
	feature_name="oh-my-raven",
	requirements=[
		RequirementItem(
			id="R1",
			description="Enforce AST-verified production entrypoint wiring",
			status="met",
			entrypoint_caller="oh_my_raven/oh_my_raven.py:RequirementTraceabilityGate.verify_acceptance_receipt",
			e2e_command='python -X utf8 -m pytest tests/test_specialist_orchestration_e2e.py -o addopts="" -q',
			evidence_ref="docs/specialist_mvp_experiment_report.json",
		),
	],
)

RequirementTraceabilityGate.verify_acceptance_receipt(
	receipt,
	repo_root=Path("."),
	expected_requirement_ids=["R1"],
)
```

`RequirementTraceabilityGate.verify_acceptance_receipt()` fails with `PrematureCompletionError` if:

- `requirements` is empty, or `unmet_requirements` or `deferred_requirements` is non-empty.
- Any requirement ID is duplicated, missing from `expected_requirement_ids` (or `requirements_manifest.json`), or added as an unrequested extra ID.
- `entrypoint_caller` points to a test path (`tests/`, `test_*.py`, `*_test.py`, `conftest.py`), escapes `repo_root` via `..`, or names a class or function symbol that does not exist in the file's Python AST.

## Repository layout

- `oh_my_raven/oh_my_raven.py`: Core specialist definitions, `OmrTaskQueue`, `OmrDispatcher`, `DynamicDecompositionGate`, `SkillAndToolReceiptGate`, `SkillPackResolver`, `CtoStrategicBrief`, `CtoAlignmentGate`, `WorktreeManager`, `DurableRecoveryLedger`, `ReviewGateValidator`, `RequirementTraceabilityGate`, and `DagSpecialistRuntime`.
- `oh_my_raven/routing.py`: `RoutingBackend`, `RouteTarget`, ordered failover, dirty-worktree failover guard, timed cooldowns, balanced multi-harness load distribution & workload-learning score (`_workload_score()`, `_target_rank_key()`), and process-safe `TelemetryLogger`.
- `oh_my_raven/specialist_support.py`: Re-export module for `raven.agent.subagent.specialist_support` compatibility.
- `playbooks/oh-my-raven/playbook.md`: Declarative v2 DAG playbook (`cto_strategy -> slice_a, slice_b -> integrate -> review -> remediate -> re_review`).
- `docs/oh_my_raven_spec.md`: Architecture specification.
- `docs/specialist_mvp_experiment_report.json`: Recorded outputs from the six live verification experiments (`R1` through `R23`).
- `scripts/run_specialist_mvp_experiment.py`: Script that runs the live failover, worktree isolation, review gate, playbook validation, 23-item requirement traceability (`R1-R23`), and dynamic pool discovery experiments.
- `tests/test_omr_repeatable_e2e_matrix.py`: Repeatable end-to-end verification matrix covering all 11 owner criteria, 6 adversarial anti-gaming traps, and the $7 \times 5$ (35-permutation) Specialist-on-Any-Harness matrix + hot-onboarding (`Cursor-ACP`) + mid-flight crash failover.
- `tests/test_specialist_orchestration_e2e.py`: End-to-end tests for worktree isolation, scope enforcement, CTO strategic brief propagation, review identity checks, recovery ledger quarantine, and AST requirement traceability.
- `tests/test_subagent_routing_backend.py`: Unit and integration tests for `RoutingBackend`, failover chains, dirty-worktree blocks, telemetry tail recovery, and dynamic pool discovery.

## Verification receipts

### Repeatable 11-Point E2E, 6 Adversarial Traps & $7 \times 5$ Portability Matrix (`95 passed`)

Run the complete repeatable verification suite (`--omr-mode=deterministic|live`):

```bash
python -X utf8 -m pytest tests/test_omr_repeatable_e2e_matrix.py tests/test_specialist_orchestration_e2e.py tests/test_subagent_routing_backend.py -o addopts="" -v
```

Measured output:

```text
tests/test_omr_repeatable_e2e_matrix.py::test_01_idempotent_multi_run_repeatability PASSED
tests/test_omr_repeatable_e2e_matrix.py::test_02_to_06_full_intake_queue_routing_and_dynamic_decomposition_pipeline PASSED
tests/test_omr_repeatable_e2e_matrix.py::test_07_specialist_skill_packs_mcps_and_tool_receipt_gate PASSED
tests/test_omr_repeatable_e2e_matrix.py::test_08_cto_and_reviewer_remediation_and_rtm_ast_gate PASSED
tests/test_omr_repeatable_e2e_matrix.py::test_09_harness_pool_distribution_and_workload_learning PASSED
tests/test_omr_repeatable_e2e_matrix.py::test_10_tunnel_view_semantic_trap_and_secret_redaction PASSED
tests/test_omr_repeatable_e2e_matrix.py::test_11_seven_by_five_specialist_harness_matrix_hot_onboarding_and_crash_failover PASSED
...
95 passed in 163.84s (0:02:43)
```

### Live experiment receipts (`docs/specialist_mvp_experiment_report.json`)

Regenerate the experiment report with `python scripts/run_specialist_mvp_experiment.py`:

- **Experiment 1 (Ordered failover and dirty-worktree block):** Primary CLI exits with code 1; `RoutingBackend` fails over to `SecondaryCli` (`SECONDARY_SPECIALIST_OK`) and logs the attempt chain to `telemetry.jsonl`. When a non-idempotent write leaves `partial.txt` in the worktree after a timeout, `RoutingBackend` raises `SubagentFailoverBlockedError` instead of running the second agent over corrupt state.
- **Experiment 2 (Isolated Git worktrees and slice integration):** Provisions `exp-slice_a` and `exp-slice_b` worktrees from base commit `ccfc24ab`, verifies zero untracked file leakage from the dirty main checkout, commits scoped changes in each worktree, and cherry-picks both slices into `exp-integration`.
- **Experiment 3 (Review identity, recovery ledger, and secret redaction):** Rejects a reviewer sharing `(cli, claude.exe, p1)` with the author (`IndependentReviewError`), rejects an approval missing required `ocr` command evidence, persists `DurableRecoveryLedger` across instances, blocks the second `changes_requested` cycle (`ReviewCycleLimitError`), and redacts `ghp_*` tokens from `build_holistic_charter()`.
- **Experiment 4 (Playbook graph validation):** Builds and validates the 7-node `oh-my-raven` v2 DAG playbook (`validate_graph_nodes() == []`).
- **Experiment 5 (23-item Requirement Traceability Gate — `R1` through `R23`):** Verifies all 23 requirement items (`R1` through `R23`, including `R21: OmrDispatcher.submit_task`, `R22: DynamicDecompositionGate.validate_and_enqueue`, and `R23: SkillAndToolReceiptGate.verify_specialist_execution`) against live files and AST symbols (`23/23 met, 0 unmet, 0 deferred`), and confirms that deferred items, test-only callers, nested `/tests/` paths, and missing expected IDs raise `PrematureCompletionError`.
- **Experiment 6 (Decoupled specialist pool onboarding):** Confirms `Engineer - Superpowers` has `command = ""` and `routes = []`, auto-discovers enabled runtime harnesses on initial load, auto-discovers `HarnessThree` as soon as it is added to the table, and injects `superpowers`, `ecc`, and `mattpocock` skill packs into the rendered bundle.

### Desloppify mechanical scan receipt (`0` T1 / `0` T2 issues)

Command:

```bash
desloppify scan --path . --no-badge
```

Measured output:

```text
Desloppify Scan (python)
  Scores: overall 22.0/100  objective 88.0/100  verified 88.0/100
  Zones: production: 4, script: 1, test: 4
  [1/17] Unused (ruff)...          0 instances -> 0 issues
  [11/17] Private imports...       clean (5 files scanned)
  [13/17] Dict key flow...         0 dict key issues
  [17/17] Duplicates...            0 clusters (99.4% duplication health)
  Open T1 issues: 0
  Open T2 issues: 0
```

## License

MIT
