# OSS Leverage and Testing Pyramid

## 1. OSS Leverage Matrix: Active Today vs v3 Cutover

| OSS / pattern | Active use in `oh_my_raven` today | v3 seam or prerequisite |
|---|---|---|
| `portalocker>=2.8.0` | Cross-process locks protect `GatePolicy.persist_log` (`gate_log.json`) and `LessonsStore` (`lessons.jsonl`). | Retain around compatibility files until durable state moves behind subsystem adapters. |
| `opentelemetry-api>=1.20.0` | Native spans/events cover `omr.gate.<gate>`, `omr.routing.run`, `omr.routing.attempt`, and `omr.council.consensus`; compatible with local `grafana/otel-lgtm` OTLP endpoints `127.0.0.1:14317/14318`. | Add OpenTelemetry SDK/exporter packages on host; configure OTLP endpoint and resource attributes without coupling domain code to exporter setup. |
| `pydantic>=2.0.0` | Strict boundary schemas validate `CouncilVote`, `CouncilVerdict`, and gate receipts. | Keep Pydantic models at adapter boundaries; persisted workflow state remains JSON-compatible. |
| `oddur/HarnessRouter` + `tenacity` pattern | `routing.py` capability-scores targets, revives each target with bounded attempts (`DEFAULT_MAX_ATTEMPTS = 3`), then fails over across targets using `max(DEFAULT_MAX_ATTEMPTS, 1 + max_failovers)`. | Preserve routing API while replacing internal retry machinery only if an OSS router proves equivalent attempt receipts and limits. |
| `karpathy/llm-council` pattern | `evaluate_council_consensus` and `OmrDispatcher.run_council_consensus` implement specialist voting, ordered gap deduplication, and chair synthesis. Consensus needs approval threshold and zero blocking gaps. | Expand to three stages: independent specialist review, peer-visible aggregation, then chair synthesis; preserve deterministic verdict schema and receipt. |

### v3 four-subsystem cutover roadmap

| Subsystem | Exact adapter seam | Host prerequisite | Cutover rule |
|---|---|---|---|
| Beads / `bd` | Replace `OmrTaskQueue` persistence behind its enqueue/claim/status API with a `bd` CLI adapter. | Install `bd`; initialize repository database; make executable available on `PATH`. | Dual-read/compare first; retain JSONL export and rollback path until parity passes. |
| LangGraph `StateGraph` + `SqliteSaver` | Adapt `OmrDispatcher.submit_task` / DAG execution to compiled graph nodes; map queue/task state to typed graph state. | Install compatible `langgraph` and checkpoint packages; provide writable SQLite path. | Keep specialist dispatch, gate, and receipt contracts stable; replay representative interrupted runs before switching writes. |
| OpenTelemetry Exporter | Configure provider/exporter at process composition root; existing `_TRACER` calls remain unchanged. | Install `opentelemetry-sdk` and OTLP exporter; run `grafana/otel-lgtm`; configure `127.0.0.1:14317` gRPC or `:14318` HTTP. | Verify traces for gate, routing, attempt, and council spans before removing file-only telemetry. |
| FastMCP | Expose dispatcher operations through a thin MCP server adapter calling current public methods. | Install compatible `fastmcp`; configure transport, process supervision, and client registration. | MCP is transport only: validation, authorization, gates, and receipts stay in domain services. |

## 2. Multi-Scope Testing Pyramid

Tests deliberately widen from deterministic functions to real process, filesystem, Git, routing, recovery, and orchestration surfaces.

| Test file / class | Scope | Real collaborators exercised | Lose-Trust symptoms verified |
|---|---|---|---|
| `tests/test_gate.py` | Unit + component | Pydantic validation, portalocker-backed files, OpenTelemetry span API, dispatcher council receipt and lessons loop | #2 plan gaps; #7 missing multi-agent challenge; premature completion / unsafe writes |
| `tests/test_done_gate.py` | Unit + integration | Dispatcher, gate policy, filesystem evidence, constrained real pytest subprocess | Unverified done claims; unsafe external actions; fabricated evidence |
| `tests/test_failover.py` | Unit + integration | Capability router, bounded revival, multi-target failover, telemetry attempt events | Harness failure / brittleness; tunnel vision from single target |
| `tests/test_omr_multiscope_pyramid.py::TestIntegrationScope` | Integration | Triage, lessons, real DAG runner, decomposition and evidence roll-up | Over-orchestration, forgotten lessons, premature completion |
| `tests/test_omr_multiscope_pyramid.py::TestOutsideInScope` | Outside-in | Public dispatcher entry points, subprocess harness, env/file policy, failover and revival | Ambiguity loops, unsafe writes, harness outage |
| `tests/test_omr_multiscope_pyramid.py::TestEndToEndGoldenSet` | End-to-end | Five user scenarios across intake through persisted receipts | Golden coverage across the 10 Lose-Trust Symptoms |
| `tests/test_omr_repeatable_e2e_matrix.py` | End-to-end / repeatability | Full intake, queue, routing, decomposition, specialist matrix, crash failover, audit ledger and source-of-truth checks | All 10, including nondeterminism, secret leakage, semantic traps, workload imbalance |
| `tests/test_specialist_orchestration_e2e.py` | End-to-end | Real Git worktrees/merges, scope validation, recovery ledger, reviewer cycles, CTO strategy, RTM/AST gates | Conflicting edits, missing review, tunnel vision, lost recovery state, premature completion |

Pyramid policy: unit tests give fast schema and decision feedback; integration tests join owned components; outside-in tests enter through public APIs with real boundary adapters; end-to-end tests prove repeatable user outcomes. A feature is not considered proven solely by unit coverage.
