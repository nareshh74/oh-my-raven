import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from oh_my_raven.gate import CouncilVote, LessonsStore, GatePolicy, classify_triage_tier, escalate_triage_tier, evaluate_council_consensus
from pathlib import Path
from types import SimpleNamespace

from oh_my_raven.oh_my_raven import (
    BUCKET1_STARTUP_MCPS,
    BUCKET1_STARTUP_PLUGINS,
    BUCKET1_STARTUP_SKILLS,
    SkillAndToolReceiptGate,
    BUCKET2_SPECIALIST_FAMILIES,
    HarnessStartupContractViolationError,
    HarnessStartupManager,
    HarnessStartupReceipt,
    OmrDispatcher,
    SessionHarnessFlagBuilder,
    SkillContractViolationError,
    SpecialistDefinition,
    SpecialistFolderManager,
    SpecialistModelConfig,
    infer_model_family,
)


def _persist_entries(path, worker):
    for item in range(5):
        policy = GatePolicy()
        policy.decide("technical_decision", f"{worker}:{item}")
        policy.persist_log(path)


def test_defaults():
    p = GatePolicy()
    assert p.decide("technical_decision", "pick lib") and p.log
    assert not p.decide("push")
    assert not p.decide("unknown_gate")


def test_irreversible_cannot_be_downgraded_without_optin(tmp_path):
    f = tmp_path / "g.json"
    f.write_text(json.dumps({"modes": {"push": "auto"}}))
    assert not GatePolicy.load(f).decide("push")
    f.write_text(json.dumps({"modes": {"push": "auto"}, "allow_irreversible_auto": True}))
    assert GatePolicy.load(f).decide("push")


def test_invalid_mode(tmp_path):
    f = tmp_path / "g.json"
    f.write_text(json.dumps({"modes": {"x": "yolo"}}))
    with pytest.raises(ValueError):
        GatePolicy.load(f)


def test_persist_log_preserves_entries_from_prior_policy(tmp_path):
    path = tmp_path / "gate_log.json"
    first, second = GatePolicy(), GatePolicy()
    first.decide("technical_decision", "first")
    first.persist_log(path)
    second.decide("plan_gap", "second")
    second.persist_log(path)
    assert [entry["detail"] for entry in json.loads(path.read_text())] == ["first", "second"]


def test_malformed_string_irreversible_auto_does_not_enable_auto(tmp_path):
    f = tmp_path / "g.json"
    f.write_text(json.dumps({"modes": {"push": "auto"}, "allow_irreversible_auto": "false"}))
    assert not GatePolicy.load(f).decide("push")
    p = GatePolicy(allow_irreversible_auto="false")
    p.modes["push"] = "auto"
    assert not p.decide("push")


def test_persist_log_atomic_replace_and_corrupt_existing_recovery(tmp_path, monkeypatch):
    import os

    path = tmp_path / "gate_log.json"
    path.write_text("{corrupt", encoding="utf-8")
    calls = []
    real_replace = os.replace

    def spy(src, dst):
        calls.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy)
    pol = GatePolicy()
    pol.decide("technical_decision", "x")
    pol.persist_log(path)
    assert len(calls) == 1 and calls[0][1] == str(path)
    assert json.loads(path.read_text(encoding="utf-8")) == [
        {"gate": "technical_decision", "detail": "x"}
    ]
    assert list(tmp_path.glob("*.tmp")) == [] and list(tmp_path.glob(".*.tmp")) == []


def test_persist_log_read_oserror_propagates_without_overwriting(tmp_path, monkeypatch):
    import os
    from pathlib import Path

    log_path = tmp_path / "gate_log.json"
    original = [{"gate": "prior", "mode": "auto", "allowed": True, "reason": "ok"}]
    log_path.write_text(json.dumps(original), encoding="utf-8")
    real_read = Path.read_text

    def fake_read(self, *a, **k):
        if self == log_path:
            raise PermissionError("locked")
        return real_read(self, *a, **k)

    replaced = []
    monkeypatch.setattr(Path, "read_text", fake_read)
    monkeypatch.setattr(os, "replace", lambda *a: replaced.append(a))
    gp = GatePolicy()
    gp.decide("technical_decision", "x")
    with pytest.raises(PermissionError):
        gp.persist_log(log_path)
    assert replaced == []
    monkeypatch.undo()
    assert json.loads(log_path.read_text(encoding="utf-8")) == original


def test_persist_log_replace_oserror_keeps_memory_log_and_no_tmp(tmp_path, monkeypatch):
    import os

    log_path = tmp_path / "gate_log.json"
    gp = GatePolicy()
    gp.decide("technical_decision", "x")

    def boom(*a):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        gp.persist_log(log_path)
    assert gp.log == [{"gate": "technical_decision", "detail": "x"}]
    assert list(tmp_path.glob(".*.tmp")) == [] and not log_path.exists()


def test_persist_log_concurrent_writers_preserve_all_entries(tmp_path):
    path = tmp_path / "gate_log.json"
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda worker: _persist_entries(path, worker), range(4)))
    assert len(json.loads(path.read_text(encoding="utf-8"))) == 20


def test_triage_and_escalation():
    assert classify_triage_tier("fix typo", 1).tier == "trivial"
    assert classify_triage_tier("not sure which approach?").tier == "ambiguous-novel"
    decision = escalate_triage_tier("trivial", "failed")
    assert decision.tier == "standard" and decision.escalated_from == "trivial"


def test_lessons_store_matches_trigger(tmp_path):
    store = LessonsStore(tmp_path / "lessons.jsonl")
    store.record_lesson("pytest evidence", "Run focused tests", "task-1")
    assert store.matching_lessons("add pytest evidence")[0]["rule"] == "Run focused tests"


def test_gate_emits_opentelemetry_span(monkeypatch):
    seen = []
    class Span:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def set_attribute(self, key, value): seen.append((key, value))
    monkeypatch.setattr("oh_my_raven.gate._TRACER.start_as_current_span", lambda name: Span())
    GatePolicy().decide("technical_decision", "pick")
    assert ("omr.gate.name", "technical_decision") in seen


def test_ambiguity_experiment_resolves_before_ask(tmp_path):
    dispatcher = object.__new__(OmrDispatcher)
    dispatcher.run_root = tmp_path
    dispatcher.gate_policy = GatePolicy()
    dispatcher.gate_log_path = tmp_path / "gate_log.json"
    result = dispatcher.run_ambiguity_experiment("t1", "which?", lambda: {"resolved": True, "answer": "a"})
    assert result["proceed"] is True and result["resolved_by"] == "experiment"
    assert json.loads((tmp_path / "experiments" / "t1.json").read_text())["verified"] is True


def test_execute_external_action_blocks_before_side_effect():
    dispatcher = object.__new__(OmrDispatcher)
    dispatcher.gate_policy = GatePolicy()
    dispatcher.gate_log_path = __import__("pathlib").Path("unused.json")
    called = []
    with pytest.raises(PermissionError):
        dispatcher.execute_external_action("push", "origin", lambda: called.append(True))
    assert called == []


def test_council_consensus_requires_ratio_and_no_gaps(monkeypatch):
    attributes = []
    class Span:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def set_attribute(self, key, value): attributes.append((key, value))
    monkeypatch.setattr("oh_my_raven.gate._TRACER.start_as_current_span", lambda name: Span())
    verdict = evaluate_council_consensus([
        CouncilVote(voter="cto", approved=True),
        {"voter": "reviewer", "approved": True, "gaps": ["add rollback", "add rollback"]},
        {"voter": "engineer-superpowers", "approved": False, "gaps": ["test failure"]},
    ])
    assert not verdict.consensus
    assert verdict.merged_gaps == ["add rollback", "test failure"]
    assert ("omr.council.approved_count", 2) in attributes
    assert evaluate_council_consensus([
        {"voter": "cto", "approved": True}, {"voter": "reviewer", "approved": True}
    ]).consensus


def test_dispatcher_council_persists_and_records_gaps(tmp_path):
    dispatcher = object.__new__(OmrDispatcher)
    dispatcher.run_root = tmp_path / "runs"
    dispatcher.gate_policy = GatePolicy()
    dispatcher.gate_log_path = dispatcher.run_root / "gate_log.json"
    dispatcher.lessons_store = LessonsStore(tmp_path / "history" / "lessons.jsonl")
    result = dispatcher.run_council_consensus("task-1", [
        {"voter": "cto", "approved": True, "gaps": ["prove rollback"]},
        {"voter": "reviewer", "approved": True, "gaps": ["prove rollback"]},
    ])
    assert result["consensus"] is False
    assert json.loads((dispatcher.run_root / "council" / "task-1.json").read_text()) == result
    assert json.loads((dispatcher.run_root / "gate_log.json").read_text())[0]["detail"] == "council_gaps:task-1:1"
    lesson = json.loads(dispatcher.lessons_store.path.read_text().splitlines()[0])
    assert lesson == {"trigger": "council_gap", "rule": "prove rollback", "source_task_id": "task-1", "active": True}


def test_onboard_specialist_merges_via_public_table_backend_and_deduplicates_targets(tmp_path):
    from types import MappingProxyType, SimpleNamespace
    from oh_my_raven.oh_my_raven import OmrTaskQueue
    from oh_my_raven.routing import RouteTarget, RoutingBackend

    decoy = RoutingBackend("Sec-Audit", SimpleNamespace(name="X"), [RouteTarget("X", "X", SimpleNamespace(name="X"))], instances=None)

    class Table:
        def __init__(self):
            self.registry = {}
            self._backends = MappingProxyType({"Sec-Audit": decoy})  # read-only decoy: registration goes via onboard()

        def backend(self, name):
            return self.registry[name]

        def onboard(self, cfg, backend=None):
            self.registry[cfg] = backend

    table = Table()
    d = OmrDispatcher(
        table=table, queue=OmrTaskQueue(tmp_path / "q.jsonl"), workdir=tmp_path,
        run_root=tmp_path / "runs", nodes_root=tmp_path / "nodes",
        history_root=tmp_path / "history", gate_policy_path=None,
    )
    h1, h2 = SimpleNamespace(name="H1"), SimpleNamespace(name="H2")
    rb = d.onboard_specialist("Sec-Audit", [RouteTarget("H1", "H1", h1), RouteTarget("H1", "H1", h1)])
    assert isinstance(rb, RoutingBackend)
    assert [t.name for t in rb._targets] == ["H1"]
    merged = d.onboard_specialist(
        "Sec-Audit", [RouteTarget("H1", "H1", h1), RouteTarget("H2", "H2", h2)], replace=False
    )
    assert merged is rb
    assert [t.name for t in rb._targets] == ["H1", "H2"]
    assert rb is not decoy and [t.name for t in decoy._targets] == ["X"]


def _materialized_specialist(tmp_path):
    ext_skills = tmp_path / "ext_skills"
    pack = ext_skills / "superpowers"
    (pack / "scripts").mkdir(parents=True)
    (pack / "SKILL.md").write_text("# Superpowers TDD", encoding="utf-8")
    (pack / "scripts" / "verify_tdd.py").write_text("import sys; print('TDD_OK'); sys.exit(0)", encoding="utf-8")
    spec = SpecialistDefinition(
        name="Engineer-Superpowers",
        role_prompt="Follow strict TDD and clean architecture.",
        skill_packs=("superpowers", "ecc"),
        disabled_skills=("legacy-pack",),
        mcp_servers=("codebase-memory", "trail"),
        disabled_mcp_servers=("github",),
        strict_mcp_scope=True,
        plugins=("omr-guard",),
        disabled_plugins=("noisy-hook",),
        allowed_tools=("read_file", "patch"),
        denied_tools=("rm",),
    )
    return SpecialistFolderManager.ensure_specialist_dir(spec, tmp_path / "specialists", extra_skill_roots=[ext_skills])


def test_specialist_folder_manager_agents_md_roundtrip_and_layout(tmp_path):
    mat = _materialized_specialist(tmp_path)
    folder = tmp_path / "specialists" / "engineer-superpowers"
    assert (folder / "AGENTS.md").is_file() and (folder / "mcp.json").is_file()
    assert (folder / "skills" / "superpowers" / "SKILL.md").is_file()
    assert (folder / "skills" / "superpowers" / "scripts" / "verify_tdd.py").is_file()
    assert (folder / "skills" / "ecc" / "SKILL.md").is_file()
    loaded = SpecialistDefinition.from_agents_md(Path(mat.specialist_dir))
    assert loaded.name == "Engineer-Superpowers"
    assert loaded.resolved_slug == "engineer-superpowers"
    for field in ("skill_packs", "disabled_skills", "mcp_servers", "disabled_mcp_servers", "strict_mcp_scope",
                  "plugins", "disabled_plugins", "allowed_tools", "denied_tools"):
        assert getattr(loaded, field) == getattr(mat, field), field
    assert loaded.role_prompt == "Follow strict TDD and clean architecture."

    root = tmp_path / "specialists"
    for bad in (SpecialistDefinition(name="Bad", slug="../escape"),
                SpecialistDefinition(name="Bad2", skill_packs=("../escape",)),
                SpecialistDefinition(name="Bad3", plugins=("a/b",))):
        with pytest.raises(ValueError):
            SpecialistFolderManager.ensure_specialist_dir(bad, root)

    ext = tmp_path / "ext_skills"
    plug_spec = dict(name="Recon", skill_packs=("superpowers", "ecc"), plugins=("omr-guard", "old-plug"))
    SpecialistFolderManager.ensure_specialist_dir(SpecialistDefinition(**plug_spec), root, extra_skill_roots=[ext])
    rf = root / "recon"
    SpecialistFolderManager.ensure_specialist_dir(
        SpecialistDefinition(**plug_spec, disabled_skills=("ecc",), disabled_plugins=("old-plug",)),
        root, extra_skill_roots=[ext])
    assert not (rf / "skills" / "ecc").exists()
    assert not (rf / "plugins" / "old-plug").exists()


def test_session_harness_flag_builder_all_four_harnesses(tmp_path):
    mat = _materialized_specialist(tmp_path)
    args = {h: list(SessionHarnessFlagBuilder.build_for_candidate(mat, h)["session_cli_args"])
            for h in ("Claude-Code", "Codex-CLI", "Copilot-CLI", "Hermes-CLI")}
    claude = args["Claude-Code"]
    for flag in ("--add-dir", "--strict-mcp-config", "--mcp-config", "--plugin-dir", "--allowedTools", "--disallowedTools"):
        assert flag in claude, flag
    assert any("mcp__github__*" in a for a in claude)
    codex = args["Codex-CLI"]
    assert "-c" in codex and any(a.startswith("skills.dirs=") for a in codex)
    assert "mcp_servers.codebase-memory.enabled=true" in codex and "mcp_servers.github.enabled=false" in codex
    assert "--enable" in codex and "--disable" in codex
    assert "mcp_servers._strict_scope=true" in codex
    for prefix in ("tools.allowed=", "tools.denied=", "skills.disabled="):
        assert any(a.startswith(prefix) for a in codex), prefix
    for flag in ("--strict-mcp-scope", "--disable-mcp", "--deny-tools", "--disable-skills"):
        assert flag in args["Hermes-CLI"], flag
    with pytest.raises(SkillContractViolationError):
        SessionHarnessFlagBuilder.build_for_candidate(mat, "Unknown-Generic-Harness")
    for flag in ("--add-dir", "--disable-builtin-mcps", "--additional-mcp-config", "--enable-mcp-server",
                 "--disable-mcp-server", "--plugin-dir", "--available-tools", "--excluded-tools"):
        assert flag in args["Copilot-CLI"], flag
    for flag in ("-s", "-t", "--safe-mode"):
        assert flag in args["Hermes-CLI"], flag


def test_bucket1_harness_onboarding_probe_and_bucket2_gstack_separation(tmp_path):
    assert "gstack" in BUCKET2_SPECIALIST_FAMILIES and "gstack" not in BUCKET1_STARTUP_SKILLS
    assert "andrej-karpathy-skill" in BUCKET1_STARTUP_SKILLS and "autonomous-delivery" in BUCKET1_STARTUP_SKILLS
    root = tmp_path / "harnesses"
    with pytest.raises(HarnessStartupContractViolationError):
        HarnessStartupManager.ensure_and_verify_harness("Claude-Code", root, auto_install=False)
    receipt = HarnessStartupManager.ensure_and_verify_harness("Claude-Code", root, auto_install=True)
    assert isinstance(receipt, HarnessStartupReceipt) and receipt.ready is True
    home = Path(receipt.harness_home)
    for s in BUCKET1_STARTUP_SKILLS:
        assert (home / "skills" / s / "SKILL.md").is_file(), s
    assert set(BUCKET1_STARTUP_MCPS) <= set(receipt.installed_mcps)
    assert (home / "startup_manifest.json").is_file()

    # Finding #4: plugin payloads
    assert set(BUCKET1_STARTUP_PLUGINS) <= set(receipt.installed_plugins)
    assert receipt.missing_plugins == ()
    for p in BUCKET1_STARTUP_PLUGINS:
        assert (home / "plugins" / p / "plugin.json").is_file(), p

    # Finding #1: existing mcp.json entries preserved
    cfg = json.loads((home / "mcp.json").read_text())
    cfg["mcpServers"]["custom-corp-mcp"] = {"type": "stdio", "command": "corp-mcp",
                                            "args": ["--prod"], "enabled": True}
    cfg["mcpServers"]["trail"]["args"] = ["--custom-flag"]
    (home / "mcp.json").write_text(json.dumps(cfg))
    HarnessStartupManager.ensure_and_verify_harness("Claude-Code", root, auto_install=True)
    cfg2 = json.loads((home / "mcp.json").read_text())["mcpServers"]
    assert "custom-corp-mcp" in cfg2 and "--custom-flag" in cfg2["trail"]["args"]
    assert set(BUCKET1_STARTUP_MCPS) <= set(cfg2)

    # Finding #3: unsafe names rejected
    with pytest.raises(ValueError):
        HarnessStartupManager.ensure_and_verify_harness("Claude-Code", root, required_startup_skills=("../escape",))
    with pytest.raises(ValueError):
        HarnessStartupManager.ensure_and_verify_harness("Claude-Code", root, required_startup_mcps=("a/b",))
    with pytest.raises(ValueError):
        HarnessStartupManager.ensure_and_verify_harness("../bad", root)

    # Finding #3: disabled MCP rejected
    dh = tmp_path / "disabled_h"
    drec = HarnessStartupManager.ensure_and_verify_harness("Claude-Code", dh, auto_install=True)
    dmcp = Path(drec.harness_home) / "mcp.json"
    dcfg = json.loads(dmcp.read_text())
    dcfg["mcpServers"]["everos"] = {"type": "stdio", "command": "everos", "args": [], "enabled": False}
    dmcp.write_text(json.dumps(dcfg))
    with pytest.raises(HarnessStartupContractViolationError):
        HarnessStartupManager.ensure_and_verify_harness("Claude-Code", dh, auto_install=False)

    # Finding #4: empty plugin dir is not installed
    (home / "plugins" / "everos" / "plugin.json").unlink()
    with pytest.raises(HarnessStartupContractViolationError):
        HarnessStartupManager.ensure_and_verify_harness("Claude-Code", root, auto_install=False)

    spec = SpecialistDefinition(name="CTO", skill_packs=("gstack", "andrej-karpathy-skill"),
                                mcp_servers=("trail", "custom-specialist-mcp"),
                                startup_skills=("andrej-karpathy-skill", "autonomous-delivery"),
                                startup_mcp_servers=("everos", "trail"))
    mat = SpecialistFolderManager.ensure_specialist_dir(spec, tmp_path / "specialists",
                                                        extra_skill_roots=[tmp_path / "ext"])
    sdir = Path(mat.specialist_dir)
    assert (sdir / "skills" / "gstack" / "SKILL.md").is_file()
    assert not (sdir / "skills" / "andrej-karpathy-skill").exists()
    loaded = SpecialistDefinition.from_agents_md(sdir)
    assert loaded.startup_skills == spec.startup_skills
    assert loaded.startup_mcp_servers == spec.startup_mcp_servers

    # Finding #5: Bucket-1 MCP not duplicated; Bucket-2 kept
    spec_mcp = json.loads((sdir / "mcp.json").read_text())
    assert "trail" not in spec_mcp["mcpServers"]
    assert "trail" in spec_mcp["_omr_meta"]["startup_mcps"]
    assert "custom-specialist-mcp" in spec_mcp["mcpServers"]

    # Finding #2: fail closed without startup_receipt
    with pytest.raises(SkillContractViolationError):
        SkillAndToolReceiptGate.verify_specialist_execution(
            SimpleNamespace(id="n1", specialist_definition=spec), SimpleNamespace(),
            json.dumps({"skill_evidence": {"gstack": {"command": "gstack", "exit_code": 0}},
                        "invoked_mcps": ["custom-specialist-mcp"]}),
            require_skill_evidence=True)


def test_reconcile_harness_pool_eliminates_bucket1_drift(tmp_path):
    names = ["Claude-Code", "Codex-CLI", "Copilot-CLI", "Hermes-CLI"]
    res = HarnessStartupManager.reconcile_harness_pool(names, tmp_path / "harnesses")
    assert res["in_sync"] is True
    assert set(BUCKET1_STARTUP_SKILLS) <= set(res["skills_intersection"])


def _rich_model_config():
    return SpecialistModelConfig(
        preferred_models=("claude-sonnet-4-6",),
        fallback_models=("gpt-5.4", "gemini-2.5-pro"),
        model_tier="frontier",
        reasoning_effort="high",
        disallowed_models=("claude-3-haiku*",),
        require_distinct_model_family_from_author=True,
        model_aliases={
            "claude-sonnet-4-6": {"claude": "claude-sonnet-4-6", "copilot": "custom/gh/claude-sonnet-4.6",
                                  "hermes": "custom/gh/claude-sonnet-4.6"},
            "gpt-5.4": {"codex": "gpt-5.4", "copilot": "custom/gh/gpt-5.4", "hermes": "custom/gh/gpt-5.4"},
        },
    )


def _model_specialist(tmp_path):
    spec = SpecialistDefinition(name="Modeler", model_config=_rich_model_config())
    return SpecialistFolderManager.ensure_specialist_dir(spec, tmp_path / "specialists")


def test_specialist_model_config_roundtrip_and_artifact_materialization(tmp_path):
    mat = _model_specialist(tmp_path)
    sdir = Path(mat.specialist_dir)
    assert json.loads((sdir / "model_config.json").read_text(encoding="utf-8")) == _rich_model_config().to_dict()
    text = (sdir / "AGENTS.md").read_text(encoding="utf-8")
    assert "model_config" in text and "## Model Config" in text and "claude-sonnet-4-6" in text
    loaded = SpecialistDefinition.from_agents_md(sdir)
    for f in ("preferred_models", "fallback_models", "model_tier", "reasoning_effort", "disallowed_models",
              "require_distinct_model_family_from_author", "model_aliases"):
        assert getattr(loaded.model_config, f) == getattr(_rich_model_config(), f), f
    assert loaded.model_config == _rich_model_config()
    assert infer_model_family("custom/gh/claude-sonnet-4.6") == "claude"
    assert infer_model_family("openai/gpt-5.4") == "gpt"
    assert infer_model_family("gemini-2.5-pro") == "gemini"


def test_session_harness_flag_builder_model_config_all_harnesses_and_acp(tmp_path):
    mat = _model_specialist(tmp_path)
    build = SessionHarnessFlagBuilder.build_for_candidate
    c = list(build(mat, "Claude-Code")["session_cli_args"])
    assert c[c.index("--model") + 1] == "claude-sonnet-4-6" and c[c.index("--effort") + 1] == "high"
    x = build(mat, "Codex-CLI", supported_models=("gpt-5.4",))
    assert x["model_fallback_used"] is True and x["resolved_model"] == "gpt-5.4"
    xa = list(x["session_cli_args"])
    assert xa[xa.index("-m") + 1] == "gpt-5.4" and 'model_reasoning_effort="high"' in xa
    p = list(build(mat, "Copilot-CLI")["session_cli_args"])
    assert p[p.index("--model") + 1] == "custom/gh/claude-sonnet-4.6" and p[p.index("--reasoning-effort") + 1] == "high"
    h = list(build(mat, "Hermes-CLI")["session_cli_args"])
    assert h[h.index("-m") + 1] == "custom/gh/claude-sonnet-4.6" and h[h.index("--reasoning-effort") + 1] == "high"
    acp = build(mat, "Windsurf-ACP", backend=SimpleNamespace(kind="acp", supports_session_isolation=True))
    for k in ("session/new", "session/set_model"):
        assert acp["acp_session_params"][k] == {"model": "claude-sonnet-4-6", "reasoning_effort": "high"}
    with pytest.raises(SkillContractViolationError):
        build(mat, "Codex-CLI", supported_models=("llama-3",))
    with pytest.raises(SkillContractViolationError):
        build(mat, "Claude-Code", supported_models=("claude-sonnet-4-6",), author_models=("claude-opus-4-6",))
