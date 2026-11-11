---
name: oh-my-raven
description: 'oh-my-raven: Deliver a reviewed feature through isolated specialist slices and fail-closed gates.'
---

# oh-my-raven

oh-my-raven: Deliver a reviewed feature through isolated specialist slices and fail-closed gates.

Steps: cto_strategy -> slice_a -> slice_b -> integrate -> review -> remediate -> re_review

```yaml playbook-spec
taskSummary: deliver reviewed feature via oh-my-raven
version: 2
mode: dag
confirm: true
triggers:
  keywords:
  - oh-my-raven
  - feature delivery
  - specialist orchestration
params: {}
nodes:
- id: cto_strategy
  subagent: CTO
  nodeSummary: CTO cross-workstream strategic alignment and consequence matrix
  promptTemplate: Analyze cross-workstream synergies, long-term goal alignment, and 5-dimension consequence
    matrix
  dependsOn: []
  inputs: {}
  executionKind: plan
  fileScope: []
  idempotent: true
- id: slice_a
  subagent: specialist
  nodeSummary: implement first slice
  promptTemplate: implement first slice
  dependsOn:
  - cto_strategy
  inputs: {}
  executionKind: write
  fileScope:
  - raven/agent/subagent/**
  idempotent: true
- id: slice_b
  subagent: specialist
  nodeSummary: implement second slice
  promptTemplate: implement second slice
  dependsOn:
  - cto_strategy
  inputs: {}
  executionKind: write
  fileScope:
  - tests/**
  idempotent: true
- id: integrate
  subagent: specialist
  nodeSummary: integrate slices
  promptTemplate: integrate slices
  dependsOn:
  - slice_a
  - slice_b
  inputs: {}
  executionKind: integrate
  fileScope: []
  idempotent: true
- id: review
  subagent: reviewer
  nodeSummary: review candidate
  promptTemplate: review candidate
  dependsOn:
  - integrate
  inputs: {}
  routes:
    approved: candidate accepted
    changes_requested: candidate needs changes
  executionKind: review
  fileScope: []
  idempotent: true
- id: remediate
  subagent: specialist
  nodeSummary: remediate review
  promptTemplate: remediate review
  dependsOn:
  - review
  inputs: {}
  when: review.changes_requested
  executionKind: write
  fileScope:
  - raven/agent/subagent/**
  - tests/**
  idempotent: true
- id: re_review
  subagent: reviewer
  nodeSummary: review remediation
  promptTemplate: review remediation
  dependsOn:
  - remediate
  - review
  inputs: {}
  when: review.changes_requested
  executionKind: review
  fileScope: []
  idempotent: true
```
