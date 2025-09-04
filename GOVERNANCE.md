# Project Governance

This document describes how the spark-fuse project is governed, how decisions are made, and how responsibilities are assigned.

## Roles and Responsibilities

- Lead Maintainer: Kevin Sames ([@kevinsames](https://github.com/kevinsames))
  - Final tie‑breaker on decisions, release management, and security response coordination.
- Maintainers: individuals with write access who review and merge changes, cut releases, and triage issues.
- Contributors: anyone submitting issues, proposals, or pull requests.

New maintainers are nominated by an existing maintainer and added via lazy consensus (see below).

## Decision Making

- Model: Lazy consensus. Unless a change is controversial, a PR approved by at least one maintainer can be merged after feedback has been addressed.
- Non‑trivial or user‑facing changes: require approval from at least one maintainer.
- Backward‑incompatible changes: require approval from two maintainers or the Lead Maintainer.
- Disagreements: Discuss in the PR or an RFC issue; unresolved disputes are decided by the Lead Maintainer.

## Proposals and RFCs

For substantial changes (new subsystems, major API shifts), open a GitHub Discussion or an issue labeled `rfc` summarizing motivation, design, and alternatives. After review, proceed with an implementation PR referencing the RFC.

## Reviewing and Merging

- PRs should be small, scoped, and include tests and documentation when applicable.
- CI must pass before merge.
- Squash merge is preferred to keep history clean; conventional commit style is encouraged but not required.

## Releases

- Versioning: Semantic Versioning (MAJOR.MINOR.PATCH).
- Release process: bump `version` in `pyproject.toml`, update `CHANGELOG` (if present), create a GitHub Release tag (e.g., `vX.Y.Z`). The publish workflow uploads to PyPI from the protected `pypi` environment.
- Release managers: the Lead Maintainer or a designated maintainer.

## Security

Please report vulnerabilities privately using GitHub Security Advisories (preferred) or by contacting the Lead Maintainer. Do not open public issues for sensitive reports.

## Code of Conduct

Participation in this project is governed by the Contributor Covenant. See `CODE_OF_CONDUCT.md`.

## Changes to this Document

Propose changes via PR. Unless there is explicit objection from a maintainer within 5 business days, changes will be merged under lazy consensus. The Lead Maintainer may make immediate fixes for clarity or urgent policy needs.

