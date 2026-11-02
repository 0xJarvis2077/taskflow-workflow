# Project instructions

This repository contains a local workflow for designing professional business tasks, checking their quality, running isolated model attempts and reviewing actual deliverables.

- Read `skills/high-value-task-workflow/SKILL.md` for task design or review. Read `README.md` and the relevant contract before changing the tools.
- For a new batch, use `docs/task-collaboration-architecture.md` and `knowledge/templates/task-batch-plan.template.json`. Ask the user for missing role-model choices before preparing real calls. Never infer choices from old batches, templates or CLI defaults.
- Keep authors, independent reconstruction, quality checks, formal solving and review contexts distinct. Review facts first; score with the same reviewer context only after facts are sealed.
- Keep public inputs separate from private references and scoring. File hashes and separate folders do not by themselves prove model isolation or business quality.
- Preserve original inputs, attempts and scores. Label simulations as simulations in internal records, and never report them as real model results.
- Normal authorized successors run through the existing Python execution layer. The coordinator handles business choices; engineering handles technical recovery; content inspection focuses on public pre-solving and material cleanup.
- Use Python 3.10+ and the standard library. Run relevant offline tests; do not start paid model calls, send external messages or publish remotely merely to test a code change.
- Keep local task data, credentials, model configuration, run traces and user-specific rules out of commits. The root `.gitignore` is an explicit public-file allowlist.
- If the owner has authorized ongoing GitHub publication and a local sync configuration exists, read `docs/github-sync.md` and synchronize reviewed, verified public changes through `tools.github_sync`. Never publish task data or bypass its checks, change the configured destination, or force-push.
- If `AGENTS.local.md` exists in this working copy, read it for local operational constraints. It is private and is not part of the shared repository.
