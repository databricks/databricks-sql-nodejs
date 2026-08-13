# Engineer-bot learning log

This file is the shared knowledge log for the databricks-sql-nodejs engineer-bot.

- The **retrospective** flow (see `.bot/config.yaml` → `retrospective.log_path`)
  appends a dated section here whenever it distills a durable, reusable learning
  from merged PRs and author runs.
- The **author** flow (see `.bot/config.yaml` → `author.knowledge_log`) reads this
  file back so fixes benefit from what the retrospective has learned.

No learnings have been recorded yet — the first retrospective PR will append the
initial dated section below.
## Entries

### 2026-08-13: learnings since 2026-08-12T17:43:56Z
- **Context:** In PR #499 the daily learning workflow declared a `workflow_dispatch` input named `window-hours` and referenced it as `${{ inputs.window-hours }}`; reviewers noted the hyphen is parsed as subtraction (`inputs.window - hours`), yielding an empty value.
  **Rule:** In GitHub Actions `${{ }}` expressions, reference hyphenated input/context names with bracket notation (`inputs['window-hours']`) or rename them to use underscores — a bare hyphen is parsed as the minus operator, not part of the identifier.
- **Context:** PR #499's workflow comment documents that a `type: number` `workflow_dispatch` input fails the whole run at startup ("workflow file issue") when the workflow also has a `schedule` trigger; it was declared `type: string` and coerced to int downstream instead.
  **Rule:** Declare `workflow_dispatch` inputs as `type: string` (and coerce downstream) when the same workflow also has a `schedule` trigger — mixing `type: number` inputs with a schedule trigger fails the run at startup.
- **Context:** In PR #499 reviewers found that pointing `retrospective.system_prompt` at a non-existent `prompts/retrospective_system.md` would hard-fail every scheduled run; the fix was to drop the key entirely so the engine used its built-in base prompt.
  **Rule:** For databricks-bot-engine config, a set-but-missing prompt-file key (e.g. `system_prompt`) is a hard error; leave the key UNSET to fall back to the engine's built-in base prompt rather than pointing it at a path that may not exist.
- **Context:** PR #499's workflow sets `MODEL_ENDPOINT` to `.../serving-endpoints/<model>/invocations`; the comment warns that using `.../serving-endpoints/anthropic/invocations` hits `translate_endpoint`'s already-v2 early return, so the CLI appends `/v1/messages` to `.../anthropic/invocations` and gets HTTP 400.
  **Rule:** For these databricks bot workflows, set `MODEL_ENDPOINT` to the concrete `.../serving-endpoints/<model>/invocations` form (which `translate_endpoint` strips to the `anthropic` base); do NOT pass `.../serving-endpoints/anthropic/invocations` — it survives translation and produces an unsupported `/anthropic/invocations/v1/messages` path (HTTP 400).
- **Context:** In PR #499 engineer-bot could not apply a valid workflow fix because `.github/` is a denied/non-writable path for its tools (read_file and edit_file returned "Path denied or invalid"), so the finding had to be flagged for a human.
  **Rule:** engineer-bot's edit tools cannot touch `.github/` paths — review findings on workflow/action files must be routed to a human; don't expect the bot to self-apply or verify changes under `.github/`.
- **Context:** Across PR #499's review threads, engineer-bot repeatedly reported fixes as pushed (commits c45c237, 5514996) that did not appear at head, and both bots re-read stale snapshots, causing repeated churn on already-resolved threads.
  **Rule:** Before claiming a fix has landed, re-fetch and confirm the change is actually present at the branch head — a local edit or a push that reverted/failed can produce false "pushed" claims and wasted re-work on the same thread.
- **Context:** PR #497's `bin/build-native.sh` runs under `set -euo pipefail` and expands a possibly-empty bash array as `"${arr[@]}"`; reviewers noted this raises `unbound variable` and aborts on bash 4.3 and earlier (macOS still ships bash 3.2 at `/usr/bin/bash` under `#!/usr/bin/env bash`).
  **Rule:** Under `set -u`, expanding an empty array as `"${arr[@]}"` aborts on bash < 4.4 (incl. stock macOS bash 3.2); guard the expansion (e.g. `${arr[@]+"${arr[@]}"}` or a length check) for portable scripts. Also note `${VAR-default}` substitutes only when unset while `${VAR:-default}` also covers set-but-empty.
