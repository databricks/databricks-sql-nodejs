#!/usr/bin/env bash
set -euo pipefail

unset JFROG_ACCESS_TOKEN ACTIONS_ID_TOKEN_REQUEST_TOKEN ACTIONS_ID_TOKEN_REQUEST_URL
export GIT_NO_REPLACE_OBJECTS=1
export GIT_CONFIG_COUNT=3
export GIT_CONFIG_KEY_0=core.fsmonitor GIT_CONFIG_VALUE_0=false
export GIT_CONFIG_KEY_1=core.untrackedCache GIT_CONFIG_VALUE_1=false
export GIT_CONFIG_KEY_2=core.hooksPath GIT_CONFIG_VALUE_2=/dev/null
if [ "$(git rev-parse HEAD)" != "$BASE_SHA" ]; then
  echo "::error::The agent changed HEAD from the pre-agent commit."
  exit 1
fi
protected=(
  ':(glob)**/tsconfig*.json'
  ':(glob)**/package.json' ':(glob)**/package-lock.json' ':(glob)**/package.yaml'
  ':(glob)**/.npmrc' ':(glob)**/.yarnrc*' ':(glob)**/.nvmrc'
  ':(glob)**/.eslintrc*' ':(glob)**/.eslintignore'
  ':(glob)**/.prettierrc*' ':(glob)**/.prettierignore'
  ':(glob)**/.editorconfig' ':(glob)**/.gitignore'
  ':(glob)**/eslint.config.*' ':(glob)**/prettier.config.*'
)
tracked_config=$(git diff --no-ext-diff --name-only "$BASE_SHA" -- "${protected[@]}")
staged_config=$(git diff --cached --no-ext-diff --name-only "$BASE_SHA" -- "${protected[@]}")
untracked_config=$(git ls-files --others -- "${protected[@]}" ':(top,exclude)node_modules/**')
if [ -n "$tracked_config$staged_config$untracked_config" ]; then
  echo "::error::Agent changes to package or lint configuration are not allowed."
  exit 1
fi
digest_script=$(git show "$BASE_SHA:.github/scripts/engineer-bot-dependency-digest.sh")
checksum=$(bash -c "$digest_script")
if [ "$checksum" != "$TOOLCHAIN_SHA256" ]; then
  echo "::error::Installed dependencies changed during the agent run."
  exit 1
fi
python - <<'PY'
from pathlib import Path
import json
import os
import re
import subprocess

def git_paths(*args):
    output = subprocess.check_output(["git", *args])
    return {os.fsdecode(path) for path in output.split(b"\0") if path}

root = Path.cwd().resolve()
base_sha = os.environ["BASE_SHA"]
index_entries = subprocess.check_output(["git", "ls-files", "-v", "-z"]).split(b"\0")
if any(entry[:1] == b"S" or entry[:1].islower() for entry in index_entries if entry):
    raise SystemExit("::error::Git index flags must not hide working-tree changes.")
recorded = set()
for raw in os.environ["TOUCHED_FILES"].split("\n"):
    if raw:
        path = (root / raw).resolve()
        if path.is_relative_to(root) and path.is_file():
            recorded.add(path.relative_to(root).as_posix())
changed = (
    git_paths("diff", "--no-ext-diff", "--no-renames", "--name-only", "-z", base_sha, "--")
    | git_paths("diff", "--cached", "--no-ext-diff", "--no-renames", "--name-only", "-z", base_sha, "--")
    | git_paths("ls-files", "--others", "--exclude-per-directory=.gitignore", "-z")
)
deleted = git_paths("diff", "--no-ext-diff", "--no-renames", "--diff-filter=D", "--name-only", "-z", base_sha, "--")
if deleted:
    raise SystemExit("::error::The pinned bot publisher cannot stage deleted files; a maintainer must handle deletions.")
if "CHANGELOG.md" not in recorded or changed - recorded:
    raise SystemExit("::error::Record CHANGELOG.md and every changed file with edit_file before publishing.")

def split_unreleased(text):
    section = re.search(r"(?ms)^## Unreleased\n(.*?)(?=^## |\Z)", text)
    if section is None:
        return text, set()
    if section.start() != re.search(r"(?m)^## ", text).start():
        raise SystemExit("::error::Place ## Unreleased above the released versions.")
    released = text[:section.start()] + text[section.end():]
    blocks = [entry.rstrip() for entry in re.findall(r"(?ms)^- (\S.*?)(?=^- |\Z)", section.group(1))]
    titles = [entry.splitlines()[0] for entry in blocks]
    if len(set(titles)) != len(titles):
        raise SystemExit("::error::Do not duplicate Unreleased entries.")
    entries = set(blocks)
    return released, entries

previous = subprocess.check_output(["git", "show", f"{base_sha}:CHANGELOG.md"], text=True)
current = Path("CHANGELOG.md").read_text()
previous_released, previous_entries = split_unreleased(previous)
current_released, current_entries = split_unreleased(current)
if (current_released != previous_released or not previous_entries <= current_entries
        or not (current_entries - previous_entries)):
    raise SystemExit("::error::Append a fix bullet under ## Unreleased and preserve existing entries.")
directive_script = subprocess.check_output(
    ["git", "show", f"{base_sha}:.github/scripts/check-engineer-bot-directives.js"], text=True
)
subprocess.run(
    ["node", "-e", directive_script], check=True,
    env={**os.environ, "GATE_PATHS": json.dumps(sorted(recorded & changed))},
)
PY
npm run prettier
npm run lint
