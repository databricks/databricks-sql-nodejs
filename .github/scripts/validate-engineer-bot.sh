#!/usr/bin/env bash
set -euo pipefail

unset JFROG_ACCESS_TOKEN ACTIONS_ID_TOKEN_REQUEST_TOKEN ACTIONS_ID_TOKEN_REQUEST_URL NODE_OPTIONS NODE_PATH
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
python -I - <<'PY'
from pathlib import Path
from collections import Counter
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
# Pinned engine run_author emits touched_files with "\n".join(result.touched_files).
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

def split_unreleased(text, require_first=False):
    section = re.search(r"(?ms)^## Unreleased[ \t]*\n(.*?)(?=^## |\Z)", text)
    if section is None:
        if require_first:
            raise SystemExit("::error::Use the heading ## Unreleased above the released versions.")
        return text, Counter(), []
    if require_first and section.start() != re.search(r"(?m)^## ", text).start():
        raise SystemExit("::error::Place ## Unreleased above the released versions.")
    released = text[:section.start()] + text[section.end():]
    blocks, notes = [], []
    entry = None
    for line in section.group(1).splitlines():
        if line.startswith("- ") and line[2:].strip():
            entry = [line[2:]]
            blocks.append(entry)
        elif entry is not None and (not line.strip() or line[:1].isspace()):
            entry.append(line)
        else:
            entry = None
            if line.strip():
                notes.append(line)
    return released, Counter("\n".join(block).rstrip() for block in blocks), notes

previous = subprocess.check_output(["git", "show", f"{base_sha}:CHANGELOG.md"], text=True)
current = Path("CHANGELOG.md").read_text()
previous_released, previous_entries, previous_notes = split_unreleased(previous)
current_released, current_entries, current_notes = split_unreleased(current, require_first=True)
previous_titles = Counter(entry.splitlines()[0] for entry in previous_entries.elements())
current_titles = Counter(entry.splitlines()[0] for entry in current_entries.elements())
if any(count > max(1, previous_titles[title]) for title, count in current_titles.items()):
    raise SystemExit("::error::Do not add duplicate Unreleased entries.")
if (current_released != previous_released or current_notes != previous_notes or not previous_entries <= current_entries
        or not (current_entries - previous_entries)):
    raise SystemExit("::error::Append a fix bullet under ## Unreleased and preserve existing entries.")
directive_script = subprocess.check_output(
    ["git", "show", f"{base_sha}:.github/scripts/check-engineer-bot-directives.js"], text=True
)
directive_check = subprocess.run(
    ["node", "-e", directive_script],
    env={**os.environ, "GATE_PATHS": json.dumps(sorted(recorded & changed))},
)
if directive_check.returncode:
    raise SystemExit(directive_check.returncode)
PY
# Invoke verified tools without loading user npm configuration.
./node_modules/.bin/prettier . --check
./node_modules/.bin/eslint 'lib/**/*.{js,ts}' 'tests/e2e/**/*.{js,ts}' --ext .js,.ts
