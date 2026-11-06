#!/usr/bin/env bash
set -euo pipefail

unset JFROG_ACCESS_TOKEN ACTIONS_ID_TOKEN_REQUEST_TOKEN ACTIONS_ID_TOKEN_REQUEST_URL
protected=(
  ':(glob)**/tsconfig*.json'
  ':(glob)**/package.json' ':(glob)**/package-lock.json'
  ':(glob)**/.npmrc' ':(glob)**/.yarnrc*' ':(glob)**/.nvmrc'
  ':(glob)**/.eslintrc*' ':(glob)**/.eslintignore'
  ':(glob)**/.prettierrc*' ':(glob)**/.prettierignore'
  ':(glob)**/.editorconfig' ':(glob)**/.gitignore'
  ':(glob)**/eslint.config.*' ':(glob)**/prettier.config.*'
)
if [ -n "$(git status --porcelain --untracked-files=all -- "${protected[@]}")" ]; then
  echo "::error::Agent changes to package or lint configuration are not allowed."
  exit 1
fi
checksum=$(git show HEAD:.github/scripts/engineer-bot-dependency-digest.sh | bash)
if [ "$checksum" != "$TOOLCHAIN_SHA256" ]; then
  echo "::error::Installed dependencies changed during the agent run."
  exit 1
fi
python - <<'PY'
from pathlib import Path
import os
import re
import subprocess

def git_paths(*args):
    output = subprocess.check_output(["git", *args])
    return {os.fsdecode(path) for path in output.split(b"\0") if path}

root = Path.cwd().resolve()
recorded = set()
for raw in os.environ["TOUCHED_FILES"].split("\n"):
    if raw:
        path = (root / raw).resolve()
        if path.is_relative_to(root) and path.is_file():
            recorded.add(path.relative_to(root).as_posix())
changed = (
    git_paths("diff", "--no-renames", "--name-only", "-z", "HEAD", "--")
    | git_paths("diff", "--cached", "--no-renames", "--name-only", "-z", "HEAD", "--")
    | git_paths("ls-files", "--others", "--exclude-standard", "-z")
)
deleted = git_paths("diff", "--no-renames", "--diff-filter=D", "--name-only", "-z", "HEAD", "--")
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
    entries = set(re.findall(r"(?m)^- (\S.*)$", section.group(1)))
    return released, entries

previous = subprocess.check_output(["git", "show", "HEAD:CHANGELOG.md"], text=True)
current = Path("CHANGELOG.md").read_text()
previous_released, previous_entries = split_unreleased(previous)
current_released, current_entries = split_unreleased(current)
if current_released != previous_released or not (current_entries - previous_entries):
    raise SystemExit("::error::Add a fix bullet under ## Unreleased and preserve released entries.")
PY
npm run prettier
npm run lint
