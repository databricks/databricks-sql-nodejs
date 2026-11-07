import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile


SCRIPT = Path(__file__).resolve().parents[1] / 'validate-engineer-bot.sh'
BASE = '# Release History\n\n## 3.0.0\n\n- Released fix.\n\n## 2.0.0\n\n- Older fix.\n'
VALID = BASE.replace('## 3.0.0', '## Unreleased\n\n- New fix. (#261)\n\n## 3.0.0')
EXISTING = BASE.replace('## 3.0.0', '## Unreleased\n\n- Existing fix.\n  Existing detail.\n\n## 3.0.0')
DUPLICATE = EXISTING.replace('  Existing detail.\n', '  Existing detail.\n- Existing fix.\n  Existing detail.\n')
MISPLACED = BASE + '## Unreleased\n\n- Existing fix.\n'
DUPLICATE_FIX = DUPLICATE.replace('## 3.0.0', '- New fix. (#261)\n\n## 3.0.0')
RELOCATED_FIX = BASE.replace('## 3.0.0', '## Unreleased\n\n- Existing fix.\n- New fix. (#261)\n\n## 3.0.0')


def git(root, *args):
    return subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'core.fsmonitor=false', '-c', 'core.untrackedCache=false', '-c', 'commit.gpgsign=false', '-C', str(root), *args], check=True, capture_output=True)


def write(root, name, content):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


with tempfile.TemporaryDirectory(prefix='nodejs-bot-gate-regression-') as temporary:
    root = Path(temporary)
    binary = root / 'bin'
    binary.mkdir()
    (binary / 'python').symlink_to(sys.executable)
    npm = binary / 'npm'
    npm.write_text('#!/usr/bin/env python3\nimport os, sys\nassert all(name not in os.environ for name in ["JFROG_ACCESS_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_URL"])\nwith open(os.environ["VALIDATION_LOG"], "a") as log: log.write(" ".join(sys.argv[1:]) + "\\n")\nif os.environ.get("VALIDATION_FAIL") == "read-stdin": sys.stdin.read()\nsys.exit(1 if os.environ.get("VALIDATION_FAIL") == sys.argv[2] else 0)\n')
    npm.chmod(0o755)
    count = 0
    for baseline in [BASE, EXISTING]:
        fixture = root / ('with-unreleased' if baseline == EXISTING else 'without-unreleased')
        fixture.mkdir()
        git(fixture, 'init', '-q')
        for name, content in [('CHANGELOG.md', baseline), ('.eslintrc', '{}\n'), ('package.json', '{}\n'), ('.gitignore', 'dist/\nlib/version.ts\n*.log\nnode_modules/\n'), ('lib/source.ts', "// eslint-disable-next-line no-console -- Existing reason.\nconsole.log('original');\n"), ('lib/directives.ts', '/* eslint-disable no-alert, no-console -- Existing reason. */\n'), ('lib/types.ts', '// @ts-ignore -- Existing reason.\nexport const value: number = "invalid";\n')]:
            write(fixture, name, content)
        for name in ['validate-engineer-bot.sh', 'engineer-bot-dependency-digest.sh', 'check-engineer-bot-directives.js']:
            write(fixture, '.github/scripts/' + name, SCRIPT.with_name(name).read_text())
        git(fixture, 'add', '.')
        git(fixture, '-c', 'user.name=Workflow Test', '-c', 'user.email=workflow-test@example.com', 'commit', '--signoff', '-qm', 'validation baseline')
        base_sha = git(fixture, 'rev-parse', 'HEAD').stdout.decode().strip()
        exclude_baseline = (fixture / '.git/info/exclude').read_text()
        global_excludes = fixture / '.git/test-global-excludes'
        good = VALID if baseline == BASE else baseline.replace('  Existing detail.', '  Existing detail.\n- New fix. (#261)')
        cases = [
            ('valid fix', good, {}, ['CHANGELOG.md'], '', False, True),
            ('unchanged changelog', baseline, {}, ['CHANGELOG.md'], '', False, False),
            ('missing changelog', None, {}, ['CHANGELOG.md'], '', False, False),
            ('empty changelog', '', {}, ['CHANGELOG.md'], '', False, False),
            ('released edit', good.replace('Released fix.', 'Rewritten fix.'), {}, ['CHANGELOG.md'], '', False, False),
            ('heading without new entry', good.replace('- New fix. (#261)\n', ''), {}, ['CHANGELOG.md'], '', False, False),
            ('unreleased below a release', BASE + '## Unreleased\n\n- New fix.\n', {}, ['CHANGELOG.md'], '', False, False),
            ('unreleased between versions', BASE.replace('## 2.0.0', '## Unreleased\n\n- New fix.\n\n## 2.0.0'), {}, ['CHANGELOG.md'], '', False, False),
            ('unrecorded changelog', good, {}, [], '', False, False),
            ('unrecorded tracked formatter edit', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md'], '', False, False),
            ('unrecorded untracked source', good, {'lib/new.ts': 'export const value = 2;\n'}, ['CHANGELOG.md'], '', False, False),
            ('unrecorded staged source', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md'], '', True, False),
            ('fully recorded source edit', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('fully recorded new source', good, {'lib/new.ts': 'export const value = 2;\n'}, ['CHANGELOG.md', 'lib/new.ts'], '', False, True),
            ('recorded deletion', good, {'lib/source.ts': None}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('recorded names with whitespace', good, {'lib/with space\t.ts': 'export const value = 2;\n'}, ['CHANGELOG.md', 'lib/with space\t.ts'], '', False, True),
            ('normalized recorded paths', good, {'lib/source.ts': 'export const value = 2;\n'}, ['./CHANGELOG.md', str(fixture / 'lib/source.ts'), 'missing.ts'], '', False, True),
            ('ignored generated files', good, {'dist/output.js': '// generated\n', 'lib/version.ts': '// generated\n'}, ['CHANGELOG.md'], '', False, True),
            ('dependency modification', good, {'node_modules/tool.js': '// changed\n'}, ['CHANGELOG.md'], '', False, False),
            ('dependency addition', good, {'node_modules/prettier-plugin-fixture/index.js': '// added\n'}, ['CHANGELOG.md'], '', False, False),
            ('dependency deletion', good, {'node_modules/tool.js': None}, ['CHANGELOG.md'], '', False, False),
            ('dependency symlink', good, {'node_modules/.bin/prettier': ('symlink', '../other.js')}, ['CHANGELOG.md'], '', False, False),
            ('validation script modification', good, {'.github/scripts/validate-engineer-bot.sh': 'exit 0\n'}, ['CHANGELOG.md'], '', False, False),
            ('digest script modification', good, {'.github/scripts/engineer-bot-dependency-digest.sh': 'exit 0\n'}, ['CHANGELOG.md'], '', False, False),
            ('agent committed changes', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], 'commit', False, False),
            ('agent committed validator', good, {'.github/scripts/validate-engineer-bot.sh': 'exit 0\n'}, ['CHANGELOG.md', '.github/scripts/validate-engineer-bot.sh'], 'commit', False, False),
            ('skip-worktree edit', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md'], 'skip-worktree', False, False),
            ('assume-unchanged edit', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md'], 'assume-unchanged', False, False),
            ('locally excluded source', good, {'lib/hidden.ts': '// unrecorded\n'}, ['CHANGELOG.md'], 'exclude-source', False, False),
            ('locally excluded config', good, {'.npmrc': 'script-shell=/bin/false\n'}, ['CHANGELOG.md', '.npmrc'], 'exclude-config', False, False),
            ('globally excluded source', good, {'lib/hidden.ts': '// unrecorded\n'}, ['CHANGELOG.md'], 'global-exclude', False, False),
            ('self-ignoring nested gitignore', good, {'lib/sub/.gitignore': '*\n', 'lib/sub/hidden.ts': '// unrecorded\n'}, ['CHANGELOG.md'], '', False, False),
            ('configured fsmonitor with valid fix', good, {}, ['CHANGELOG.md'], 'fsmonitor', False, True),
            ('configured fsmonitor with unrecorded edit', good, {'lib/source.ts': 'export const value = 2;\n'}, ['CHANGELOG.md'], 'fsmonitor', False, False),
            ('preserved baseline duplicates', DUPLICATE_FIX, {}, ['CHANGELOG.md'], 'baseline-duplicates', False, True),
            ('removed baseline duplicate', DUPLICATE_FIX.replace('- Existing fix.\n  Existing detail.\n', '', 1), {}, ['CHANGELOG.md'], 'baseline-duplicates', False, False),
            ('increased baseline duplicates', DUPLICATE_FIX.replace('- New fix. (#261)', '- Existing fix.\n  Existing detail.\n- New fix. (#261)'), {}, ['CHANGELOG.md'], 'baseline-duplicates', False, False),
            ('relocated baseline section', RELOCATED_FIX, {}, ['CHANGELOG.md'], 'baseline-misplaced', False, True),
            ('new block suppression', good, {'lib/source.ts': '/* eslint-disable */\nexport const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new next-line suppression', good, {'lib/source.ts': '// eslint-disable-next-line no-alert\nalert(1);\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new inline suppression', good, {'lib/source.ts': 'alert(1); // eslint-disable-line no-alert\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new prettier suppression', good, {'lib/source.ts': '// prettier-ignore\nexport const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new next-line suppression after comment', good, {'lib/source.ts': '// Explain the fix.\n// eslint-disable-next-line no-alert\nalert(1);\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new block suppression after comment', good, {'lib/source.ts': '/* Explain the fix. */\n/* eslint-disable */\nalert(1);\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new trailing suppression after comment', good, {'lib/source.ts': 'alert(1); /* Explain the fix. */ /* eslint-disable-line no-alert */\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new prettier suppression after comment', good, {'lib/source.ts': '// Explain the fix.\n// prettier-ignore\nexport const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new inline rule config suppression', good, {'lib/source.ts': '/* eslint no-console: "off" */\nconsole.log(1);\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new environment suppression', good, {'lib/source.ts': '/* eslint-env browser */\nalert(1);\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new global suppression', good, {'lib/source.ts': '/* global foo */\nfoo();\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new globals suppression', good, {'lib/source.ts': '/* globals foo */\nfoo();\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new exported suppression', good, {'lib/source.ts': '/* exported foo */\nvar foo = 1;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new ts-ignore suppression', good, {'lib/source.ts': '// @ts-ignore\nconst value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new ts-expect-error suppression', good, {'lib/source.ts': '// @ts-expect-error\nconst value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new ts-nocheck suppression after comment', good, {'lib/source.ts': '// Explain the fix.\n// @ts-nocheck\nconst value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new triple-slash type suppression', good, {'lib/source.ts': '/// @ts-ignore\nconst value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new multiline type suppression', good, {'lib/source.ts': '/**\n * @ts-ignore */\nconst value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('ordinary prose comments', good, {'lib/source.ts': '// eslint flags this because it is unused.\n// global state is reset here.\n// exported for tests.\n/* eslint-config-airbnb provides defaults. */\nexport const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('directive prefix prose', good, {'lib/source.ts': '// eslint-disable-not-a-directive explains the test.\nexport const value = 2;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('block prose is a real globals suppression', good, {'lib/source.ts': '/* global state is reset here */\nstate();\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('directive string fixture', good, {'lib/source.ts': "export const directive = '/* eslint-disable */';\n"}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('directive template fixture', good, {'lib/source.ts': 'export const directive = `\n// prettier-ignore\n`;\n'}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('moved existing directive', good, {'lib/source.ts': "// A comment.\n// eslint-disable-next-line no-console\nconsole.log('original');\n"}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('reworded directive justification', good, {'lib/source.ts': "// eslint-disable-next-line no-console -- Better explanation.\nconsole.log('original');\n"}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('long justification delimiter', good, {'lib/source.ts': "// eslint-disable-next-line no-console ---- Better explanation.\nconsole.log('original');\n"}, ['CHANGELOG.md', 'lib/source.ts'], '', False, True),
            ('reflowed existing directive', good, {'lib/directives.ts': '/* eslint-disable\n  no-alert,\n  no-console\n  -- Better explanation. */\n'}, ['CHANGELOG.md', 'lib/directives.ts'], '', False, True),
            ('reordered existing directive', good, {'lib/directives.ts': '/* eslint-disable no-console, no-alert -- Better explanation. */\n'}, ['CHANGELOG.md', 'lib/directives.ts'], '', False, True),
            ('quoted existing directive rules', good, {'lib/directives.ts': '/* eslint-disable "no-console", "no-alert" -- Better explanation. */\n'}, ['CHANGELOG.md', 'lib/directives.ts'], '', False, True),
            ('broadened multi-rule suppression', good, {'lib/directives.ts': '/* eslint-disable no-console, no-alert, no-undef -- Better explanation. */\n'}, ['CHANGELOG.md', 'lib/directives.ts'], '', False, False),
            ('reworded existing type directive', good, {'lib/types.ts': '// @ts-ignore -- Better explanation.\nexport const value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/types.ts'], '', False, True),
            ('broadened type suppression', good, {'lib/types.ts': '// @ts-nocheck -- Better explanation.\nexport const value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/types.ts'], '', False, False),
            ('new type suppression in existing justification', good, {'lib/directives.ts': '/* eslint-disable no-alert, no-console -- Better explanation.\n * @ts-ignore */\nexport const value: number = "invalid";\n'}, ['CHANGELOG.md', 'lib/directives.ts'], '', False, False),
            ('broadened existing directive', good, {'lib/source.ts': "// eslint-disable-next-line no-console, no-alert\nconsole.log('original');\n"}, ['CHANGELOG.md', 'lib/source.ts'], '', False, False),
            ('new markdown suppression', good, {'notes.md': '<!-- prettier-ignore -->\ntext\n'}, ['CHANGELOG.md', 'notes.md'], '', False, False),
            ('new-file suppression', good, {'lib/new.ts': '// prettier-ignore\nexport const value = 2;\n'}, ['CHANGELOG.md', 'lib/new.ts'], '', False, False),
            ('Prettier failure', good, {}, ['CHANGELOG.md'], 'prettier', False, False),
            ('ESLint failure', good, {}, ['CHANGELOG.md'], 'lint', False, False),
            ('npm reads stdin', good, {}, ['CHANGELOG.md'], 'read-stdin', False, True),
        ]
        if baseline == EXISTING:
            cases += [('duplicate unreleased entry', baseline.replace('- Existing fix.', '- Existing fix.\n- Existing fix.'), {}, ['CHANGELOG.md'], '', False, False)]
            cases += [
                ('removed pending entry', good.replace('- Existing fix.\n  Existing detail.\n', ''), {}, ['CHANGELOG.md'], '', False, False),
                ('rewritten pending entry', good.replace('- Existing fix.', '- Rewritten fix.'), {}, ['CHANGELOG.md'], '', False, False),
                ('rewritten continuation', good.replace('  Existing detail.', '  Rewritten detail.'), {}, ['CHANGELOG.md'], '', False, False),
            ]
        for directory in ['', 'lib/', 'tests/e2e/']:
            for name in ['package.json', 'package-lock.json', 'package.yaml', 'tsconfig.json', 'tsconfig.build.json', '.npmrc', '.yarnrc', '.yarnrc.yml', '.nvmrc', '.eslintrc', '.eslintrc.json', '.eslintrc.js', '.eslintignore', '.prettierrc', '.prettierrc.js', '.prettierignore', '.editorconfig', '.gitignore', 'eslint.config.js', 'prettier.config.js']:
                path = directory + name
                cases.append(('protected config ' + path, good, {path: '{"changed":true}\n'}, ['CHANGELOG.md', path], '', False, False))
        for label, changelog, files, recorded, failure, staged, expected in cases:
            git(fixture, 'update-index', '--no-skip-worktree', '--', 'lib/source.ts')
            git(fixture, 'update-index', '--no-assume-unchanged', '--', 'lib/source.ts')
            git(fixture, 'reset', '--hard', '-q', base_sha)
            (fixture / '.git/info/exclude').write_text(exclude_baseline)
            global_excludes.write_text('')
            git(fixture, 'config', '--local', 'core.excludesFile', str(global_excludes))
            git(fixture, 'config', '--local', 'core.fsmonitor', 'false')
            git(fixture, 'clean', '-fdxq')
            validation_base_sha = base_sha
            if failure.startswith('baseline-'):
                write(fixture, 'CHANGELOG.md', DUPLICATE if failure == 'baseline-duplicates' else MISPLACED)
                git(fixture, 'add', '--', 'CHANGELOG.md')
                git(fixture, '-c', 'user.name=Workflow Test', '-c', 'user.email=workflow-test@example.com', 'commit', '--signoff', '-qm', 'pre-existing changelog quirk')
                validation_base_sha = git(fixture, 'rev-parse', 'HEAD').stdout.decode().strip()
            shutil.rmtree(fixture / 'node_modules', ignore_errors=True)
            write(fixture, 'node_modules/tool.js', '// original\n')
            write(fixture, 'node_modules/other.js', '// other\n')
            write(fixture, 'node_modules/.gitignore', '*\n')
            (fixture / 'node_modules/typescript').symlink_to(SCRIPT.parents[2] / 'node_modules/typescript')
            (fixture / 'node_modules/.bin').mkdir()
            (fixture / 'node_modules/.bin/prettier').symlink_to('../tool.js')
            snapshot_result = subprocess.run(['bash', '-e', '-o', 'pipefail', str(SCRIPT.with_name('engineer-bot-dependency-digest.sh'))], cwd=fixture, capture_output=True, check=True)
            checksum = snapshot_result.stdout.decode().strip()
            if changelog is None:
                (fixture / 'CHANGELOG.md').unlink()
            else:
                write(fixture, 'CHANGELOG.md', changelog)
            for name, content in files.items():
                if content is None:
                    (fixture / name).unlink()
                elif isinstance(content, tuple):
                    (fixture / name).unlink()
                    (fixture / name).symlink_to(content[1])
                else:
                    write(fixture, name, content)
            if staged:
                git(fixture, 'add', '--', 'lib/source.ts')
            if failure == 'commit':
                git(fixture, 'add', '-A')
                git(fixture, '-c', 'user.name=Workflow Test', '-c', 'user.email=workflow-test@example.com', 'commit', '--signoff', '-qm', 'agent changes')
            elif failure in ['skip-worktree', 'assume-unchanged']:
                git(fixture, 'update-index', '--' + failure, '--', 'lib/source.ts')
            elif failure in ['exclude-source', 'exclude-config']:
                hidden = '.npmrc' if failure == 'exclude-config' else 'lib/hidden.ts'
                (fixture / '.git/info/exclude').write_text(exclude_baseline + '\n' + hidden + '\n')
            elif failure == 'global-exclude':
                global_excludes.write_text('lib/hidden.ts\n')
            elif failure == 'fsmonitor':
                monitor = fixture / '.git/test-fsmonitor'
                monitor.write_text('#!/bin/sh\n: > "$FSMONITOR_MARKER"\nexit 1\n')
                monitor.chmod(0o755)
                git(fixture, 'config', '--local', 'core.fsmonitor', str(monitor))
            log = fixture / 'npm.log'
            log.unlink(missing_ok=True)
            monitor_marker = fixture / '.git/fsmonitor-invoked'
            monitor_marker.unlink(missing_ok=True)
            env = {'PATH': f'{binary}:{os.environ["PATH"]}', 'BASE_SHA': validation_base_sha, 'GIT_NO_REPLACE_OBJECTS': '1', 'TOUCHED_FILES': '\n'.join(recorded), 'TOOLCHAIN_SHA256': checksum, 'VALIDATION_LOG': str(log), 'VALIDATION_FAIL': failure, 'FSMONITOR_MARKER': str(monitor_marker), 'JFROG_ACCESS_TOKEN': 'test-placeholder', 'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'test-placeholder', 'ACTIONS_ID_TOKEN_REQUEST_URL': 'test-placeholder'}
            result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', 'validation_script=$(git show "$BASE_SHA:.github/scripts/validate-engineer-bot.sh"); bash -c "$validation_script"'], cwd=fixture, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True)
            assert (result.returncode == 0) == expected, (label, result.stdout, result.stderr)
            assert not monitor_marker.exists(), label
            if label == 'recorded deletion':
                assert 'pinned bot publisher cannot stage deleted files' in result.stderr
            if label.startswith('dependency '):
                assert 'Installed dependencies changed' in result.stdout
                assert not log.exists()
            if failure == 'commit':
                assert 'changed HEAD' in result.stdout
            if failure in ['skip-worktree', 'assume-unchanged']:
                assert 'Git index flags' in result.stderr
            if failure in ['commit', 'skip-worktree', 'assume-unchanged', 'exclude-source', 'exclude-config', 'global-exclude']:
                assert not log.exists()
            if 'suppression' in label or label == 'broadened existing directive':
                assert 'Do not add lint, formatting, or type-check suppression directives' in result.stderr
                assert not log.exists()
            if expected:
                assert log.read_text() == 'run prettier\nrun lint\n', label
            if failure == 'prettier':
                assert log.read_text() == 'run prettier\n', label
            count += 1
    print(f'Passed {count} gate cases: nested configs, recorded edits, changelog validation, credential scrubbing, and style failures.')
