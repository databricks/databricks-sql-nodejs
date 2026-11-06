import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile


SCRIPT = Path(__file__).resolve().parents[1] / 'validate-engineer-bot.sh'
BASE = '# Release History\n\n## 3.0.0\n\n- Released fix.\n\n## 2.0.0\n\n- Older fix.\n'
VALID = BASE.replace('## 3.0.0', '## Unreleased\n\n- New fix. (#261)\n\n## 3.0.0')
EXISTING = BASE.replace('## 3.0.0', '## Unreleased\n\n- Existing fix.\n\n## 3.0.0')


def git(root, *args):
    return subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-C', str(root), *args], check=True, capture_output=True)


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
        for name, content in [('CHANGELOG.md', baseline), ('.eslintrc', '{}\n'), ('package.json', '{}\n'), ('.gitignore', 'dist/\nlib/version.ts\n*.log\nnode_modules/\n'), ('lib/source.ts', 'export const value = 1;\n')]:
            write(fixture, name, content)
        for name in ['validate-engineer-bot.sh', 'engineer-bot-dependency-digest.sh']:
            write(fixture, '.github/scripts/' + name, SCRIPT.with_name(name).read_text())
        git(fixture, 'add', '.')
        git(fixture, '-c', 'user.name=Workflow Test', '-c', 'user.email=workflow-test@example.com', 'commit', '--signoff', '-qm', 'validation baseline')
        good = VALID if baseline == BASE else baseline.replace('- Existing fix.', '- Existing fix.\n- New fix. (#261)')
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
            ('Prettier failure', good, {}, ['CHANGELOG.md'], 'prettier', False, False),
            ('ESLint failure', good, {}, ['CHANGELOG.md'], 'lint', False, False),
            ('npm reads stdin', good, {}, ['CHANGELOG.md'], 'read-stdin', False, True),
        ]
        if baseline == EXISTING:
            cases += [('duplicate unreleased entry', baseline.replace('- Existing fix.', '- Existing fix.\n- Existing fix.'), {}, ['CHANGELOG.md'], '', False, False)]
        for directory in ['', 'lib/', 'tests/e2e/']:
            for name in ['package.json', 'package-lock.json', 'tsconfig.json', 'tsconfig.build.json', '.npmrc', '.yarnrc', '.yarnrc.yml', '.nvmrc', '.eslintrc', '.eslintrc.json', '.eslintrc.js', '.eslintignore', '.prettierrc', '.prettierrc.js', '.prettierignore', '.editorconfig', '.gitignore', 'eslint.config.js', 'prettier.config.js']:
                path = directory + name
                cases.append(('protected config ' + path, good, {path: '{"changed":true}\n'}, ['CHANGELOG.md', path], '', False, False))
        for label, changelog, files, recorded, failure, staged, expected in cases:
            git(fixture, 'reset', '--hard', '-q', 'HEAD')
            git(fixture, 'clean', '-fdq')
            shutil.rmtree(fixture / 'node_modules', ignore_errors=True)
            write(fixture, 'node_modules/tool.js', '// original\n')
            write(fixture, 'node_modules/other.js', '// other\n')
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
            log = fixture / 'npm.log'
            log.unlink(missing_ok=True)
            env = {'PATH': f'{binary}:{os.environ["PATH"]}', 'TOUCHED_FILES': '\n'.join(recorded), 'TOOLCHAIN_SHA256': checksum, 'VALIDATION_LOG': str(log), 'VALIDATION_FAIL': failure, 'JFROG_ACCESS_TOKEN': 'test-placeholder', 'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'test-placeholder', 'ACTIONS_ID_TOKEN_REQUEST_URL': 'test-placeholder'}
            result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', 'validation_script=$(git show HEAD:.github/scripts/validate-engineer-bot.sh); bash -c "$validation_script"'], cwd=fixture, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True)
            assert (result.returncode == 0) == expected, (label, result.stdout, result.stderr)
            if label == 'recorded deletion':
                assert 'pinned bot publisher cannot stage deleted files' in result.stderr
            if label.startswith('dependency '):
                assert 'Installed dependencies changed' in result.stdout
                assert not log.exists()
            if expected:
                assert log.read_text() == 'run prettier\nrun lint\n', label
            if failure == 'prettier':
                assert log.read_text() == 'run prettier\n', label
            count += 1
    print(f'Passed {count} gate cases: nested configs, recorded edits, changelog validation, credential scrubbing, and style failures.')
