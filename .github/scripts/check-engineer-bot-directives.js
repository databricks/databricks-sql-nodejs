const fs = require('fs');
const { execFileSync } = require('child_process');
const ts = require('typescript');

function directives(filename, text) {
  const comments = new Map();
  if (/\.[cm]?[jt]sx?$/.test(filename)) {
    const source = ts.createSourceFile(filename, text, ts.ScriptTarget.Latest, true);
    const collect = (start, end) => {
      comments.set(`${start}:${end}`, text.slice(start, end));
    };
    const visit = (node) => {
      ts.forEachLeadingCommentRange(text, node.pos, collect);
      ts.forEachTrailingCommentRange(text, node.end, collect);
      node.getChildren(source).forEach(visit);
    };
    visit(source);
  } else {
    for (const match of text.matchAll(/^[ \t]*(?:<!--|\/\*|\/\/|#)\s*prettier-ignore[^\r\n]*/gm)) {
      comments.set(match.index, match[0].trim());
    }
  }
  const counts = new Map();
  const add = (directive) => {
    counts.set(directive, (counts.get(directive) || 0) + 1);
  };
  for (const comment of comments.values()) {
    const value = comment
      .replace(/^(?:\/\/|\/\*|<!--|#)/, '')
      .replace(/(?:\*\/|-->)$/, '')
      .trim()
      .replace(/\s+/g, ' ');
    const eslintDirective =
      /^eslint-disable-(?:next-line|line)(?:\s|$)/.test(value) ||
      (comment.startsWith('/*') && /^(?:eslint(?:-disable|-enable|-env)?|globals?|exported)(?:\s|$)/.test(value));
    if (eslintDirective) {
      let directive = value.split(/\s-{2,}\s/, 1)[0];
      const disable = /^(eslint-(?:disable(?:-next-line|-line)?|enable))(?:\s+(.*))?$/.exec(directive);
      if (disable) {
        const rules = (disable[2] || '')
          .split(',')
          .map((rule) => rule.trim().replace(/^(['"])(.*)\1$/, '$2'))
          .filter(Boolean)
          .sort();
        directive = `${disable[1]} ${rules.join(',')}`.trim();
      }
      add(directive);
    } else if (/^prettier-ignore(?:-[a-z-]+)?\b/.test(value)) {
      add(value);
    }
    for (const line of comment.split(/\r?\n/)) {
      const directive = /^[\s/*]*(@ts-(?:ignore|expect-error|nocheck))\b/.exec(line);
      if (directive) add(directive[1]);
    }
  }
  return counts;
}

for (const filename of JSON.parse(process.env.GATE_PATHS)) {
  let previous = '';
  try {
    previous = execFileSync('git', ['show', `${process.env.BASE_SHA}:${filename}`], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
  } catch {
    // New files have no baseline comments.
  }
  // Counts allow directives to move with refactored code; coverage still needs diff review.
  const before = directives(filename, previous);
  const after = directives(filename, fs.readFileSync(filename, 'utf8'));
  for (const [directive, count] of before) {
    if (!/^eslint-enable(?:\s|$)/.test(directive)) continue;
    const removedEnables = Math.max(0, count - (after.get(directive) || 0));
    const disable = directive.replace(/^eslint-enable/, 'eslint-disable');
    const removedDisables = Math.max(0, (before.get(disable) || 0) - (after.get(disable) || 0));
    if (removedEnables > removedDisables) {
      console.error(
        `::error::Do not remove eslint-enable directives while keeping matching block disables: ${filename}`,
      );
      process.exit(1);
    }
  }
  for (const [directive, count] of after) {
    if (!/^eslint-enable(?:\s|$)/.test(directive) && count > (before.get(directive) || 0)) {
      console.error(`::error::Do not add lint, formatting, or type-check suppression directives: ${filename}`);
      process.exit(1);
    }
  }
}
