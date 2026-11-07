const fs = require('fs');
const { execFileSync } = require('child_process');
const ts = require('typescript');

function directives(filename, text) {
  const comments = new Map();
  if (/\.[cm]?[jt]sx?$/.test(filename)) {
    const source = ts.createSourceFile(filename, text, ts.ScriptTarget.Latest, true);
    const collect = (start, end) => comments.set(`${start}:${end}`, text.slice(start, end));
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
  for (const comment of comments.values()) {
    const value = comment
      .replace(/^(?:\/\/|\/\*|<!--|#)/, '')
      .replace(/(?:\*\/|-->)$/, '')
      .trim()
      .replace(/\s+/g, ' ');
    if (/^(?:eslint-disable(?:-next-line|-line)?|prettier-ignore(?:-[a-z-]+)?)\b/.test(value)) {
      counts.set(value, (counts.get(value) || 0) + 1);
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
  const before = directives(filename, previous);
  const after = directives(filename, fs.readFileSync(filename, 'utf8'));
  for (const [directive, count] of after) {
    if (count > (before.get(directive) || 0)) {
      console.error(`::error::Do not add lint or formatting suppression directives: ${filename}`);
      process.exit(1);
    }
  }
}
