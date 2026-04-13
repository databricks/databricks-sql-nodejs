/**
 * Detects whether the Node.js SQL driver is being invoked by an AI coding agent
 * by checking for well-known environment variables that agents set in their
 * spawned shell processes.
 *
 * Detection only succeeds when exactly one agent environment variable is present,
 * to avoid ambiguous attribution when multiple agent environments overlap.
 *
 * Adding a new agent requires only a new entry in `knownAgents`.
 *
 * References for each environment variable:
 *   - ANTIGRAVITY_AGENT: Closed source. Google Antigravity sets this variable.
 *   - CLAUDECODE: https://github.com/anthropics/claude-code (sets CLAUDECODE=1)
 *   - CLINE_ACTIVE: https://github.com/cline/cline (shipped in v3.24.0)
 *   - CODEX_CI: https://github.com/openai/codex (part of UNIFIED_EXEC_ENV array in codex-rs)
 *   - CURSOR_AGENT: Closed source. Referenced in a gist by johnlindquist.
 *   - GEMINI_CLI: https://google-gemini.github.io/gemini-cli/docs/tools/shell.html (sets GEMINI_CLI=1)
 *   - OPENCODE: https://github.com/opencode-ai/opencode (sets OPENCODE=1)
 */

const knownAgents: Array<{ envVar: string; product: string }> = [
  { envVar: 'ANTIGRAVITY_AGENT', product: 'antigravity' },
  { envVar: 'CLAUDECODE', product: 'claude-code' },
  { envVar: 'CLINE_ACTIVE', product: 'cline' },
  { envVar: 'CODEX_CI', product: 'codex' },
  { envVar: 'CURSOR_AGENT', product: 'cursor' },
  { envVar: 'GEMINI_CLI', product: 'gemini-cli' },
  { envVar: 'OPENCODE', product: 'opencode' },
];

export default function detectAgent(env: Record<string, string | undefined> = process.env): string {
  const detected = knownAgents.filter((a) => env[a.envVar]).map((a) => a.product);

  if (detected.length === 1) {
    return detected[0];
  }
  return '';
}
