import { expect } from 'chai';
import detectAgent from '../../../lib/utils/agentDetector';

describe('detectAgent', () => {
  const allAgents = [
    { envVar: 'ANTIGRAVITY_AGENT', product: 'antigravity' },
    { envVar: 'CLAUDECODE', product: 'claude-code' },
    { envVar: 'CLINE_ACTIVE', product: 'cline' },
    { envVar: 'CODEX_CI', product: 'codex' },
    { envVar: 'CURSOR_AGENT', product: 'cursor' },
    { envVar: 'GEMINI_CLI', product: 'gemini-cli' },
    { envVar: 'OPENCODE', product: 'opencode' },
  ];

  for (const { envVar, product } of allAgents) {
    it(`detects ${product} when ${envVar} is set`, () => {
      expect(detectAgent({ [envVar]: '1' })).to.equal(product);
    });
  }

  it('returns empty string when no agent is detected', () => {
    expect(detectAgent({})).to.equal('');
  });

  it('returns empty string when multiple agents are detected', () => {
    expect(detectAgent({ CLAUDECODE: '1', CURSOR_AGENT: '1' })).to.equal('');
  });

  it('ignores empty env var values', () => {
    expect(detectAgent({ CLAUDECODE: '' })).to.equal('');
  });

  it('ignores undefined env var values', () => {
    expect(detectAgent({ CLAUDECODE: undefined })).to.equal('');
  });
});
