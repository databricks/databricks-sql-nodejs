const { expect } = require('chai');

const { buildUserAgentString } = require('../../dist/utils');

describe('buildUserAgentString', () => {
  // It should follow https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3 and
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
  //
  // UserAgent ::= <ProductName> '/' <ProductVersion> '(' <Comment> ')'
  // ProductName ::= 'NodejsDatabricksSqlConnector'
  // <Comment> ::= [ <ClientId> ';' ] 'Node.js' <NodeJsVersion> ';' <OSPlatform> <OSVersion>
  //
  // Examples:
  // - with <ClientId> provided: NodejsDatabricksSqlConnector/0.1.8-beta.1 (Client ID; Node.js 16.13.1; Darwin 21.5.0)
  // - without <ClientId> provided: NodejsDatabricksSqlConnector/0.1.8-beta.1 (Node.js 16.13.1; Darwin 21.5.0)

  function checkUserAgentString(ua, clientId) {
    // Prefix: 'NodejsDatabricksSqlConnector/'
    // Version: three period-separated digits and optional suffix
    const re =
      /^(?<productName>NodejsDatabricksSqlConnector)\/(?<productVersion>\d+\.\d+\.\d+(-[^(]+)?)\((?<comment>[^)]+)\)$/i;
    const match = re.exec(ua);
    expect(match).to.not.be.eq(null);

    const { comment } = match.groups;

    expect(comment.split(';').length).to.be.gte(2); // at least Node ans OS version should be there

    if (clientId) {
      expect(comment.trim()).to.satisfy((s) => s.startsWith(`${clientId};`));
    }
  }

  it('matches pattern with clientId', () => {
    const clientId = 'Some Client ID';
    const ua = buildUserAgentString(clientId);
    checkUserAgentString(ua, clientId);
  });

  it('matches pattern without clientId', () => {
    const ua = buildUserAgentString();
    checkUserAgentString(ua);
  });
});
