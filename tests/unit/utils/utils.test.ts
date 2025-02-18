import { expect } from 'chai';
import Int64 from 'node-int64';

import { TJobExecutionStatus, TProgressUpdateResp } from '../../../thrift/TCLIService_types';

import { buildUserAgentString, definedOrError, formatProgress, ProgressUpdateTransformer } from '../../../lib/utils';

const progressUpdateResponseStub: TProgressUpdateResp = {
  headerNames: [],
  rows: [],
  progressedPercentage: 0,
  status: TJobExecutionStatus.NOT_AVAILABLE,
  footerSummary: '',
  startTime: new Int64(0),
};

describe('buildUserAgentString', () => {
  // It should follow https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3 and
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
  //
  // UserAgent ::= <ProductName> '/' <ProductVersion> '(' <Comment> ')'
  // ProductName ::= 'NodejsDatabricksSqlConnector'
  // <Comment> ::= [ <userAgentHeader> ';' ] 'Node.js' <NodeJsVersion> ';' <OSPlatform> <OSVersion>
  //
  // Examples:
  // - with <userAgentHeader> provided: NodejsDatabricksSqlConnector/0.1.8-beta.1 (Client ID; Node.js 16.13.1; Darwin 21.5.0)
  // - without <userAgentHeader> provided: NodejsDatabricksSqlConnector/0.1.8-beta.1 (Node.js 16.13.1; Darwin 21.5.0)

  function checkUserAgentString(ua: string, userAgentHeader?: string) {
    // Prefix: 'NodejsDatabricksSqlConnector/'
    // Version: three period-separated digits and optional suffix
    const re =
      /^(?<productName>NodejsDatabricksSqlConnector)\/(?<productVersion>\d+\.\d+\.\d+(-[^(]+)?)\s*\((?<comment>[^)]+)\)$/i;
    const match = re.exec(ua);
    expect(match).to.not.be.eq(null);

    const { comment } = match?.groups ?? {};

    expect(comment.split(';').length).to.be.gte(2); // at least Node and OS version should be there

    if (userAgentHeader) {
      expect(comment.trim()).to.satisfy((s: string) => s.startsWith(`${userAgentHeader};`));
    }
  }

  it('matches pattern with userAgentHeader', () => {
    const userAgentHeader = 'Some Client ID';
    const ua = buildUserAgentString(userAgentHeader);
    checkUserAgentString(ua, userAgentHeader);
  });

  it('matches pattern without userAgentHeader', () => {
    const ua = buildUserAgentString();
    checkUserAgentString(ua);
  });
});

describe('formatProgress', () => {
  it('formats progress', () => {
    const result = formatProgress(progressUpdateResponseStub);
    expect(result).to.be.eq('\n');
  });
});

describe('ProgressUpdateTransformer', () => {
  it('should have equal columns', () => {
    const t = new ProgressUpdateTransformer(progressUpdateResponseStub);

    expect(t.formatRow(['Column 1', 'Column 2'])).to.be.eq('Column 1  |Column 2  ');
  });

  it('should format response as table', () => {
    const t = new ProgressUpdateTransformer({
      ...progressUpdateResponseStub,
      headerNames: ['Column 1', 'Column 2'],
      rows: [
        ['value 1.1', 'value 1.2'],
        ['value 2.1', 'value 2.2'],
      ],
      footerSummary: 'footer',
    });

    expect(String(t)).to.be.eq(
      'Column 1  |Column 2  \n' + 'value 1.1 |value 1.2 \n' + 'value 2.1 |value 2.2 \n' + 'footer',
    );
  });
});

describe('definedOrError', () => {
  it('should return value if it is defined', () => {
    const values = [null, 0, 3.14, false, true, '', 'Hello, World!', [], {}];
    for (const value of values) {
      const result = definedOrError(value);
      expect(result).to.be.equal(value);
    }
  });

  it('should throw error if value is undefined', () => {
    expect(() => {
      definedOrError(undefined);
    }).to.throw();
  });
});
