const { expect, AssertionError } = require('chai');

const { buildUserAgentString, definedOrError, formatProgress, ProgressUpdateTransformer } = require('../../lib/utils');
const CloseableCollection = require('../../lib/utils/CloseableCollection').default;

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
      /^(?<productName>NodejsDatabricksSqlConnector)\/(?<productVersion>\d+\.\d+\.\d+(-[^(]+)?)\s*\((?<comment>[^)]+)\)$/i;
    const match = re.exec(ua);
    expect(match).to.not.be.eq(null);

    const { comment } = match.groups;

    expect(comment.split(';').length).to.be.gte(2); // at least Node and OS version should be there

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

describe('formatProgress', () => {
  it('formats progress', () => {
    const result = formatProgress({
      headerNames: [],
      rows: [],
    });
    expect(result).to.be.eq('\n');
  });
});

describe('ProgressUpdateTransformer', () => {
  it('should have equal columns', () => {
    const t = new ProgressUpdateTransformer();

    expect(t.formatRow(['Column 1', 'Column 2'])).to.be.eq('Column 1  |Column 2  ');
  });

  it('should format response as table', () => {
    const t = new ProgressUpdateTransformer({
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

describe('CloseableCollection', () => {
  it('should add item if not already added', () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const item = {};

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);
  });

  it('should add item if it is already added', () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const item = {};

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);
  });

  it('should delete item if already added', () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const item = {};

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);

    collection.delete(item);
    expect(item.onClose).to.be.undefined;
    expect(collection.items.size).to.be.eq(0);
  });

  it('should delete item if not added', () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const item = {};
    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);

    const otherItem = { onClose: () => {} };
    collection.delete(otherItem);
    // if item is not in collection - it should be just skipped
    expect(otherItem.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);
  });

  it('should delete item if it was closed', async () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const item = {
      close() {
        this.onClose();
        return Promise.resolve();
      },
    };

    collection.add(item);
    expect(item.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(1);

    await item.close();
    expect(item.onClose).to.be.undefined;
    expect(collection.items.size).to.be.eq(0);
  });

  it('should close all and delete all items', async () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const item1 = {
      close() {
        this.onClose();
        return Promise.resolve();
      },
    };

    const item2 = {
      close() {
        this.onClose();
        return Promise.resolve();
      },
    };

    collection.add(item1);
    collection.add(item2);
    expect(item1.onClose).to.be.not.undefined;
    expect(item2.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(2);

    await collection.closeAll();
    expect(item1.onClose).to.be.undefined;
    expect(item2.onClose).to.be.undefined;
    expect(collection.items.size).to.be.eq(0);
  });

  it('should close all and delete only first successfully closed items', async () => {
    const collection = new CloseableCollection();
    expect(collection.items.size).to.be.eq(0);

    const errorMessage = 'Error from item 2';

    const item1 = {
      close() {
        this.onClose();
        return Promise.resolve();
      },
    };

    const item2 = {
      close() {
        // Item should call `.onClose` only if it was successfully closed
        return Promise.reject(new Error(errorMessage));
      },
    };

    const item3 = {
      close() {
        this.onClose();
        return Promise.resolve();
      },
    };

    collection.add(item1);
    collection.add(item2);
    collection.add(item3);
    expect(item1.onClose).to.be.not.undefined;
    expect(item2.onClose).to.be.not.undefined;
    expect(item3.onClose).to.be.not.undefined;
    expect(collection.items.size).to.be.eq(3);

    try {
      await collection.closeAll();
      expect.fail('It should throw an error');
    } catch (error) {
      if (error instanceof AssertionError) {
        throw error;
      }
      expect(error.message).to.eq(errorMessage);
      expect(item1.onClose).to.be.undefined;
      expect(item2.onClose).to.be.not.undefined;
      expect(item3.onClose).to.be.not.undefined;
      expect(collection.items.size).to.be.eq(2);
    }
  });
});
