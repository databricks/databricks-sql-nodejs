// Copyright (c) 2026 Databricks, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Unit tests for `SeaSessionBackend.executeStatement` query-tags
 * threading. The JS-side adapter pre-serialises the public
 * `queryTags: Record<string, string | null | undefined>` map via
 * the existing `serializeQueryTags` util (so null-valued tags carry
 * through) and writes the result into the napi
 * `statementConf["query_tags"]`. The kernel then forwards the conf
 * overlay verbatim onto the SEA wire.
 *
 * These tests verify that the JS adapter constructs the napi options
 * shape correctly. End-to-end behaviour against a live warehouse is
 * exercised separately in `tests/e2e/sea/`.
 */

import { expect } from 'chai';
import sinon from 'sinon';
import SeaSessionBackend from '../../../lib/sea/SeaSessionBackend';
import {
  SeaNativeConnection,
  SeaNativeStatement,
  SeaNativeExecuteOptions,
} from '../../../lib/sea/SeaNativeLoader';
import IClientContext, { ClientConfig } from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';

class FakeNativeStatement implements SeaNativeStatement {
  public async fetchNextBatch() {
    return null;
  }

  public async schema() {
    return { ipcBytes: Buffer.alloc(0) };
  }

  public async cancel() {
    // no-op
  }

  public async close() {
    // no-op
  }
}

function makeFakeContext(): IClientContext {
  const logger: IDBSQLLogger = {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    log(_level: LogLevel, _message: string): void {
      // no-op
    },
  };
  const config = {} as ClientConfig;
  return {
    getConfig: () => config,
    getLogger: () => logger,
    getConnectionProvider: () => {
      throw new Error('not used');
    },
    getClient: () => {
      throw new Error('not used');
    },
    getDriver: () => {
      throw new Error('not used');
    },
  } as unknown as IClientContext;
}

describe('SeaSessionBackend — query tags threading', () => {
  let executeSpy: sinon.SinonSpy;
  let connection: SeaNativeConnection;
  let session: SeaSessionBackend;

  beforeEach(() => {
    const stmt = new FakeNativeStatement();
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    executeSpy = sinon.spy(async (_sql: string, _options?: SeaNativeExecuteOptions) => stmt);
    connection = {
      executeStatement: executeSpy,
      close: async () => {},
    } as unknown as SeaNativeConnection;
    session = new SeaSessionBackend({ connection, context: makeFakeContext() });
  });

  it('omits the napi options arg when queryTags is not set', async () => {
    await session.executeStatement('SELECT 1', {});
    expect(executeSpy.calledOnce).to.equal(true);
    expect(executeSpy.firstCall.args[0]).to.equal('SELECT 1');
    expect(executeSpy.firstCall.args[1]).to.equal(undefined);
  });

  it('omits the napi options arg when queryTags is empty', async () => {
    await session.executeStatement('SELECT 1', { queryTags: {} });
    expect(executeSpy.firstCall.args[1]).to.equal(undefined);
  });

  it('forwards a single tag through statementConf["query_tags"]', async () => {
    await session.executeStatement('SELECT 1', {
      queryTags: { team: 'platform' },
    });
    const opts = executeSpy.firstCall.args[1] as SeaNativeExecuteOptions;
    expect(opts.statementConf).to.deep.equal({ query_tags: 'team:platform' });
  });

  it('forwards multiple tags as comma-separated key:value pairs', async () => {
    await session.executeStatement('SELECT 1', {
      queryTags: { team: 'platform', env: 'staging' },
    });
    const opts = executeSpy.firstCall.args[1] as SeaNativeExecuteOptions;
    expect(opts.statementConf!.query_tags).to.match(
      /^(team:platform,env:staging|env:staging,team:platform)$/,
    );
  });

  it('preserves null-valued tags as bare keys (no colon)', async () => {
    await session.executeStatement('SELECT 1', {
      queryTags: { highPriority: null, team: 'platform' },
    });
    const opts = executeSpy.firstCall.args[1] as SeaNativeExecuteOptions;
    const encoded = opts.statementConf!.query_tags;
    // Object iteration is insertion-order for string keys; serializeQueryTags
    // follows that. Two possible orderings depending on Object.keys order.
    expect(encoded).to.match(
      /^(highPriority,team:platform|team:platform,highPriority)$/,
    );
  });

  it('preserves undefined-valued tags as bare keys (no colon)', async () => {
    await session.executeStatement('SELECT 1', {
      queryTags: { highPriority: undefined, team: 'platform' },
    });
    const opts = executeSpy.firstCall.args[1] as SeaNativeExecuteOptions;
    expect(opts.statementConf!.query_tags).to.contain('highPriority');
    expect(opts.statementConf!.query_tags).to.contain('team:platform');
  });

  it('escapes special chars (colon, comma, backslash) in values', async () => {
    await session.executeStatement('SELECT 1', {
      queryTags: { k: 'a:b,c\\d' },
    });
    const opts = executeSpy.firstCall.args[1] as SeaNativeExecuteOptions;
    // `:` → `\:`, `,` → `\,`, `\` → `\\`
    expect(opts.statementConf!.query_tags).to.equal('k:a\\:b\\,c\\\\d');
  });

  it('escapes backslashes in keys', async () => {
    await session.executeStatement('SELECT 1', {
      queryTags: { 'k\\1': 'v' },
    });
    const opts = executeSpy.firstCall.args[1] as SeaNativeExecuteOptions;
    expect(opts.statementConf!.query_tags).to.equal('k\\\\1:v');
  });
});
