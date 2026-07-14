/**
 * Copyright (c) 2025 Databricks Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

import { expect } from 'chai';
import sinon from 'sinon';
import Int64 from 'node-int64';
import { TOperationHandle, TOperationType, THandleIdentifier } from '../../../thrift/TCLIService_types';
import RowSetProvider from '../../../lib/result/RowSetProvider';
import ClientContextStub from '../.stubs/ClientContextStub';

function makeOperationHandle(): TOperationHandle {
  return {
    operationId: {
      guid: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
      secret: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    } as THandleIdentifier,
    operationType: TOperationType.EXECUTE_STATEMENT,
    hasResultSet: true,
  };
}

describe('RowSetProvider', () => {
  describe('chunk telemetry emission', () => {
    it('emits CLOUDFETCH_CHUNK with monotonic chunkIndex per fetch', async () => {
      const context = new ClientContextStub();
      const emitCloudFetchChunk = sinon.stub();
      context.telemetryEmitter = { emitCloudFetchChunk } as any;

      const handle = makeOperationHandle();
      const provider = new RowSetProvider(context, handle, [], false, 'stmt-1');

      await provider.fetchNext({ limit: 100 });
      await provider.fetchNext({ limit: 100 });

      expect(emitCloudFetchChunk.calledTwice).to.be.true;
      expect(emitCloudFetchChunk.firstCall.args[0].chunkIndex).to.equal(0);
      expect(emitCloudFetchChunk.secondCall.args[0].chunkIndex).to.equal(1);
      expect(emitCloudFetchChunk.firstCall.args[0].statementId).to.equal('stmt-1');
    });

    it('does not emit when statementId is undefined', async () => {
      const context = new ClientContextStub();
      const emitCloudFetchChunk = sinon.stub();
      context.telemetryEmitter = { emitCloudFetchChunk } as any;

      const provider = new RowSetProvider(context, makeOperationHandle(), [], false /* no statementId */);

      await provider.fetchNext({ limit: 100 });

      expect(emitCloudFetchChunk.called).to.be.false;
    });

    it('does not emit when telemetry emitter is undefined', async () => {
      const context = new ClientContextStub();
      // emitter left undefined → safeEmit short-circuits

      const provider = new RowSetProvider(context, makeOperationHandle(), [], false, 'stmt-1');

      // No assertion on rejection — should resolve normally without emitter
      const result = await provider.fetchNext({ limit: 100 });
      expect(result).to.exist;
    });

    it('swallows emitter exceptions and does not throw to the caller', async () => {
      const context = new ClientContextStub();
      const logSpy = sinon.spy(context.logger, 'log');
      const emitCloudFetchChunk = sinon.stub().throws(new Error('boom'));
      context.telemetryEmitter = { emitCloudFetchChunk } as any;

      const provider = new RowSetProvider(context, makeOperationHandle(), [], false, 'stmt-1');

      // Should not throw — telemetry never breaks the driver
      const result = await provider.fetchNext({ limit: 100 });
      expect(result).to.exist;

      // Should log at debug
      const debugCall = logSpy.getCalls().find((c) => /Telemetry emit error/.test(c.args[1] as string));
      expect(debugCall, 'should log a debug-level emit-error line').to.exist;
    });

    it('sums arrowBatches lengths into the bytes field', async () => {
      const context = new ClientContextStub();
      const emitCloudFetchChunk = sinon.stub();
      context.telemetryEmitter = { emitCloudFetchChunk } as any;

      // Override the driver's response with arrow batches only — no inline
      // columns, no binary columns — so the assertion is on a single shape.
      context.driver.fetchResultsResp = {
        ...context.driver.fetchResultsResp,
        results: {
          startRowOffset: new Int64(0),
          rows: [],
          arrowBatches: [
            { batch: Buffer.from(new Uint8Array(100)), rowCount: new Int64(10) },
            { batch: Buffer.from(new Uint8Array(50)), rowCount: new Int64(5) },
          ],
        },
      };

      const provider = new RowSetProvider(context, makeOperationHandle(), [], false, 'stmt-1');
      await provider.fetchNext({ limit: 100 });

      expect(emitCloudFetchChunk.calledOnce).to.be.true;
      expect(emitCloudFetchChunk.firstCall.args[0].bytes).to.equal(150);
    });

    it('counts column-based result bytes (F10)', async () => {
      const context = new ClientContextStub();
      const emitCloudFetchChunk = sinon.stub();
      context.telemetryEmitter = { emitCloudFetchChunk } as any;

      // COLUMN_BASED_SET shape: string and i32 columns, no arrow batches.
      // Before F10, this path emitted bytes:0 because the byte counter only
      // looked at arrowBatches.
      context.driver.fetchResultsResp = {
        ...context.driver.fetchResultsResp,
        results: {
          startRowOffset: new Int64(0),
          rows: [],
          columns: [
            { stringVal: { values: ['hello', 'world'], nulls: Buffer.from([0x00]) } },
            { i32Val: { values: [1, 2, 3], nulls: Buffer.from([0x00, 0x00]) } },
          ],
        },
      };

      const provider = new RowSetProvider(context, makeOperationHandle(), [], false, 'stmt-1');
      await provider.fetchNext({ limit: 100 });

      expect(emitCloudFetchChunk.calledOnce).to.be.true;
      // strings: "hello"(5) + "world"(5) + nulls(1) = 11; i32: 3*4 + nulls(2) = 14; total 25
      expect(emitCloudFetchChunk.firstCall.args[0].bytes).to.equal(25);
    });

    it('counts binaryColumns bytes (F10)', async () => {
      const context = new ClientContextStub();
      const emitCloudFetchChunk = sinon.stub();
      context.telemetryEmitter = { emitCloudFetchChunk } as any;

      context.driver.fetchResultsResp = {
        ...context.driver.fetchResultsResp,
        results: {
          startRowOffset: new Int64(0),
          rows: [],
          binaryColumns: Buffer.from(new Uint8Array(64)),
        },
      };

      const provider = new RowSetProvider(context, makeOperationHandle(), [], false, 'stmt-1');
      await provider.fetchNext({ limit: 100 });

      expect(emitCloudFetchChunk.firstCall.args[0].bytes).to.equal(64);
    });
  });
});
