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
 * Unit tests for the SEA operation lifecycle (`cancel`, `close`,
 * `finished`) — both via the `SeaOperationLifecycle` helpers and
 * via `SeaOperationBackend` which composes them.
 *
 * We mock the napi binding's `Statement` handle so the test process
 * doesn't touch any native code; the helpers and the backend are
 * structurally typed against `SeaStatementHandle` exactly so this
 * works.
 */

import { expect } from 'chai';
import sinon from 'sinon';
import { OperationStatus, OperationState } from '../../../lib/contracts/OperationStatus';
import IClientContext from '../../../lib/contracts/IClientContext';
import IDBSQLLogger, { LogLevel } from '../../../lib/contracts/IDBSQLLogger';
import {
  SeaStatementHandle,
  createLifecycleState,
  seaCancel,
  seaClose,
  seaFinished,
  failIfNotActive,
} from '../../../lib/sea/SeaOperationLifecycle';
import SeaOperationBackend from '../../../lib/sea/SeaOperationBackend';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import HiveDriverError from '../../../lib/errors/HiveDriverError';

class TestLogger implements IDBSQLLogger {
  public readonly entries: Array<{ level: LogLevel; message: string }> = [];

  log(level: LogLevel, message: string): void {
    this.entries.push({ level, message });
  }
}

function makeContext(): IClientContext {
  const logger = new TestLogger();
  // Only `getLogger` is exercised by the lifecycle helpers; the rest
  // of `IClientContext` is stubbed to throw so accidental coupling
  // to it shows up loudly in tests.
  const notUsed = () => {
    throw new Error('IClientContext member not expected to be used by lifecycle');
  };
  return {
    getConfig: notUsed,
    getLogger: () => logger,
    getConnectionProvider: notUsed,
    getClient: notUsed,
    getDriver: notUsed,
  } as unknown as IClientContext;
}

function makeStatement(overrides: Partial<SeaStatementHandle> = {}): {
  handle: SeaStatementHandle;
  cancel: sinon.SinonStub;
  close: sinon.SinonStub;
} {
  const cancel = sinon.stub().resolves();
  const close = sinon.stub().resolves();
  return {
    handle: { cancel, close, ...overrides },
    cancel,
    close,
  };
}

describe('SeaOperationLifecycle (helpers)', () => {
  describe('seaCancel', () => {
    it('calls statement.cancel() and resolves with a success Status', async () => {
      const ctx = makeContext();
      const { handle, cancel } = makeStatement();
      const state = createLifecycleState();

      const status = await seaCancel(state, handle, ctx, 'op-id-1');

      expect(cancel.calledOnce).to.equal(true);
      expect(status.isSuccess).to.equal(true);
      expect(state.isCancelled).to.equal(true);
    });

    it('is idempotent — second call does not hit the binding', async () => {
      const ctx = makeContext();
      const { handle, cancel } = makeStatement();
      const state = createLifecycleState();

      await seaCancel(state, handle, ctx, 'op-id-2');
      await seaCancel(state, handle, ctx, 'op-id-2');

      expect(cancel.calledOnce).to.equal(true);
    });

    it('short-circuits when the operation is already closed', async () => {
      const ctx = makeContext();
      const { handle, cancel } = makeStatement();
      const state = createLifecycleState();
      state.isClosed = true;

      const status = await seaCancel(state, handle, ctx, 'op-id-3');

      expect(cancel.called).to.equal(false);
      expect(status.isSuccess).to.equal(true);
    });

    it('sets isCancelled BEFORE awaiting the binding (so concurrent fetch sees it)', async () => {
      const ctx = makeContext();
      const state = createLifecycleState();

      // Cancel returns a promise that resolves only when we say so.
      let release: (() => void) | undefined;
      const cancelPromise = new Promise<void>((resolve) => {
        release = resolve;
      });
      const handle: SeaStatementHandle = {
        cancel: () => cancelPromise,
        close: async () => undefined,
      };

      const inflight = seaCancel(state, handle, ctx, 'op-id-4');

      // Yield once so the synchronous prelude of seaCancel runs.
      await Promise.resolve();
      expect(state.isCancelled).to.equal(true);
      // Before the await resolves, failIfNotActive must already throw.
      expect(() => failIfNotActive(state)).to.throw();

      release!();
      const status = await inflight;
      expect(status.isSuccess).to.equal(true);
    });

    it('propagates binding errors via the kernel error mapping', async () => {
      const ctx = makeContext();
      const state = createLifecycleState();
      const handle: SeaStatementHandle = {
        cancel: async () => {
          // Simulate the binding's JSON-envelope error format.
          const payload = JSON.stringify({
            code: 'InvalidStatementHandle',
            message: 'statement already closed',
          });
          throw new Error(`__databricks_error__:${payload}`);
        },
        close: async () => undefined,
      };

      let thrown: unknown;
      try {
        await seaCancel(state, handle, ctx, 'op-err-1');
      } catch (err) {
        thrown = err;
      }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.contain('statement already closed');
    });

    it('logs a debug message tagged with the operation id', async () => {
      const ctx = makeContext();
      const logger = ctx.getLogger() as TestLogger;
      const { handle } = makeStatement();
      const state = createLifecycleState();

      await seaCancel(state, handle, ctx, 'op-id-log');

      expect(logger.entries.some((e) => e.level === LogLevel.debug && e.message.includes('op-id-log'))).to.equal(true);
    });
  });

  describe('seaClose', () => {
    it('calls statement.close() and resolves with a success Status', async () => {
      const ctx = makeContext();
      const { handle, close } = makeStatement();
      const state = createLifecycleState();

      const status = await seaClose(state, handle, ctx, 'op-close-1');

      expect(close.calledOnce).to.equal(true);
      expect(status.isSuccess).to.equal(true);
      expect(state.isClosed).to.equal(true);
    });

    it('is idempotent — second call does not hit the binding', async () => {
      const ctx = makeContext();
      const { handle, close } = makeStatement();
      const state = createLifecycleState();

      await seaClose(state, handle, ctx, 'op-close-2');
      await seaClose(state, handle, ctx, 'op-close-2');

      expect(close.calledOnce).to.equal(true);
    });

    it('propagates binding errors via the kernel error mapping', async () => {
      const ctx = makeContext();
      const state = createLifecycleState();
      const handle: SeaStatementHandle = {
        cancel: async () => undefined,
        close: async () => {
          const payload = JSON.stringify({
            code: 'NetworkError',
            message: 'connection reset by peer',
          });
          throw new Error(`__databricks_error__:${payload}`);
        },
      };

      let thrown: unknown;
      try {
        await seaClose(state, handle, ctx, 'op-err-close');
      } catch (err) {
        thrown = err;
      }
      expect(thrown).to.be.instanceOf(HiveDriverError);
      expect((thrown as Error).message).to.contain('connection reset');
    });
  });

  describe('seaFinished', () => {
    it('resolves immediately when no callback is provided (M0 no-op)', async () => {
      const state = createLifecycleState();
      const start = Date.now();
      await seaFinished(state);
      // Should be near-instantaneous — no 100ms poll.
      expect(Date.now() - start).to.be.lessThan(50);
    });

    it('invokes the progress callback exactly once with a FINISHED status', async () => {
      const state = createLifecycleState();
      const callback = sinon.stub();

      await seaFinished(state, { callback });

      expect(callback.calledOnce).to.equal(true);
      const arg = callback.firstCall.args[0] as OperationStatus;
      expect(arg.state).to.equal(OperationState.Succeeded);
      expect(arg.hasResultSet).to.equal(true);
    });

    it('awaits an async progress callback', async () => {
      const state = createLifecycleState();
      let resolvedInsideCallback = false;
      const callback = async () => {
        await new Promise<void>((r) => setTimeout(r, 10));
        resolvedInsideCallback = true;
      };

      await seaFinished(state, { callback });

      expect(resolvedInsideCallback).to.equal(true);
    });

    it('is a no-op when the operation is already cancelled', async () => {
      const state = createLifecycleState();
      state.isCancelled = true;
      const callback = sinon.stub();

      await seaFinished(state, { callback });

      expect(callback.called).to.equal(false);
    });
  });

  describe('failIfNotActive', () => {
    it('throws OperationStateError(Canceled) when cancelled', () => {
      const state = createLifecycleState();
      state.isCancelled = true;
      // The kernel-error mapping routes Cancelled → OperationStateError.
      try {
        failIfNotActive(state);
        expect.fail('expected throw');
      } catch (err) {
        expect(err).to.be.instanceOf(OperationStateError);
        expect((err as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Canceled);
      }
    });

    it('throws HiveDriverError when closed', () => {
      const state = createLifecycleState();
      state.isClosed = true;
      try {
        failIfNotActive(state);
        expect.fail('expected throw');
      } catch (err) {
        expect(err).to.be.instanceOf(HiveDriverError);
      }
    });

    it('does nothing when active', () => {
      const state = createLifecycleState();
      // Should not throw.
      failIfNotActive(state);
    });
  });
});

describe('SeaOperationBackend (lifecycle integration)', () => {
  it('cancel() forwards to statement.cancel()', async () => {
    const ctx = makeContext();
    const { handle, cancel } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    const status = await op.cancel();

    expect(cancel.calledOnce).to.equal(true);
    expect(status.isSuccess).to.equal(true);
  });

  it('close() forwards to statement.close()', async () => {
    const ctx = makeContext();
    const { handle, close } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    const status = await op.close();

    expect(close.calledOnce).to.equal(true);
    expect(status.isSuccess).to.equal(true);
  });

  it('finished() resolves immediately and fires the callback once', async () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    const responses: OperationStatus[] = [];
    const start = Date.now();
    await op.waitUntilReady({ callback: (r) => responses.push(r) });

    expect(Date.now() - start).to.be.lessThan(50);
    expect(responses).to.have.length(1);
    expect(responses[0].state).to.equal(OperationState.Succeeded);
  });

  it('fetchChunk after cancel throws the cancellation error', async () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    await op.cancel();

    let thrown: unknown;
    try {
      await op.fetchChunk({ limit: 10 });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).to.be.instanceOf(OperationStateError);
    expect((thrown as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Canceled);
  });

  it('cancel() is idempotent across the backend surface', async () => {
    const ctx = makeContext();
    const { handle, cancel } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    await op.cancel();
    await op.cancel();
    await op.cancel();

    expect(cancel.calledOnce).to.equal(true);
  });

  it('close() is idempotent across the backend surface', async () => {
    const ctx = makeContext();
    const { handle, close } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    await op.close();
    await op.close();

    expect(close.calledOnce).to.equal(true);
  });

  it('status() reports FINISHED_STATE when active', async () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    const status = await op.status(false);
    expect(status.state).to.equal(OperationState.Succeeded);
  });

  it('status() reports CANCELED_STATE after cancel', async () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    await op.cancel();
    const status = await op.status(false);
    expect(status.state).to.equal(OperationState.Cancelled);
  });

  it('id getter is stable', () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx, id: 'fixed-id' });

    expect(op.id).to.equal('fixed-id');
    expect(op.id).to.equal('fixed-id');
  });

  it('id getter defaults to a uuid when none is supplied', () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    // RFC4122 v4 — 36 chars with hyphens at positions 8/13/18/23.
    expect(op.id).to.match(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  it('hasResultSet is true by default (kernel always streams)', () => {
    const ctx = makeContext();
    const { handle } = makeStatement();
    const op = new SeaOperationBackend({ statement: handle, context: ctx });

    expect(op.hasResultSet()).to.equal(true);
  });
});
