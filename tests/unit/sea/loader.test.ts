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

import { expect } from 'chai';
import { SeaNativeLoader, SeaNativeBinding } from '../../../lib/sea/SeaNativeLoader';

// Pure-logic tests for SeaNativeLoader. These exercise the load-failure
// hint branches, the Node-version gate, the shape check, and caching via
// the injectable `load` and `nodeMajor` seams — so they run everywhere
// regardless of whether a real `.node` is installed on the test machine
// OR which Node version the runner happens to be (the CI matrix spans
// 14–20, below and above the >=18 floor). Tests that exercise the load
// path inject a supported Node major so the version gate never short-
// circuits them; the gate's own tests inject the version under test.
const SUPPORTED_NODE_MAJOR = () => 18;

function stubBinding(overrides: Partial<Record<keyof SeaNativeBinding, unknown>> = {}): SeaNativeBinding {
  return {
    version: () => '1.2.3',
    openSession: async () => ({}),
    Connection: function Connection() {},
    Statement: function Statement() {},
    ...overrides,
  } as unknown as SeaNativeBinding;
}

function errWithCode(code: string, message: string): NodeJS.ErrnoException {
  const err = new Error(message) as NodeJS.ErrnoException;
  err.code = code;
  return err;
}

// Capture the message of the error thrown by `fn` (fails the test if
// nothing is thrown). Lets a single failure be asserted against several
// substrings without chai's `.and.to.throw` re-targeting quirk.
function thrownMessage(fn: () => unknown): string {
  try {
    fn();
  } catch (err) {
    return err instanceof Error ? err.message : String(err);
  }
  return expect.fail('expected the call to throw, but it did not') as never;
}

describe('SeaNativeLoader', () => {
  describe('successful load', () => {
    it('get() returns the binding from the injected loader', () => {
      const binding = stubBinding();
      const loader = new SeaNativeLoader(() => binding, SUPPORTED_NODE_MAJOR);
      expect(loader.get()).to.equal(binding);
      expect(loader.tryGet()).to.equal(binding);
    });

    it('caches the result — the load function runs at most once', () => {
      let calls = 0;
      const binding = stubBinding();
      const loader = new SeaNativeLoader(() => {
        calls += 1;
        return binding;
      }, SUPPORTED_NODE_MAJOR);
      loader.get();
      loader.tryGet();
      loader.get();
      expect(calls).to.equal(1);
    });
  });

  describe('load-failure hints', () => {
    it('MODULE_NOT_FOUND → "not installed" hint pointing at the README', () => {
      const loader = new SeaNativeLoader(() => {
        throw errWithCode('MODULE_NOT_FOUND', "Cannot find module '../../native/sea'");
      }, SUPPORTED_NODE_MAJOR);
      expect(loader.tryGet()).to.equal(undefined);
      const msg = thrownMessage(() => loader.get());
      expect(msg).to.match(/not installed/);
      expect(msg).to.match(/README/);
    });

    it('ERR_DLOPEN_FAILED → includes the underlying dlerror string and remediation', () => {
      const loader = new SeaNativeLoader(() => {
        throw errWithCode('ERR_DLOPEN_FAILED', 'GLIBC_2.32 not found');
      }, SUPPORTED_NODE_MAJOR);
      const msg = thrownMessage(() => loader.get());
      expect(msg).to.match(/GLIBC_2\.32 not found/);
      expect(msg).to.match(/musl/);
      expect(msg).to.match(/rm -rf node_modules/);
    });

    it('a generic Error (no code) preserves its message', () => {
      const loader = new SeaNativeLoader(() => {
        throw new Error('totally unexpected');
      }, SUPPORTED_NODE_MAJOR);
      expect(() => loader.get()).to.throw(/totally unexpected/);
    });

    it('a non-Error throw is wrapped', () => {
      const loader = new SeaNativeLoader(() => {
        // eslint-disable-next-line no-throw-literal
        throw 'a string';
      }, SUPPORTED_NODE_MAJOR);
      expect(() => loader.get()).to.throw(/non-standard error/);
    });
  });

  describe('shape check', () => {
    it('rejects a binding missing an expected export', () => {
      const loader = new SeaNativeLoader(() => stubBinding({ openSession: undefined }), SUPPORTED_NODE_MAJOR);
      expect(loader.tryGet()).to.equal(undefined);
      const msg = thrownMessage(() => loader.get());
      expect(msg).to.match(/missing expected export/);
      expect(msg).to.match(/openSession/);
    });
  });

  describe('Node-version gate', () => {
    it('fails closed on a Node version below the floor', () => {
      let loadCalled = false;
      const loader = new SeaNativeLoader(
        () => {
          loadCalled = true;
          return stubBinding();
        },
        () => 16,
      );
      expect(() => loader.get()).to.throw(/requires Node >=18/);
      expect(loadCalled, 'load() must not be attempted on an unsupported Node').to.equal(false);
    });

    it('fails closed when the Node version is unparseable (NaN)', () => {
      const loader = new SeaNativeLoader(
        () => stubBinding(),
        () => NaN,
      );
      expect(() => loader.get()).to.throw(/requires Node >=18/);
    });
  });
});
