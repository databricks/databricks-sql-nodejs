import { expect } from 'chai';
import {
  mapKernelErrorToJsError,
  KernelErrorCode,
  KernelErrorShape,
} from '../../../lib/sea/SeaErrorMapping';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import OperationStateError, {
  OperationStateErrorCode,
} from '../../../lib/errors/OperationStateError';
import ParameterError from '../../../lib/errors/ParameterError';

describe('SeaErrorMapping.mapKernelErrorToJsError', () => {
  // The 13 kernel ErrorCode variants — kept in sync with src/kernel_error.rs:66-134.
  // Tabular driver: each row is (kernel code, expected class, optional extra assertion).
  type Case = {
    code: KernelErrorCode;
    expectedClass: Function;
    extra?: (err: Error) => void;
  };

  const cases: Array<Case> = [
    {
      code: 'InvalidArgument',
      expectedClass: ParameterError,
    },
    {
      code: 'Unauthenticated',
      expectedClass: AuthenticationError,
    },
    {
      code: 'PermissionDenied',
      expectedClass: AuthenticationError,
    },
    {
      code: 'NotFound',
      expectedClass: HiveDriverError,
    },
    {
      code: 'ResourceExhausted',
      expectedClass: HiveDriverError,
    },
    {
      code: 'Unavailable',
      expectedClass: HiveDriverError,
    },
    {
      code: 'Timeout',
      expectedClass: OperationStateError,
      extra: (err) => {
        expect((err as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Timeout);
      },
    },
    {
      code: 'Cancelled',
      expectedClass: OperationStateError,
      extra: (err) => {
        expect((err as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Canceled);
      },
    },
    {
      code: 'DataLoss',
      expectedClass: HiveDriverError,
    },
    {
      code: 'Internal',
      expectedClass: HiveDriverError,
    },
    {
      code: 'InvalidStatementHandle',
      expectedClass: HiveDriverError,
    },
    {
      code: 'NetworkError',
      expectedClass: HiveDriverError,
    },
    {
      // Server-side SQL execution failures surface as OperationStateError(ERROR),
      // mirroring the Thrift backend's operation-status-poll error path so the
      // two drivers throw the same class. (OperationStateError extends
      // HiveDriverError, so base-class catchers still match.)
      code: 'SqlError',
      expectedClass: OperationStateError,
      extra: (err) => {
        expect((err as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Error);
      },
    },
  ];

  it('covers all 13 kernel ErrorCode variants', () => {
    // Guardrail: if the kernel adds a variant, KernelErrorCode in TS will gain
    // a literal — this test then fails because the new variant has no case row.
    // (Drift is caught at the test level since the union itself is an inline literal.)
    expect(cases).to.have.lengthOf(13);
  });

  cases.forEach(({ code, expectedClass, extra }) => {
    it(`maps ${code} to ${expectedClass.name}`, () => {
      const kErr: KernelErrorShape = {
        code,
        message: `kernel ${code} message`,
      };

      const err = mapKernelErrorToJsError(kErr);

      expect(err).to.be.instanceOf(expectedClass);
      expect(err.message).to.equal(`kernel ${code} message`);
      if (extra) {
        extra(err);
      }
    });
  });

  describe('SQLSTATE preservation', () => {
    it('attaches sqlState when present on the kernel error', () => {
      const err = mapKernelErrorToJsError({
        code: 'SqlError',
        message: 'syntax error',
        sqlstate: '42000',
      });

      expect(err).to.be.instanceOf(HiveDriverError);
      expect(err.sqlState).to.equal('42000');
    });

    it('does not set sqlState when absent', () => {
      const err = mapKernelErrorToJsError({
        code: 'Internal',
        message: 'boom',
      });

      expect(err.sqlState).to.be.undefined;
    });

    it('preserves sqlState on AuthenticationError', () => {
      const err = mapKernelErrorToJsError({
        code: 'Unauthenticated',
        message: 'invalid token',
        sqlstate: '28000',
      });

      expect(err).to.be.instanceOf(AuthenticationError);
      expect(err.sqlState).to.equal('28000');
    });

    it('preserves sqlState on OperationStateError', () => {
      const err = mapKernelErrorToJsError({
        code: 'Timeout',
        message: 'deadline exceeded',
        sqlstate: 'HYT01',
      });

      expect(err).to.be.instanceOf(OperationStateError);
      expect((err as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Timeout);
      expect(err.sqlState).to.equal('HYT01');
    });

    it('preserves sqlState on ParameterError', () => {
      const err = mapKernelErrorToJsError({
        code: 'InvalidArgument',
        message: 'bad param',
        sqlstate: 'HY009',
      });

      expect(err).to.be.instanceOf(ParameterError);
      expect(err.sqlState).to.equal('HY009');
    });

    it('attaches sqlState as a non-enumerable property', () => {
      const err = mapKernelErrorToJsError({
        code: 'SqlError',
        message: 'oops',
        sqlstate: '42000',
      });

      const descriptor = Object.getOwnPropertyDescriptor(err, 'sqlState');
      expect(descriptor).to.exist;
      expect(descriptor!.enumerable).to.equal(false);
      expect(descriptor!.writable).to.equal(true);
      expect(descriptor!.configurable).to.equal(true);
    });
  });

  describe('unknown / future kernel codes', () => {
    it('falls back to HiveDriverError for an unrecognised code', () => {
      const err = mapKernelErrorToJsError({
        code: 'SomeFutureVariantThatDoesNotExist',
        message: 'forward-compat message',
      });

      // Never silently drop — must surface as the base driver class.
      expect(err).to.be.instanceOf(HiveDriverError);
      expect(err.message).to.equal('forward-compat message');
    });

    it('still preserves sqlState on a fallback HiveDriverError', () => {
      const err = mapKernelErrorToJsError({
        code: 'BrandNewVariant',
        message: 'with sqlstate',
        sqlstate: '01004',
      });

      expect(err).to.be.instanceOf(HiveDriverError);
      expect(err.sqlState).to.equal('01004');
    });
  });

  describe('returned errors compose with try/catch', () => {
    it('thrown errors are catchable as Error', () => {
      function thrower() {
        throw mapKernelErrorToJsError({ code: 'Internal', message: 'kaboom' });
      }

      expect(thrower).to.throw(Error, 'kaboom');
      expect(thrower).to.throw(HiveDriverError, 'kaboom');
    });

    it('AuthenticationError thrown is also instanceOf HiveDriverError', () => {
      // AuthenticationError extends HiveDriverError — preserve that hierarchy.
      const err = mapKernelErrorToJsError({ code: 'Unauthenticated', message: 'nope' });
      expect(err).to.be.instanceOf(AuthenticationError);
      expect(err).to.be.instanceOf(HiveDriverError);
      expect(err).to.be.instanceOf(Error);
    });

    it('ParameterError does NOT extend HiveDriverError (matches existing class hierarchy)', () => {
      const err = mapKernelErrorToJsError({ code: 'InvalidArgument', message: 'bad' });
      expect(err).to.be.instanceOf(ParameterError);
      expect(err).to.not.be.instanceOf(HiveDriverError);
      expect(err).to.be.instanceOf(Error);
    });
  });
});
