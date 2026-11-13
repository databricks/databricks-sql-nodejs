import { expect } from 'chai';
import {
  mapKernelErrorToJsError,
  decodeNapiKernelError,
  KernelErrorCode,
  KernelErrorShape,
  ErrorWithSqlState,
} from '../../../lib/kernel/KernelErrorMapping';
import HiveDriverError from '../../../lib/errors/HiveDriverError';
import AuthenticationError from '../../../lib/errors/AuthenticationError';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import ParameterError from '../../../lib/errors/ParameterError';

describe('KernelErrorMapping.mapKernelErrorToJsError', () => {
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
      code: 'SqlError',
      expectedClass: HiveDriverError,
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

describe('KernelErrorMapping.decodeNapiKernelError — diagnostics enrichment (Thrift/Python parity)', () => {
  // The wire sentinel the napi binding prefixes onto structured kernel errors
  // (mirrors `__databricks_error__:` in KernelErrorMapping.ts / native/kernel/src/error.rs).
  const SENTINEL = '__databricks_error__:';
  const envelope = (fields: Record<string, unknown>): Error => new Error(SENTINEL + JSON.stringify(fields));

  it('surfaces displayMessage / diagnosticInfo / errorDetailsJson in kernelMetadata (were dropped before)', () => {
    const err = decodeNapiKernelError(
      envelope({
        code: 'SqlError',
        message: '[TABLE_OR_VIEW_NOT_FOUND] table not found',
        sqlState: '42P01',
        errorCode: 'TABLE_OR_VIEW_NOT_FOUND',
        vendorCode: 0,
        displayMessage: 'TABLE_OR_VIEW_NOT_FOUND: `main`.`x` cannot be found',
        diagnosticInfo: 'org.apache.spark.sql.AnalysisException: ...',
        errorDetailsJson: '{"errorClass":"TABLE_OR_VIEW_NOT_FOUND"}',
      }),
    ) as ErrorWithSqlState;

    // Class parity with Thrift: a server SQL failure → OperationStateError(Error).
    expect(err).to.be.instanceOf(OperationStateError);
    expect((err as OperationStateError).errorCode).to.equal(OperationStateErrorCode.Error);

    // The three previously-dropped diagnostics are now surfaced.
    expect(err.kernelMetadata?.displayMessage).to.equal('TABLE_OR_VIEW_NOT_FOUND: `main`.`x` cannot be found');
    expect(err.kernelMetadata?.diagnosticInfo).to.match(/AnalysisException/);
    expect(err.kernelMetadata?.errorDetailsJson).to.equal('{"errorClass":"TABLE_OR_VIEW_NOT_FOUND"}');
  });

  it('keeps the existing envelope fields alongside the new diagnostics', () => {
    const err = decodeNapiKernelError(
      envelope({
        code: 'SqlError',
        message: 'boom',
        sqlState: '42000',
        errorCode: 'PARSE_SYNTAX_ERROR',
        vendorCode: 1234,
        httpStatus: 400,
        retryable: false,
        queryId: '01ef-abcd',
        displayMessage: 'Syntax error',
      }),
    ) as ErrorWithSqlState;
    expect(err.sqlState).to.equal('42000');
    expect(err.kernelMetadata?.errorCode).to.equal('PARSE_SYNTAX_ERROR');
    expect(err.kernelMetadata?.vendorCode).to.equal(1234);
    expect(err.kernelMetadata?.queryId).to.equal('01ef-abcd');
    expect(err.kernelMetadata?.displayMessage).to.equal('Syntax error');
  });

  it('attaches kernelMetadata even when ONLY a diagnostic field is present', () => {
    // Previously the attach-guard enumerated only errorCode/vendorCode/httpStatus/
    // retryable/queryId, so a diagnostics-only envelope dropped them silently.
    const err = decodeNapiKernelError(
      envelope({ code: 'Internal', message: 'kaboom', diagnosticInfo: 'stack trace here' }),
    ) as ErrorWithSqlState;
    expect(err.kernelMetadata?.diagnosticInfo).to.equal('stack trace here');
  });

  it('omits kernelMetadata entirely for a bare envelope (no optional fields)', () => {
    const err = decodeNapiKernelError(envelope({ code: 'Internal', message: 'plain' })) as ErrorWithSqlState;
    expect(err.kernelMetadata).to.equal(undefined);
  });

  it('Thrift parity: kernel exposes the same diagnostics Thrift carries via OperationStateError.response', () => {
    // Thrift's OperationStateError carries the full TGetOperationStatusResp on
    // `.response` (displayMessage + sqlState + numeric errorCode + diagnostics).
    // kernel must expose the equivalent set so a consumer gets parity regardless of
    // backend: same class + sqlState + a diagnostics surface.
    const err = decodeNapiKernelError(
      envelope({
        code: 'SqlError',
        message: 'div by zero',
        sqlState: '22012',
        vendorCode: 0,
        displayMessage: 'Division by zero',
        diagnosticInfo: 'ArithmeticException',
      }),
    ) as ErrorWithSqlState;
    expect(err).to.be.instanceOf(OperationStateError); // class parity (Thrift: OperationStateError)
    expect(err.sqlState).to.equal('22012'); // Thrift: response.sqlState
    expect(err.kernelMetadata?.displayMessage).to.equal('Division by zero'); // Thrift: response.displayMessage
    expect(err.kernelMetadata?.diagnosticInfo).to.equal('ArithmeticException'); // Thrift: response.errorMessage
  });
});

describe('KernelErrorMapping.decodeNapiKernelError — cause chain', () => {
  const SENTINEL = '__databricks_error__:';

  const cases: Array<[string, Function]> = [
    ['Unauthenticated', AuthenticationError],
    ['PermissionDenied', AuthenticationError],
    ['SqlError', OperationStateError],
    ['Cancelled', OperationStateError],
    ['InvalidArgument', ParameterError],
    ['NetworkError', HiveDriverError],
    ['SomeFutureVariant', HiveDriverError],
  ];

  cases.forEach(([code, expectedClass]) => {
    it(`attaches a non-enumerable code and message cause for ${code}`, () => {
      const json = JSON.stringify({ code, message: 'HTTP 403: Invalid access token', httpStatus: 403 });
      const napiError = new Error(SENTINEL + json);

      const err = decodeNapiKernelError(napiError) as Error & { cause?: unknown };

      expect(err).to.be.instanceOf(expectedClass);
      expect(err.message).to.equal('HTTP 403: Invalid access token');
      expect(err.cause).to.be.instanceOf(Error);
      expect(err.cause).to.not.equal(napiError);
      expect((err.cause as Error).message).to.equal(`${code}: HTTP 403: Invalid access token`);
      expect(napiError.message).to.equal(SENTINEL + json);
      const descriptor = Object.getOwnPropertyDescriptor(err, 'cause');
      expect(descriptor?.enumerable).to.equal(false);
      expect(descriptor?.writable).to.equal(true);
    });
  });

  it('keeps the envelope diagnostics out of the cause and bounds its length', () => {
    const json = JSON.stringify({
      code: 'SqlError',
      message: `boom ${'m'.repeat(2000)}`,
      diagnosticInfo: 'secret-diagnostic-info',
      errorDetailsJson: `{"details":"${'d'.repeat(10000)}"}`,
    });

    const err = decodeNapiKernelError(new Error(SENTINEL + json)) as ErrorWithSqlState & { cause?: Error };

    expect(err.kernelMetadata?.diagnosticInfo).to.equal('secret-diagnostic-info');
    const causeMessage = String(err.cause?.message);
    expect(causeMessage.startsWith('SqlError: boom mmm')).to.equal(true);
    expect(causeMessage.length).to.equal(500);
    expect(causeMessage.endsWith('…')).to.equal(true);
    expect(causeMessage).to.not.contain('secret-diagnostic-info');
    expect(causeMessage).to.not.contain('details');
    expect(causeMessage).to.not.contain(SENTINEL);
  });

  it('adds no cause to binding-side errors that are returned as-is', () => {
    const plain = new Error('openSession: `token` is required');
    const err = decodeNapiKernelError(plain) as Error & { cause?: unknown };
    expect(err).to.equal(plain);
    expect(err.cause).to.equal(undefined);
  });
});

it('preserves local file error code and path from native completion', () => {
  const error = decodeNapiKernelError(
    new Error(
      `__databricks_error__:${JSON.stringify({
        code: 'NotFound',
        message: 'Local file does not exist',
        localOsError: 2,
        localErrorKind: 'NotFound',
        localPath: '/allowed/missing',
      })}`,
    ),
  ) as NodeJS.ErrnoException;
  expect(error.code).to.equal('ENOENT');
  expect(error.path).to.equal('/allowed/missing');
  expect(error.errno).to.be.lessThan(0);
});
