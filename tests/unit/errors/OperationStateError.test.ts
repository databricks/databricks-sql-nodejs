import { expect } from 'chai';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
import { mapKernelErrorToJsError } from '../../../lib/kernel/KernelErrorMapping';
import { TOperationState, TStatusCode } from '../../../thrift/TCLIService_types';

describe('OperationStateError', () => {
  it('should expose response.sqlState at top level', () => {
    const error = new OperationStateError(OperationStateErrorCode.Error, {
      status: { statusCode: TStatusCode.SUCCESS_STATUS, sqlState: '00000' },
      operationState: TOperationState.ERROR_STATE,
      sqlState: '22012',
      displayMessage: 'Division by zero',
    }) as OperationStateError & { sqlState?: string };

    expect(error.errorCode).to.be.eq(OperationStateErrorCode.Error);
    expect(error.message).to.be.eq('Division by zero');
    expect(error.sqlState).to.be.eq('22012');
  });

  it('should fall back to response.status.sqlState', () => {
    const error = new OperationStateError(OperationStateErrorCode.Error, {
      status: { statusCode: TStatusCode.ERROR_STATUS, sqlState: '42P01' },
      operationState: TOperationState.ERROR_STATE,
    }) as OperationStateError & { sqlState?: string };

    expect(error.sqlState).to.be.eq('42P01');
  });

  it('should attach sqlState with the same descriptor as the kernel backend', () => {
    const thriftError = new OperationStateError(OperationStateErrorCode.Error, {
      status: { statusCode: TStatusCode.ERROR_STATUS },
      operationState: TOperationState.ERROR_STATE,
      sqlState: '22012',
    });
    const kernelError = mapKernelErrorToJsError({ code: 'SqlError', message: 'x', sqlstate: '22012' });

    expect(kernelError).to.be.instanceOf(OperationStateError);
    expect(Object.getOwnPropertyDescriptor(thriftError, 'sqlState')).to.deep.eq(
      Object.getOwnPropertyDescriptor(kernelError, 'sqlState'),
    );
    expect(Object.keys(thriftError)).to.not.include('sqlState');
    expect(JSON.parse(JSON.stringify(thriftError))).to.not.have.property('sqlState');
  });

  it('should not define sqlState when the server reports none', () => {
    const error = new OperationStateError(OperationStateErrorCode.Error, {
      status: { statusCode: TStatusCode.ERROR_STATUS },
      operationState: TOperationState.ERROR_STATE,
    });

    expect(Object.prototype.hasOwnProperty.call(error, 'sqlState')).to.be.false;
  });
});
