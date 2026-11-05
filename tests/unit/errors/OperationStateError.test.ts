import { expect } from 'chai';
import OperationStateError, { OperationStateErrorCode } from '../../../lib/errors/OperationStateError';
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
});
