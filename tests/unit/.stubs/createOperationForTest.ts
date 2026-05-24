import { TOperationHandle, TSparkDirectResults } from '../../../thrift/TCLIService_types';
import DBSQLOperation from '../../../lib/DBSQLOperation';
import ThriftOperationBackend from '../../../lib/thrift-backend/ThriftOperationBackend';
import IClientContext from '../../../lib/contracts/IClientContext';

interface CreateOperationForTestArgs {
  handle: TOperationHandle;
  directResults?: TSparkDirectResults;
  context: IClientContext;
}

/**
 * Test helper that mirrors the pre-PR-378 `new DBSQLOperation({ handle, ... })`
 * legacy ctor shape, but routes through the post-PR-378 `{ backend, ... }`
 * shape by constructing a `ThriftOperationBackend` explicitly. Keeps the
 * facade decoupled from concrete backend imports.
 */
export function createOperationForTest({
  handle,
  directResults,
  context,
}: CreateOperationForTestArgs): DBSQLOperation {
  const backend = new ThriftOperationBackend({ handle, directResults, context });
  return new DBSQLOperation({ backend, context });
}
