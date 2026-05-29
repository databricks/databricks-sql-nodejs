import { TSessionHandle, TProtocolVersion } from '../../../thrift/TCLIService_types';
import DBSQLSession from '../../../lib/DBSQLSession';
import ThriftSessionBackend from '../../../lib/thrift-backend/ThriftSessionBackend';
import IClientContext from '../../../lib/contracts/IClientContext';

interface CreateSessionForTestArgs {
  handle: TSessionHandle;
  context: IClientContext;
  serverProtocolVersion?: TProtocolVersion;
}

/**
 * Test helper that mirrors the pre-PR-378 `new DBSQLSession({ handle, ... })`
 * legacy ctor shape, but routes through the post-PR-378 `{ backend, ... }`
 * shape by constructing a `ThriftSessionBackend` explicitly. Keeps the
 * facade decoupled from concrete backend imports.
 */
export function createSessionForTest({
  handle,
  context,
  serverProtocolVersion,
}: CreateSessionForTestArgs): DBSQLSession {
  const backend = new ThriftSessionBackend({ handle, context, serverProtocolVersion });
  return new DBSQLSession({ backend, context });
}
