import DBSQLError, { DBSQLErrorOptions } from './DBSQLError';

export enum ClientErrorCode {
  ConnectionLost = 'CONNECTION_LOST',
  ClientNotInitialized = 'CLIENT_NOT_INITIALIZED',
}

const errorMessages: Record<ClientErrorCode, string> = {
  [ClientErrorCode.ConnectionLost]: 'Connection is lost',
  [ClientErrorCode.ClientNotInitialized]: 'Client is not initialized',
};

export interface ClientErrorOptions extends DBSQLErrorOptions {}

export default class ClientError extends DBSQLError {
  public readonly errorCode: ClientErrorCode;

  constructor(errorCode: ClientErrorCode, options: ClientErrorOptions = {}) {
    super(errorMessages[errorCode], options);
    this.errorCode = errorCode;
  }
}
