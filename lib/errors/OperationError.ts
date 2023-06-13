import DBSQLError, { DBSQLErrorOptions } from './DBSQLError';

export interface OperationErrorOptions extends DBSQLErrorOptions {}

export default class OperationError extends DBSQLError {
  constructor(message: string, options: OperationErrorOptions = {}) {
    super(message, options);
  }
}
