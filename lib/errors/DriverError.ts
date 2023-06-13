import DBSQLError, { DBSQLErrorOptions } from './DBSQLError';

export interface DriverErrorOptions extends DBSQLErrorOptions {}

export default class DriverError extends DBSQLError {
  constructor(message: string, options: DriverErrorOptions = {}) {
    super(message, options);
  }
}
