import HiveDriverError from './HiveDriverError';

export default class AuthenticationError extends HiveDriverError {
  constructor(message?: string) {
    super(message);
    this.name = 'AuthenticationError';
  }
}
