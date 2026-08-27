import HiveDriverError from './HiveDriverError';

/** Protocol-neutral details for constructing a driver StatusError. */
export interface StatusErrorOptions {
  message?: string;
  code?: number;
  sqlState?: string;
  infoMessages?: ReadonlyArray<string>;
}

/**
 * Legacy shape accepted by the original constructor. Keeping this structural
 * type means existing Thrift call sites remain source-compatible without making
 * StatusError itself depend on generated Thrift types.
 */
interface LegacyStatusErrorOptions {
  statusCode?: number;
  errorMessage?: string;
  errorCode?: number;
  sqlState?: string;
  infoMessages?: ReadonlyArray<string>;
}

export default class StatusError extends HiveDriverError {
  public name = 'Status Error';

  public code: number;

  public sqlState?: string;

  constructor(options: StatusErrorOptions | LegacyStatusErrorOptions) {
    const { message, code } = options as StatusErrorOptions;
    const { errorMessage, errorCode } = options as LegacyStatusErrorOptions;
    const normalizedMessage = message ?? errorMessage ?? '';
    super(normalizedMessage);

    this.code = code ?? errorCode ?? -1;
    this.sqlState = options.sqlState;

    if (Array.isArray(options.infoMessages)) {
      this.stack = options.infoMessages.join('\n');
    }
  }
}
