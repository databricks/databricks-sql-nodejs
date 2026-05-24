import { TTableSchema } from '../../thrift/TCLIService_types';

/**
 * Backend-neutral result-format taxonomy. Mirrors the three on-wire shapes
 * `ThriftOperationBackend` actually dispatches on (`COLUMN_BASED_SET`,
 * `ARROW_BASED_SET`, `URL_BASED_SET`); a SEA implementer surfaces the same
 * three so result-handling stays format-agnostic.
 */
export enum ResultFormat {
  ColumnBased = 'COLUMN_BASED',
  ArrowBased = 'ARROW_BASED',
  UrlBased = 'URL_BASED',
}

/**
 * Neutral result-set metadata returned by `IOperationBackend.getResultMetadata()`.
 *
 * `schema` keeps the Thrift `TTableSchema` shape for now because the public
 * `DBSQLOperation.getSchema()` and `getMetadata()` already expose it on
 * `IOperation`; carrying it across the boundary preserves back-compat. The
 * SEA backend will adapt its column descriptors into the same shape until
 * the public IOperation surface is migrated in a later PR.
 */
export interface ResultMetadata {
  /** Column schema; null if the operation has no result set. */
  schema?: TTableSchema;

  /** Wire format the result handler should dispatch on. */
  resultFormat: ResultFormat;

  /** Whether the result payload is LZ4-compressed. */
  lz4Compressed?: boolean;

  /** Optional Arrow IPC schema bytes (for ARROW_BASED / URL_BASED formats). */
  arrowSchema?: Buffer;

  /** True iff the operation is a staging (PUT/GET/REMOVE) operation. */
  isStagingOperation: boolean;
}
