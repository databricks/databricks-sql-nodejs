import Int64 from 'node-int64';
import { TSparkParameter, TSparkParameterValue } from '../thrift/TCLIService_types';

export type DBSQLParameterValue = undefined | null | boolean | number | bigint | Int64 | Date | string;

/**
 * Wire shape expected by the SEA napi codec
 * (`databricks-sql-kernel/napi/src/params.rs::TypedValueInput`). The Rust
 * side parses `value` per `sqlType`; we stringify the JS value the same
 * way `toSparkParameter` does for Thrift, then hand the pair to napi.
 *
 * Why a parallel converter rather than re-using `toSparkParameter` and
 * unwrapping the wire-Thrift `TSparkParameter`: napi consumes a flat
 * `{ sqlType, value: string | null }` POJO. Mining the value out of a
 * `TSparkParameter` (which wraps it in `TSparkParameterValue.stringValue`
 * and may use `name` for named-binding mode) requires more glue than
 * just emitting the SEA shape directly. The two emitters share the
 * same stringification rules — boolean → "TRUE"/"FALSE", Date →
 * `toISOString()`, etc.
 */
export interface TypedValueInput {
  /**
   * Canonical Databricks SQL type name (`"INT"`, `"STRING"`,
   * `"DECIMAL(10,2)"`, …). The napi codec is case-insensitive for the
   * simple variants and requires the parenthesised form for DECIMAL.
   */
  sqlType: string;
  /**
   * String-encoded literal, or `null` for SQL NULL. The Rust codec
   * short-circuits to `TypedValue::Null` regardless of `sqlType` when
   * this is `null` — matches the Thrift `TSparkParameter(name)` (no
   * type, no value) shape on the wire.
   */
  value: string | null;
}

export enum DBSQLParameterType {
  VOID = 'VOID', // aka NULL
  STRING = 'STRING',
  DATE = 'DATE',
  TIMESTAMP = 'TIMESTAMP',
  FLOAT = 'FLOAT',
  DECIMAL = 'DECIMAL',
  DOUBLE = 'DOUBLE',
  INTEGER = 'INTEGER',
  BIGINT = 'BIGINT',
  SMALLINT = 'SMALLINT',
  TINYINT = 'TINYINT',
  BOOLEAN = 'BOOLEAN',
  INTERVALMONTH = 'INTERVAL MONTH',
  INTERVALDAY = 'INTERVAL DAY',
}

interface DBSQLParameterOptions {
  type?: DBSQLParameterType;
  value: DBSQLParameterValue;
}

interface ToSparkParameterOptions {
  name?: string;
}

export class DBSQLParameter {
  public readonly type?: string;

  public readonly value: DBSQLParameterValue;

  constructor({ type, value }: DBSQLParameterOptions) {
    this.type = type;
    this.value = value;
  }

  public toSparkParameter({ name }: ToSparkParameterOptions = {}): TSparkParameter {
    // If VOID type was set explicitly - ignore value
    if (this.type === DBSQLParameterType.VOID) {
      return new TSparkParameter({ name }); // for NULL neither `type` nor `value` should be set
    }

    // Infer NULL values
    if (this.value === undefined || this.value === null) {
      return new TSparkParameter({ name }); // for NULL neither `type` nor `value` should be set
    }

    if (typeof this.value === 'boolean') {
      return new TSparkParameter({
        name,
        type: this.type ?? DBSQLParameterType.BOOLEAN,
        value: new TSparkParameterValue({
          stringValue: this.value ? 'TRUE' : 'FALSE',
        }),
      });
    }

    if (typeof this.value === 'number') {
      return new TSparkParameter({
        name,
        type: this.type ?? (Number.isInteger(this.value) ? DBSQLParameterType.INTEGER : DBSQLParameterType.DOUBLE),
        value: new TSparkParameterValue({
          stringValue: Number(this.value).toString(),
        }),
      });
    }

    if (this.value instanceof Int64 || typeof this.value === 'bigint') {
      return new TSparkParameter({
        name,
        type: this.type ?? DBSQLParameterType.BIGINT,
        value: new TSparkParameterValue({
          stringValue: this.value.toString(),
        }),
      });
    }

    if (this.value instanceof Date) {
      return new TSparkParameter({
        name,
        type: this.type ?? DBSQLParameterType.TIMESTAMP,
        value: new TSparkParameterValue({
          stringValue: this.value.toISOString(),
        }),
      });
    }

    return new TSparkParameter({
      name,
      type: this.type ?? DBSQLParameterType.STRING,
      value: new TSparkParameterValue({
        stringValue: this.value,
      }),
    });
  }

  /**
   * SEA-backend sibling of `toSparkParameter`. Emits the flat
   * `{ sqlType, value }` shape the napi codec consumes.
   *
   * Stringification rules are kept in lock-step with `toSparkParameter`
   * so a positional parameter bound on the Thrift backend and on the
   * SEA backend hits the server with the same wire-level type-name +
   * value string. Divergence between the two emitters here would
   * silently re-introduce the kind of cross-backend behavior split the
   * driver-test parity suite is built to catch.
   *
   * @returns null for SQL NULL (caller is expected to emit it as
   *   `{ sqlType: "VOID", value: null }` or to skip the entry,
   *   depending on call-site convention). This method itself never
   *   throws; unsupported value shapes fall through the STRING default
   *   to match the Thrift emitter's behaviour.
   */
  public toNapiTypedValue(): TypedValueInput {
    // VOID literal — explicit NULL, the napi codec short-circuits.
    if (this.type === DBSQLParameterType.VOID) {
      return { sqlType: 'VOID', value: null };
    }

    // Inferred NULL — same shape as VOID. The napi codec's contract is
    // that `value: null` produces `TypedValue::Null` regardless of
    // sqlType, so any non-empty sqlType would work here; we emit VOID
    // for consistency with the explicit-NULL path above.
    if (this.value === undefined || this.value === null) {
      return { sqlType: 'VOID', value: null };
    }

    if (typeof this.value === 'boolean') {
      return {
        sqlType: this.type ?? DBSQLParameterType.BOOLEAN,
        // Thrift emits "TRUE" / "FALSE"; the napi `parse_bool` accepts
        // both "true"/"false" and "TRUE"/"FALSE" via its case-insensitive
        // match. Keep the casing aligned with the Thrift emitter so any
        // log scrape that diffs the two wires sees identical strings.
        value: this.value ? 'TRUE' : 'FALSE',
      };
    }

    if (typeof this.value === 'number') {
      return {
        sqlType: this.type ?? (Number.isInteger(this.value) ? DBSQLParameterType.INTEGER : DBSQLParameterType.DOUBLE),
        value: Number(this.value).toString(),
      };
    }

    if (this.value instanceof Int64 || typeof this.value === 'bigint') {
      return {
        sqlType: this.type ?? DBSQLParameterType.BIGINT,
        value: this.value.toString(),
      };
    }

    if (this.value instanceof Date) {
      return {
        sqlType: this.type ?? DBSQLParameterType.TIMESTAMP,
        value: this.value.toISOString(),
      };
    }

    return {
      sqlType: this.type ?? DBSQLParameterType.STRING,
      value: this.value,
    };
  }
}

/**
 * Convert the user-facing `ordinalParameters` array into the flat
 * `TypedValueInput[]` shape the SEA napi codec accepts.
 *
 * Mirrors the ordinal-only branch of `lib/DBSQLSession.ts::getQueryParameters`
 * — entries may be a `DBSQLParameter` instance or a bare value, and a
 * bare value is wrapped in a `DBSQLParameter` for emission. The wrapping
 * path is the load-bearing one (today the Node driver's public surface
 * accepts bare JS values for positional binding); this helper is the
 * single source of truth for how those bare values stringify against
 * the napi codec.
 *
 * Returns an empty array for an undefined / empty input. The caller is
 * expected to fall back to a no-positional-params execute in that case.
 */
export function convertOrdinalParametersToTypedValueInputs(
  ordinalParameters?: Array<DBSQLParameter | DBSQLParameterValue>,
): Array<TypedValueInput> {
  if (ordinalParameters === undefined || ordinalParameters.length === 0) {
    return [];
  }
  return ordinalParameters.map((value) => {
    const param = value instanceof DBSQLParameter ? value : new DBSQLParameter({ value });
    return param.toNapiTypedValue();
  });
}
