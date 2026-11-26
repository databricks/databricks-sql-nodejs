import { RecordBatchReader } from 'apache-arrow';
import { TGetResultSetMetadataResp, TTableSchema, TTypeId } from '../../thrift/TCLIService_types';

function isPlaceholderSchema(schema?: TTableSchema): boolean {
  if (schema?.columns?.length !== 1) {
    return false;
  }
  const [column] = schema.columns;
  return column?.columnName === 'Result' && column.typeDesc?.types?.[0]?.primitiveEntry?.type === TTypeId.STRING_TYPE;
}

function hasNoArrowFields(arrowSchema: Buffer): boolean {
  try {
    const reader = RecordBatchReader.from(arrowSchema);
    reader.open();
    return reader.schema?.fields.length === 0;
  } catch {
    return false;
  }
}

/**
 * Column schema of a Thrift result. For statements without output (e.g. DDL)
 * the server's `TTableSchema` is a placeholder `Result STRING` column, while the
 * Arrow schema in the same response has no fields; report no columns, as the
 * kernel backend does. Without Arrow metadata the placeholder can't be told apart
 * from a real `Result` column and is returned as is.
 */
export default function getResultSchema(metadata: TGetResultSetMetadataResp): TTableSchema | undefined {
  const { schema, arrowSchema } = metadata;
  if (arrowSchema && isPlaceholderSchema(schema) && hasNoArrowFields(arrowSchema)) {
    return { columns: [] };
  }
  return schema;
}
