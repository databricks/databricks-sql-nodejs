interface MetadataError {
  sqlState?: string | null;
  errorMessage?: string | null;
  displayMessage?: string | null;
}

export default function isObjectNotFound(error: MetadataError): boolean {
  // Match Databricks JDBC's metadata not-found classification.
  return (
    error.sqlState === '42704' ||
    [error.errorMessage, error.displayMessage].some((message) =>
      ['NO_SUCH_CATALOG_EXCEPTION', 'SCHEMA_NOT_FOUND', 'TABLE_OR_VIEW_NOT_FOUND', 'INVALID_PARAMETER_VALUE'].some(
        (errorClass) => message?.includes(errorClass),
      ),
    )
  );
}
