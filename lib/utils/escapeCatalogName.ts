/**
 * Match JDBC by preserving immediate `\_` pairs, even after another backslash.
 */
export default function escapeCatalogName(catalogName: string | undefined): string | undefined {
  if (catalogName == null) {
    return catalogName;
  }
  return catalogName.replace(/\\?_/g, '\\_');
}
