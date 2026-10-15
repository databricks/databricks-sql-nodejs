/** Escape catalog underscores without double-escaping existing `\_` sequences. */
export default function escapeCatalogName(catalogName: string | undefined): string | undefined {
  if (catalogName == null) {
    return catalogName;
  }
  return catalogName.replace(/\\?_/g, '\\_');
}
