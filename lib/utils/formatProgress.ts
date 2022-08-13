import { TProgressUpdateResp } from '../../thrift/TCLIService_types';

export class ProgressUpdateTransformer {
  private progressUpdate: TProgressUpdateResp;
  private rowWidth: number = 10;

  constructor(progressUpdate: TProgressUpdateResp) {
    this.progressUpdate = progressUpdate;
  }

  formatRow(row: Array<string>): string {
    return row.map((cell) => cell.padEnd(this.rowWidth, ' ')).join('|');
  }

  toString() {
    const header = this.formatRow(this.progressUpdate.headerNames);
    const footer = this.progressUpdate.footerSummary;
    const rows = this.progressUpdate.rows.map((row: Array<string>) => {
      return this.formatRow(row);
    });

    return [header, ...rows, footer].join('\n');
  }
}

export default function formatProgress(progressUpdate: TProgressUpdateResp): string {
  return String(new ProgressUpdateTransformer(progressUpdate));
}
