import fs from 'fs';
import path from 'path';

const createTableSql = fs.readFileSync(path.join(__dirname, 'create_table.sql')).toString();
const insertDataSql = fs.readFileSync(path.join(__dirname, 'insert_data.sql')).toString();

export { createTableSql, insertDataSql };

type AllTypesRowShape = {
  flt: number;
  [key: string]: unknown;
};

export function fixArrowResult(rows: Array<AllTypesRowShape>) {
  return rows.map((row) => ({
    ...row,
    // This field is 32-bit floating point value, and since Arrow encodes it accurately,
    // it will have only approx. 7 significant decimal places. But in JS all floating
    // point numbers are double-precision, so after decoding we get, for example,
    // 1.414199948310852 instead of 1.4142. Therefore, we need to "fix" value
    // returned by Arrow, so it can be tested against our fixture
    flt: Number(row.flt.toFixed(4)),
  }));
}
