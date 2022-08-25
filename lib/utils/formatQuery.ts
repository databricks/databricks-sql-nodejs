const formatUnicorn = require('format-unicorn/safe');

function escapeNumber(param: Number): string {
  return param.toString();
}

function escapeString(param: string): string {
  return `'${param.replace("'", "''")}'`;
}

function escapeSequence(param: Array<any>): string {
  const escapedArray: string[] = [];
  param.forEach((element) => {
    escapedArray.push(escapeItem(element));
  });
  return `(${escapedArray.join(',')})`;
}

function zeroPad(number: Number): string {
  return number.toString().padStart(2);
}

function escapeDate(param: Date): string {
  return `${param.getFullYear()}-${zeroPad(param.getMonth() + 1)}-${zeroPad(param.getDate())} ${zeroPad(
    param.getHours(),
  )}:${zeroPad(param.getMinutes())}:${zeroPad(param.getSeconds())}`;
}

function escapeItem(param: any): string {
  switch (typeof param) {
    case 'string':
      return escapeString(param);
    case 'number':
      return escapeNumber(param);
    case 'object':
      if (param.isArray()) {
        return escapeSequence(param);
      }
      if (param instanceof Date) return escapeDate(param);
      break;
    default:
      break;
  }
  throw new Error('Unexpected parameter');
}

export default function formatQuery(statement: string, queryParams: { [name: string]: any }): string {
  const sanitizedParams: { [name: string]: string } = {};
  for (const [key, value] of Object.entries(queryParams)) {
    sanitizedParams[key] = escapeItem(value);
  }
  return formatUnicorn(statement, sanitizedParams);
}
