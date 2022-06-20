import fs from "fs";
import path from "path";

function getVersion(): string {
  const json = JSON.parse(fs.readFileSync(path.join(__dirname, "../package.json")).toString());
  return json.version;
}

export default getVersion();
