// file: benchmark_all.js
const fs = require('fs');
const path = require('path');
// const { DBSQLClient } = require('@databricks/sql');
const { performance } = require('perf_hooks');
const DBSQLClient = require('./dist/DBSQLClient').default;


// Load connection config from environment variables (or modify to use a config file)
const config = {
  host: 'benchmarking-prod-aws-us-west-2.cloud.databricks.com',
//   path: '/sql/1.0/warehouses/7e635336d748166a',
path: 'sql/protocolv1/o/5870029948831567/0401-031703-hcqr7y4g',
  token: 'xx'
};
if (!config.host || !config.path || !config.token) {
  console.error("ERROR: Please set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN environment variables.");
  process.exit(1);
}
const ITERATIONS = 10;
// Folder that contains your SQL files
const benchmarkingDir = path.join(__dirname, "benchmarking");

// Define the four modes for running benchmarks
const modes = [
  {
    modeName: "CF_on_LZ4_on",
    clientConfig: { host: config.host, path: config.path, token: config.token, useCloudFetch: true, useLZ4Compression: true }
  },
  {
    modeName: "CF_on_LZ4_off",
    clientConfig: { host: config.host, path: config.path, token: config.token, useCloudFetch: true, useLZ4Compression: false }
  },
  {
    modeName: "CF_off_LZ4_on",
    clientConfig: { host: config.host, path: config.path, token: config.token, useCloudFetch: false, useLZ4Compression: true }
  },
  {
    modeName: "CF_off_LZ4_off",
    clientConfig: { host: config.host, path: config.path, token: config.token, useCloudFetch: false, useLZ4Compression: false }
  }
];

/**
 * Extracts the query text from a SQL file.
 * It skips header lines that start with "--".
 */
function extractQuery(fileContent) {
  const lines = fileContent.split("\n");
  // Filter out comment lines and blank lines at the top.
  const queryLines = lines.filter(line => !line.trim().startsWith("--") && line.trim() !== "");
  return queryLines.join("\n").trim();
}

/**
 * Runs a single query under a given mode and returns a result object.
 * If any error occurs, it sets the result values to "FAILED" and moves on.
 */
async function runQueryForMode(queryName, queryText, modeName, clientConfig) {
  const result = { queryName, modeName, time_ms: null, peakMemory_MB: null, rowCount: 0, error: null };
  const client = new DBSQLClient();
  const benchfoodClient = new DBSQLClient();
  try {
    await client.connect(clientConfig);
    const session = await client.openSession();
    await session.executeStatement("SET use_cached_result = false");

    await benchfoodClient.connect({
      host: 'e2-benchfood.cloud.databricks.com',
      path: '/sql/1.0/warehouses/6e681b20741e4674',
      token: 'xx'
    })
    const benchfoodSession = await benchfoodClient.openSession();

    const times = [];
    for (let i = 0; i < ITERATIONS; i += 1) {
      const t0 = performance.now();
      const operation = await session.executeStatement(queryText, { useCloudFetch: clientConfig.useCloudFetch, useLZ4Compression: clientConfig.useLZ4Compression });
      let rowCount = 0;
      // let chunk;
      // const fetchSize = 2000000; // High maxRows value
      // let peakHeap = process.memoryUsage().heapUsed;

      // do {
      //   chunk = await operation.fetchChunk({ maxRows: fetchSize });
      //   if (chunk && chunk.length) rowCount += chunk.length;
      //   const currentHeap = process.memoryUsage().heapUsed;
      //   if (currentHeap > peakHeap) peakHeap = currentHeap;
      // } while (await operation.hasMoreRows());
    await operation.fetchAll();

      await operation.close();
      const t1 = performance.now();
      times.push(t1 - t0);
      result.rowCount = rowCount;

      // benchfood
      // host: e2-benchfood.cloud.databricks.com
      // path: /sql/1.0/warehouses/6e681b20741e4674
      // token: xx

      const insertQuery = `
          INSERT INTO main.node_benchmarking_schema.benchmarking_results
          VALUES (CURRENT_TIMESTAMP, modeName, '${queryName}', ${i + 1}, ${Math.round(t1-t0)});
      `;
      console.log("modeName: ", modeName, "queryName: ", queryName, "iteration: ", i + 1, "time: ", Math.round(t1-t0));
      await benchfoodSession.executeStatement(insertQuery);

      // add a delay to avoid hitting the rate limit
      await new Promise(resolve => setTimeout(resolve, 1000)); // 1 second delay
    }

    // Calculate median time
    times.sort((a, b) => a - b);
    // const medianTime = times[Math.floor(times.length / 2)];

    // result.time_ms = Math.round(medianTime);
    // const trimmedTimes = times.slice(1, -1);
    const sum = times.reduce((a, b) => a + b, 0);
    const averageTime = sum / times.length;

    result.time_ms = Math.round(averageTime);
    result.peakMemory_MB = (process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(1);

    await session.close();
    await client.close();
    await benchfoodSession.close();
    await benchfoodClient.close();
    if (global.gc) { global.gc(); }
  } catch (err) {
    result.error = err.toString();
    result.time_ms = "FAILED";
    result.peakMemory_MB = "FAILED";
    result.rowCount = "FAILED";
    try {
      await client.close();
      await  benchfoodClient.close();
    } catch (e) {
      // ignore errors on close
    }
  }
  return result;
}

/**
 * Iterates over all SQL files and runs each query in all defined modes.
 */
async function runBenchmarks() {
  // List only files that end with .sql
  const files = fs.readdirSync(benchmarkingDir).filter(file => file.endsWith(".sql"));
  const allResults = [];

  for (const file of files) {
    const filePath = path.join(benchmarkingDir, file);
    const fileContent = fs.readFileSync(filePath, "utf8");
    const query = extractQuery(fileContent);
    const queryName = path.basename(file, ".sql");

    console.log(`\nRunning benchmarks for query: ${queryName}`);
    for (const mode of modes) {
      console.log(`  Mode: ${mode.modeName}`);
      // if(!queryName.includes('large_results_0100mb')){
      //   continue;
      // }
      const res = await runQueryForMode(queryName, query, mode.modeName, mode.clientConfig);
      allResults.push(res);
      if (res.error) {
        console.log(`    FAILED: ${res.error}`);
      } else {
        console.log(`    Completed in ${res.time_ms} ms, rows: ${res.rowCount}, Peak memory: ${res.peakMemory_MB} MB`);
      }
    }
  }
  return allResults;
}

// Main execution: run benchmarks and output results
(async () => {
  try {
    const results = await runBenchmarks();

    console.log("\n=== Benchmark Summary ===");
    console.table(results);

    // Write results to CSV file for further analysis
    const csvHeader = "QueryName,Mode,Time_ms,PeakMemory_MB,RowCount,Error\n";
    const csvContent = results.map(r =>
      `${r.queryName},${r.modeName},${r.time_ms},${r.peakMemory_MB},${r.rowCount},${r.error || ""}`
    ).join("\n");
    fs.writeFileSync("benchmark_results.csv", csvHeader + csvContent);
    console.log("\nResults written to benchmark_results.csv");
    process.exit(0);
  } catch (err) {
    console.error("Error running benchmarks:", err);
    process.exit(1);
  }
})();
