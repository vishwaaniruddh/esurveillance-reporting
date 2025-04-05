const { Worker, isMainThread, parentPort, workerData } = require("worker_threads");

async function makeAlertReportTableRoute(fastify, options) {
  fastify.get("/make-alert-report-table", async (request, reply) => {
    const mysqlClient = fastify.mysql;
    if (!mysqlClient) {
      return reply.status(500).send({ message: "MySQL database connection not available." });
    }

    try {
      // Step 1: Get distinct dates from alerts table
      const [dates] = await mysqlClient.query("SELECT DISTINCT DATE(receivedtime) AS alert_date FROM backalerts WHERE DATE(receivedtime) > '2025-03-24'");
      if (dates.length === 0) {
        return reply.send({ message: "No distinct dates found in alerts table." });
      }

      console.log(dates);
      // Step 2: Process each date using worker threads
      const workers = dates.map(({ alert_date }) => {
        return new Promise((resolve, reject) => {
          const worker = new Worker(__filename, { workerData: { alert_date } });

          worker.on("message", resolve);
          worker.on("error", reject);
          worker.on("exit", (code) => {
            if (code !== 0) reject(new Error(`Worker exited with code ${code}`));
          });
        });
      });

      await Promise.all(workers);

      return reply.send({ message: "Partitioning complete!" });

    } catch (error) {
      console.error("Error in partitioning alerts:", error);
      return reply.status(500).send({ error: "Internal Server Error", message: error.message });
    }
  });
}

// Worker Thread Logic (Only executes when workerData is provided)
if (!isMainThread) {
  const mysql = require("mysql2/promise");

  async function processAlertPartition() {
    const { alert_date } = workerData;
    const table_name = `backalerts_${alert_date.toISOString().split("T")[0].replace(/-/g, "")}`;

    const mysqlPool = mysql.createPool({
      host: "localhost",
      user: "root",
      password: "",
      database: "esurv",
      connectionLimit: 5, // Limit concurrent connections
    });

    const connection = await mysqlPool.getConnection();
    try {
      console.log(`Processing alerts for date: ${alert_date} (Table: ${table_name})`);

      // Step 1: Create partition table if it doesn't exist
      await connection.query(`
        CREATE TABLE IF NOT EXISTS ${table_name} AS 
        SELECT * FROM backalerts WHERE DATE(receivedtime) = ?;
      `, [alert_date]);

      // Step 2: Count records
      const [[{ num_records }]] = await connection.query(`SELECT COUNT(*) AS num_records FROM ${table_name}`);

      // Step 3: Insert tracking info
      await connection.query(`
        INSERT INTO alerts_data_tracker_datewise (alerts_data_date, newtableName, number_of_records)
        VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE number_of_records = VALUES(number_of_records);
      `, [alert_date, table_name, num_records]);

      console.log(`✅ Table ${table_name} created with ${num_records} records.`);

      parentPort.postMessage(`Success: ${table_name}`);

    } catch (err) {
      console.error(`Error processing ${table_name}:`, err);
      parentPort.postMessage(`Error: ${table_name}`);
    } finally {
      connection.release();
    }
  }

  processAlertPartition();
}

module.exports = makeAlertReportTableRoute;
