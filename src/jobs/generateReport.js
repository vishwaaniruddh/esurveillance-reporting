const { Worker } = require("bullmq");
const { Pool } = require("pg");
const fs = require("fs");
const { parse } = require("json2csv");

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

const worker = new Worker("reportQueue", async (job) => {
  console.log("Generating report...");

  const client = await pool.connect();
  try {
    const { rows } = await client.query("SELECT * FROM rass");
    
    const csvData = parse(rows);
    const filePath = `./reports/report_${Date.now()}.csv`;
    fs.writeFileSync(filePath, csvData);
    
    console.log(`Report saved: ${filePath}`);
  } finally {
    client.release();
  }
});

console.log("📢 Worker started for report generation...");
