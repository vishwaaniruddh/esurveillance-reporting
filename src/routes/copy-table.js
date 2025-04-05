const { Worker, isMainThread, parentPort } = require('worker_threads');

async function copyTableRoute(fastify, options) {
  fastify.post('/copy-table', async (request, reply) => {
    try {
      const { tableName } = request.body;
      if (!tableName) {
        return reply.status(400).send({ message: 'Table name is required.' });
      }

      // Fetch the schema and data from MySQL
      let mysqlSchema, mysqlData;
      if (fastify.mysql) {
        [mysqlSchema] = await fastify.mysql.query(`DESCRIBE ${tableName}`);
        [mysqlData] = await fastify.mysql.query(`SELECT * FROM ${tableName}`);
      } else {
        return reply.status(500).send({ message: 'MySQL database connection not available.' });
      }

      if (!fastify.pg) {
        return reply.status(500).send({ message: 'PostgreSQL database connection not available.' });
      }

      // Generate PostgreSQL table creation query
      const pgTableSchema = generatePostgresSchema(tableName, mysqlSchema);
      await fastify.pg.query(pgTableSchema);

      // Split data into chunks for multithreading
      const chunkSize = 1000;
      const chunks = [];
      for (let i = 0; i < mysqlData.length; i += chunkSize) {
        chunks.push(mysqlData.slice(i, i + chunkSize));
      }

      // Use worker threads for efficient data insertion
      const workers = chunks.map(chunk => {
        return new Promise((resolve, reject) => {
          const worker = new Worker(__filename, { workerData: { tableName, chunk, mysqlSchema } });
          worker.on('message', resolve);
          worker.on('error', reject);
          worker.on('exit', code => {
            if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
          });
        });
      });

      await Promise.all(workers);

      // After the data has been inserted, fix the column names to lowercase
      await fixColumnsToLowerCase(fastify, tableName);

      return reply.send({ message: `${tableName} successfully copied to PostgreSQL and columns are converted to lowercase.` });
      
    } catch (error) {
      console.error("Error copying table:", error);
      return reply.status(500).send({ message: "An error occurred while copying the table.", error: error.message });
    }
  });
}

// Function to generate PostgreSQL schema from MySQL schema
function generatePostgresSchema(tableName, schema) {
  const columns = schema.map(col => {
    let type = col.Type.toLowerCase();
    if (type.includes('int')) type = 'INTEGER';
    else if (type.includes('varchar') || type.includes('text')) type = 'TEXT';
    else if (type.includes('datetime') || type.includes('timestamp')) type = 'TIMESTAMP';
    else if (type.includes('blob')) type = 'BYTEA';
    else if (type.includes('date')) type = 'DATE';  // For DATE type conversion

    return `"${col.Field}" ${type}`;
  }).join(', ');

  // SQL to drop and then create the table
  return `DROP TABLE IF EXISTS "${tableName}"; CREATE TABLE IF NOT EXISTS "${tableName}" (${columns})`;
}

// Worker thread logic for inserting data
if (!isMainThread) {
  const { workerData } = require('worker_threads');
  const { tableName, chunk, mysqlSchema } = workerData;
  const { Pool } = require('pg');

  const pgPool = new Pool({
    host: 'localhost',
    user: 'postgres',
    password: 'root',
    database: 'esurv',
    port: 5432,
  });

  async function insertData() {
    try {
      const client = await pgPool.connect();
      const columns = mysqlSchema.map(col => `"${col.Field}"`).join(', ');
  
      // Convert data into PostgreSQL compatible values
      const values = chunk.map(row => {
        return `(${mysqlSchema.map(col => {
          if (row[col.Field] === null) return 'NULL';
          if (Buffer.isBuffer(row[col.Field])) return `'\\x${row[col.Field].toString('hex')}'`;
  
          // Handle DATE fields: Convert to 'YYYY-MM-DD' format
          if (col.Type.toLowerCase().includes('date')) {
            const date = new Date(row[col.Field]);
            return `'${date.toISOString().slice(0, 10)}'`;  // Only take the date part: YYYY-MM-DD
          }
  
          // Handle DATETIME or TIMESTAMP fields: Convert to 'YYYY-MM-DD HH:MM:SS' format
          if (col.Type.toLowerCase().includes('datetime') || col.Type.toLowerCase().includes('timestamp')) {
            const date = new Date(row[col.Field]);
            return `'${date.toISOString().slice(0, 19)}'`;  // Convert to YYYY-MM-DD HH:MM:SS format
          }
  
          // For other fields, just return the value as string
          return `'${row[col.Field]}'`;
        }).join(', ')})`;
      }).join(', ');
  
      const insertQuery = `INSERT INTO "${tableName}" (${columns}) VALUES ${values}`;
  
      // Log the generated query for debugging
      console.log("Generated Insert Query:", insertQuery);
  
      await client.query(insertQuery);
      client.release();
      parentPort.postMessage('success');
    } catch (err) {
      console.error("Worker error:", err);
      parentPort.postMessage('error');
    }
  }
  

  insertData();
}

// Function to fix columns to lowercase in PostgreSQL
async function fixColumnsToLowerCase(fastify, tableName) {
  const client = await fastify.pg.connect();
  try {
    // Step 1: Get column names of the table
    const columnsRes = await client.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public'`,
      [tableName]
    );

    let alterQueries = [];

    for (const row of columnsRes.rows) {
      const colName = row.column_name;
      const lowerColName = colName.toLowerCase();

      if (colName !== lowerColName) {
        // Step 2: Generate ALTER TABLE statements to rename columns
        const alterQuery = `ALTER TABLE "${tableName}" RENAME COLUMN "${colName}" TO "${lowerColName}";`;
        alterQueries.push(alterQuery);
      }
    }

    // Step 3: Execute all ALTER TABLE statements
    for (const query of alterQueries) {
      await client.query(query);
    }

    return true; // Columns are successfully renamed to lowercase
  } catch (error) {
    console.error("Error renaming columns:", error);
    throw new Error("An error occurred while fixing column names.");
  } finally {
    client.release();
  }
}

module.exports = copyTableRoute;
