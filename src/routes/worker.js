const { parentPort } = require('worker_threads');
const { Pool } = require('pg');

// Helper function to prepare bulk insert query for PostgreSQL
function prepareBulkInsertQuery(tableName, data, schema) {
  const columns = schema.map(col => col.Field).join(', ');
  const values = data.map(row => {
    return `(${schema.map(col => {
      if (row[col.Field] === null) {
        return 'NULL';
      }
      if (Buffer.isBuffer(row[col.Field])) {
        return `'\\x${row[col.Field].toString('hex')}'`; // Handle binary data (e.g., blobs)
      }
      if (col.Type.toLowerCase().includes('datetime') || col.Type.toLowerCase().includes('timestamp')) {
        const timestamp = new Date(row[col.Field]);
        return `'${timestamp.toISOString()}'`; // Convert datetime to ISO format for PostgreSQL
      }
      return `'${row[col.Field]}'`;
    }).join(', ')})`;
  }).join(', ');

  return `INSERT INTO ${tableName} (${columns}) VALUES ${values}`;
}

// Database connection pool
const pool = new Pool({
  user: 'postgres',     // replace with your PostgreSQL username
  host: 'localhost',
  database: 'esurv',
  password: 'root',     // replace with your PostgreSQL password
  port: 5432,
});

parentPort.on('message', async (message) => {
  const { tableName, data, schema } = message;
  
  try {
    // Split data into chunks if it exceeds a threshold size
    const CHUNK_SIZE = 10000; // Adjust the chunk size if needed
    let chunkIndex = 0;

    // Process each chunk of data
    for (let i = 0; i < data.length; i += CHUNK_SIZE) {
      const chunkData = data.slice(i, i + CHUNK_SIZE);
      const bulkInsertQuery = prepareBulkInsertQuery(tableName, chunkData, schema);
      console.log(`Executing query for chunk ${chunkIndex + 1}:`, bulkInsertQuery); // Log for debugging
      await pool.query(bulkInsertQuery);
      chunkIndex++;
    }

    parentPort.postMessage({ status: 'success', message: 'Data inserted successfully' });
  } catch (error) {
    console.error('Error during bulk insert:', error.message);
    parentPort.postMessage({ status: 'error', message: error.message });
  } finally {
    // Close the pool after work is done
    await pool.end();
  }
});
