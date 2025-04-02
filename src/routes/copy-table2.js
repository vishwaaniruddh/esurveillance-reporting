const { Worker, isMainThread, parentPort } = require('worker_threads');
const fs = require('fs');
const path = require('path');
const util = require('util');

// Function to generate CSV for COPY command in PostgreSQL
function generateCSVForCopy(data, schema) {
  const header = schema.map(col => col.Field).join(',');
  const rows = data.map(row => {
    return schema.map(col => {
      if (row[col.Field] === null) {
        return 'NULL';
      }
      if (Buffer.isBuffer(row[col.Field])) {
        return `'\\x${row[col.Field].toString('hex')}'`; // Handle binary data
      }
      if (col.Type.toLowerCase().includes('datetime') || col.Type.toLowerCase().includes('timestamp')) {
        const timestamp = new Date(row[col.Field]);
        return `'${timestamp.toISOString()}'`;
      }
      return `'${row[col.Field]}'`;
    }).join(',');
  }).join('\n');

  return `${header}\n${rows}`;
}

async function copyTableRoute(fastify, options) {
  fastify.post('/copy-table', async (request, reply) => {
    try {
      const { tableName } = request.body;

      if (!tableName) {
        return reply.status(400).send({ message: 'Table name is required.' });
      }

      // Fetch the table schema and data from MySQL
      let mysqlSchema;
      let mysqlData;

      if (fastify.mysql) {
        [mysqlSchema] = await fastify.mysql.query(`DESCRIBE ${tableName}`);
        [mysqlData] = await fastify.mysql.query(`SELECT * FROM ${tableName}`);

        console.log(mysqlSchema)
      } else {
        return reply.status(500).send({ message: 'MySQL database connection not available.' });
      }

      if (fastify.pg) {
        // Prepare CSV data in memory for bulk loading into PostgreSQL using COPY command
        const csvData = generateCSVForCopy(mysqlData, mysqlSchema);

        // Temp file path to store the CSV data in memory
        const filePath = path.join(__dirname, `${tableName}_copy.csv`);

        // Write the CSV data to a temp file
        await util.promisify(fs.writeFile)(filePath, csvData);

        // Use PostgreSQL COPY to load data from the file
        const copyQuery = `
          COPY ${tableName} (${mysqlSchema.map(col => col.Field).join(', ')})
          FROM '${filePath}'
          DELIMITER ','
          CSV HEADER;
        `;
        
        await fastify.pg.query(copyQuery);

        // Delete the temp CSV file after copy operation
        fs.unlinkSync(filePath);

        return reply.send({ message: `${tableName} successfully copied to PostgreSQL using COPY command.` });
      } else {
        return reply.status(500).send({ message: 'PostgreSQL database connection not available.' });
      }
    } catch (error) {
      console.error("Error copying table:", error);
      return reply.status(500).send({ message: "An error occurred while copying the table.", error: error.message });
    }
  });
}

module.exports = copyTableRoute;
