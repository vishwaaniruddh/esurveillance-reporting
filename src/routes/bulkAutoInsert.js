const mysql = require('mysql2'); // Import MySQL2 for database connection

async function insertAlertsRoute(fastify, options) {
  fastify.post('/insert-alerts', async (request, reply) => {
    // Get the date from the POST body (you can also use query parameters if preferred)
    const { date } = request.body;  // assuming date is passed in the body like { "date": "2025-03-27" }

    if (!date) {
      return reply.status(400).send({ message: 'Date is required in the request body.' });
    }

    const connection = mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: '', 
      database: 'esurv'
    });

    // Counter for inserted batches
    let batchCounter = 0;

    // Function to create the table if not exists
    const createTable = async (date) => {
      const tableName = `backalerts_${date.replace(/-/g, '')}`;
      const createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} LIKE backalerts`;

      try {
        await connection.promise().query(createTableQuery);
        console.log(`Table ${tableName} created or already exists.`);
      } catch (error) {
        console.error(`Error creating table ${tableName}:`, error);
        throw error;
      }
    };

    // Function to insert records in batches
    const insertRecordsBatch = async (date) => {
      try {
        // Query to fetch records for a specific date
        const query = `SELECT * FROM backalerts WHERE DATE(receivedtime) = ? LIMIT 5000`;
        
        const [records] = await connection.promise().query(query, [date]);

        if (records.length === 0) {
          return false;
        }

        // Create table based on the received date
        const tableName = `backalerts_${date.replace(/-/g, '')}`;
        const insertQuery = `INSERT INTO ${tableName} SELECT * FROM backalerts WHERE DATE(receivedtime) = ? order by id desc`;

        await connection.promise().query(insertQuery, [date]);

        batchCounter++; // Increment the batch counter
        console.log(`${batchCounter}. Inserted ${records.length} records into ${tableName}.`);

        return true;
      } catch (error) {
        console.error("Error inserting records:", error);
        return false;
      }
    };

    // Loop function to repeat insertion with a delay
    const loopInsert = async (date) => {
      let continueInserting = true;

      while (continueInserting) {
        continueInserting = await insertRecordsBatch(date);

        // If there are records inserted, wait 100 ms before inserting the next batch
        if (continueInserting) {
          await new Promise(resolve => setTimeout(resolve, 100)); // 100 ms delay
        }
      }

      // After the loop finishes, send the response
      return reply.send({ message: 'All records successfully inserted.' });
    };

    try {
      // First, create the new table for the specific date
      await createTable(date);

      // Start the insertion loop
      await loopInsert(date);
      
    } catch (error) {
      console.error("Error during insert operation:", error);
      return reply.status(500).send({ message: 'An error occurred during the insertion process.', error: error.message });
    } finally {
      // Ensure to close the connection after all operations are done
      connection.end();
    }
  });
}

module.exports = insertAlertsRoute;
