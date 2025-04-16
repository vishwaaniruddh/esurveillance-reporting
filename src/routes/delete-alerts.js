const mysql = require('mysql2'); // Import MySQL2 for database connection

async function deleteAlertsRoute(fastify, options) {
  fastify.post('/delete-alerts', async (request, reply) => {
    const connection = mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: '',  // Make sure to set your password here
      database: 'esurv' // Replace with your database name
    });

    // Counter for deleted batches
    let batchCounter = 0;

    // Function to perform deletion
    const deleteRecordsBatch = async () => {
      try {
        // SQL query to delete 10,001 records older than 10 days
        const query = `DELETE FROM backalerts WHERE DATE(receivedtime) in ('2025-03-22','2025-03-23','2025-03-24') LIMIT 5000`;
        // const query = `DELETE FROM backalerts WHERE DATE(receivedtime) < CURDATE() - INTERVAL 10 DAY ORDER BY id ASC LIMIT 5000`;

        // Execute the query
        const [result] = await connection.promise().query(query);

        // If no rows were deleted, exit the loop
        if (result.affectedRows === 0) {
          return false;
        }

        batchCounter++;  // Increment the batch counter
        console.log(`${batchCounter}. Deleted ${result.affectedRows} records.`); // Log the batch deletion count
        return true;
      } catch (error) {
        console.error("Error deleting records:", error);
        return false;
      }
    };

    // Loop function to repeat deletion with 2-second delay
    const loopDelete = async () => {
      let continueDeleting = true;

      while (continueDeleting) {
        continueDeleting = await deleteRecordsBatch();

        // If there are records deleted, wait 500 ms before deleting the next batch
        if (continueDeleting) {
          await new Promise(resolve => setTimeout(resolve, 100)); // 500 ms delay
        }
      }

      // After the loop finishes, send the response
      return reply.send({ message: 'All records older than 10 days successfully deleted.' });
    };

    // Start the deletion loop
    await loopDelete();

    // Ensure to close the connection after all operations are done
    connection.end();
  });
}

module.exports = deleteAlertsRoute;
