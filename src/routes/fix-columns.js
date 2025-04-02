async function fixColumns(fastify, options) {
    fastify.get('/fix-columns', async (req, reply) => {
        const client = await fastify.pg.connect();

        try {
            // Step 1: Get all tables
            const tablesRes = await client.query(
                `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`
            );
            const tables = tablesRes.rows.map(row => row.table_name);

            let alterQueries = [];

            for (const table of tables) {
                // Step 2: Get column names
                const columnsRes = await client.query(
                    `SELECT column_name FROM information_schema.columns WHERE table_name = $1`,
                    [table]
                );

                for (const row of columnsRes.rows) {
                    const colName = row.column_name;
                    const lowerColName = colName.toLowerCase();

                    if (colName !== lowerColName) {
                        // Step 3: Generate ALTER TABLE statements with quoted column names
                        const alterQuery = `ALTER TABLE "${table}" RENAME COLUMN "${colName}" TO "${lowerColName}";`;
                        alterQueries.push(alterQuery);
                    }
                }
            }

            // Step 4: Execute all ALTER TABLE statements
            for (const query of alterQueries) {
                await client.query(query);
            }

            await client.end();
            return reply.send({ success: true, message: 'All columns converted to lowercase', queries: alterQueries });
        } catch (error) {
            await client.end();
            return reply.status(500).send({ success: false, error: error.message });
        }
    });
}

module.exports = fixColumns;
