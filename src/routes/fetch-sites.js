// fetch-sites.js
async function fetchSitesRoute(fastify, options) {
  fastify.get('/fetch-sites', async (request, reply) => {
    try {
      // Extract page and limit from query parameters
      const { page = 1, limit = 20 } = request.query;

      // Ensure limit does not exceed 20 (for performance reasons)
      const maxLimit = 20;
      const validLimit = Math.min(limit, maxLimit);
      const offset = (page - 1) * validLimit;

      // Fetch the paginated sites data from PostgreSQL
      const result = await fastify.pg.query(
        'SELECT * FROM sites LIMIT $1 OFFSET $2',
        [validLimit, offset]
      );
      const queryResult = result.rows;

      // Fetch the total count of records for pagination
      const countResult = await fastify.pg.query('SELECT COUNT(*) AS total FROM sites');
      const totalCount = countResult.rows[0].total;

      // If no result was fetched
      if (!queryResult || queryResult.length === 0) {
        return reply.status(404).send({ message: "No sites found." });
      }

      // Calculate total pages based on total records and limit
      const totalPages = Math.ceil(totalCount / validLimit);

      // Return the sites data with pagination details
      return reply.send({
        data: queryResult,
        pagination: {
          page: Number(page),
          limit: validLimit,
          totalRecords: totalCount,  // Total number of records in the table
          totalPages: totalPages,    // Total number of pages
        },
      });
    } catch (error) {
      console.error("Error fetching sites:", error);
      return reply.status(500).send({ message: "An error occurred while fetching the sites." });
    }
  });
}

module.exports = fetchSitesRoute;
