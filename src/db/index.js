const fastifyPlugin = require("fastify-plugin");

async function dbConnector(fastify, options) {
  // MySQL Connection
  fastify.register(require("fastify-mysql"), {
    promise: true,
    connectionString: process.env.MYSQL_CONNECTION_STRING || "mysql://reportingserver:reportingserver@localhost/esurv",
  });

  // PostgreSQL Connection
  fastify.register(require("fastify-postgres"), {
    connectionString: process.env.DATABASE_URL || "postgres://postgres:root@localhost:5432/esurv",
  });
}

module.exports = fastifyPlugin(dbConnector);
