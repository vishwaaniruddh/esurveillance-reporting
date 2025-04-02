// src/server.js
const fastify = require('fastify')({ logger: true });

// Register CORS plugin (Allow all origins for now)
fastify.register(require('@fastify/cors'), {
  origin: "*", // Allow all origins
  methods: ["GET", "POST", "PUT", "DELETE"], // Allowed methods
});

fastify.register(require('./db/index'));  // Ensure correct path

const copyTableRoute = require('./routes/copy-table');  // Register the new route

// Register your routes
fastify.register(require("./routes/auth"));
fastify.register(require("./routes/fix-columns"));
fastify.register(require("./routes/fetch-sites"));
fastify.register(require("./routes/alerts"));
fastify.register(require("./routes/alertsRoutes"));
fastify.register(copyTableRoute);  // Register the copy table route

// Ensure Redis is ready before starting the server
fastify.after(async (err) => {
  if (err) {
    fastify.log.error('Error during fastify.after:', err);
    process.exit(1);
  }
});

const start = async () => {
  try {
    await fastify.ready();
    await fastify.listen({ port: 3000 });
    console.log('🚀 Server running on http://localhost:3000');
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
