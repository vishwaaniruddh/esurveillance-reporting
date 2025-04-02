// src/routes/test-redis.js
async function testRedisRoute(fastify, options) {
  fastify.get('/test-redis', async (request, reply) => {
    try {
      // Log Redis client state
      fastify.log.info('Redis client state:', fastify.redis);

      // Check if Redis client is available
      if (!fastify.redis) {
        fastify.log.error('Redis client is not available!');
        throw new Error('Redis client is not available!');
      }

      // Perform a simple Redis operation
      const redisPing = await fastify.redis.ping();
      fastify.log.info(`Redis PING response: ${redisPing}`);

      // Set and get a test value
      await fastify.redis.set('test-key', 'Hello, Redis!');
      const value = await fastify.redis.get('test-key');

      reply.send({ status: 'success', value });
    } catch (error) {
      fastify.log.error('Error interacting with Redis:', error);
      reply.status(500).send({
        status: 'error',
        message: error.message,
        stack: error.stack,
      });
    }
  });
}

module.exports = testRedisRoute;