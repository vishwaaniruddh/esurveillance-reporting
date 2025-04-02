// src/plugins/redis.js
const Redis = require('ioredis');

async function redisConnector(fastify, options) {
  try {
    const redis = new Redis({
      host: 'localhost',
      port: 6379,
    });

    redis.on('connect', () => {
      fastify.log.info('Successfully connected to Redis/Memurai!');
    });

    redis.on('error', (err) => {
      fastify.log.error('Redis connection error:', err);
    });

    // Attach the Redis client to Fastify instance
    fastify.decorate('redis', redis);

    // Ensure redis.ping completes before continuing.
    try {
      const redisPing = await fastify.redis.ping();
      fastify.log.info(`Redis PING response: ${redisPing}`);
      fastify.log.info('Redis client attached successfully!');
    } catch (pingError) {
      fastify.log.error('Redis PING failed:', pingError);
      throw pingError; // Propagate the error to prevent plugin from resolving
    }
  } catch (error) {
    fastify.log.error('Failed to connect to Redis/Memurai:', error);
    throw error; // Propagate the error to prevent the plugin from resolving
  }
}

module.exports = redisConnector;