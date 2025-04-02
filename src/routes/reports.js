module.exports = async function (fastify, options) {
    fastify.get("/report", async (request, reply) => {
      return { message: "Report endpoint works!" };
    });
  };
  