// src/routes/auth.js
export default async function (fastify, options) {
    fastify.post(
      "/login",
      {
        schema: {
          description: "Login a user",
          tags: ["Authentication"],
          body: {
            type: "object",
            required: ["email", "password"],
            properties: {
              email: { type: "string", format: "email" },
              password: { type: "string" },
            },
          },
          response: {
            200: {
              description: "Login successful",
              type: "object",
              properties: {
                message: { type: "string" },
                user: {
                  type: "object",
                  properties: {
                    id: { type: "integer" },
                    name: { type: "string" },
                    email: { type: "string" },
                    status: { type: "string" },
                  },
                },
              },
            },
            400: {
              description: "Bad Request",
              type: "object",
              properties: {
                message: { type: "string" },
              },
            },
            401: {
              description: "Invalid credentials",
              type: "object",
              properties: {
                message: { type: "string" },
              },
            },
            500: {
              description: "Internal server error",
              type: "object",
              properties: {
                message: { type: "string" },
              },
            },
          },
        },
      },
      async (request, reply) => {
        const { email, password } = request.body;
  
        if (!email || !password) {
          return reply.status(400).send({ message: "Email and password are required" });
        }
  
        try {
          const { rows } = await fastify.pg.query(
            "SELECT * FROM users WHERE email = $1 AND password = $2",
            [email, password]
          );
  
          if (rows.length === 0) {
            return reply.status(401).send({ message: "Invalid credentials" });
          }
  
          const user = rows[0];
          return reply.status(200).send({
            message: "Login successful",
            user: { id: user.id, name: user.name, email: user.email, status: user.status },
          });
        } catch (err) {
          console.error(err);
          return reply.status(500).send({ message: "Internal server error" });
        }
      }
    );
  }
  