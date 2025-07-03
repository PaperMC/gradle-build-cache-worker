export default {
  async fetch(request, env, ctx): Promise<Response> {
    const authHeader = request.headers.get("Authorization");
    if (!authHeader || !authHeader.startsWith("Basic ")) {
      return new Response("Unauthorized", { status: 401 });
    }
    const encodedCredentials = authHeader.split(" ")[1];
    const decodedCredentials = atob(encodedCredentials);
    const [username, password] = decodedCredentials.split(":");
    const expectedPassword = await env.KV.get("USER_" + username);
    if (expectedPassword == null || expectedPassword !== password) {
      return new Response("Unauthorized", { status: 401 });
    }

    if (!env.BUCKET) {
      return new Response('R2 bucket "BUCKET" not bound.', { status: 500 });
    }

    const key = new URL(request.url).pathname.slice(1);

    if (!key) {
      return new Response("A key in the URL path is required. e.g., /my-object-key", { status: 400 });
    }

    switch (request.method) {
      case "GET": {
        const object = await env.BUCKET.get(key);

        if (object === null) {
          return new Response(`Object with key "${key}" not found.`, { status: 404 });
        }

        await env.KV.put("LAST_USED_" + key, `${Date.now()}`);

        const headers = new Headers();
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);

        return new Response(object.body, {
          headers,
        });
      }

      case "PUT": {
        if (request.body === null) {
          return new Response("Request body is required for PUT.", { status: 400 });
        }

        await env.BUCKET.put(key, request.body, {
          httpMetadata: request.headers,
        });

        await env.KV.put("LAST_USED_" + key, `${Date.now()}`);

        return new Response(`Object with key "${key}" stored successfully.`, {
          status: 200,
        });
      }

      default:
        return new Response("Method Not Allowed", {
          status: 405,
          headers: {
            Allow: "GET, PUT",
          },
        });
    }
  },

  async scheduled(controller, env, ctx) {
    const keys = await env.KV.list({ prefix: "LAST_USED_" });
    const now = Date.now();
    // 1 week in milliseconds
    const expirationTime = 7 * 24 * 60 * 60 * 1000;

    for (const { name } of keys.keys) {
      const lastUsedString = await env.KV.get(name);
      if (lastUsedString === null) {
        // Shouldn't happen?
        continue;
      }
      const lastUsed = parseInt(lastUsedString, 10);
      if (isNaN(lastUsed) || now - lastUsed > expirationTime) {
        await env.BUCKET.delete(name.replace("LAST_USED_", ""));
        await env.KV.delete(name);
      }
    }
  },
} satisfies ExportedHandler<Env>;
