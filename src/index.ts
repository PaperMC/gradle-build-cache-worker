export default {
  async fetch(request, env, ctx): Promise<Response> {
    const authHeader = request.headers.get("Authorization");
    if (!authHeader || !authHeader.startsWith("Basic ")) {
      return new Response("Unauthorized", { status: 401 });
    }
    try {
      const encodedCredentials = authHeader.split(" ")[1];
      const decodedCredentials = atob(encodedCredentials);
      const [username, password] = decodedCredentials.split(":");
      const expectedPassword = await env.KV.get("USER_" + username);
      if (expectedPassword == null || expectedPassword !== password) {
        return new Response("Unauthorized", { status: 401 });
      }
    } catch (e) {
      return new Response("Unauthorized", { status: 401 });
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

  scheduled: async function (controller, env, ctx) {
    /**
     * 1) Delete expired objects
     */
    const keys = await env.KV.list({ prefix: "LAST_USED_" });
    const now = Date.now();
    const expirationTime = parseInt(env.EXPIRE_AFTER_ACCESS);
    const remainingKeys: string[] = [];

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
      } else {
        remainingKeys.push(name);
      }
    }

    /**
     * 2) Enforce maximum cache size
     */
    const maxSizeBytes = parseInt(env.MAX_CACHE_SIZE);
    if (maxSizeBytes <= 0) {
      // Max size disabled
      return;
    }

    let totalSize = 0;
    const sizeMap: Record<string, number> = {};
    const objects = await env.BUCKET.list();
    for (const object of objects.objects) {
      totalSize += object.size;
      sizeMap[object.key] = object.size;
    }

    if (totalSize <= maxSizeBytes) {
      return;
    }

    const remainingMap = await env.KV.get(remainingKeys);
    const remainingAccessed: [string, number][] = [];
    for (const [key, value] of Object.entries(remainingMap)) {
      remainingAccessed.push([key.replace("LAST_USED_", ""), parseInt(value, 10)]);
    }

    remainingAccessed.sort((a, b) => a[1] - b[1]);
    while (totalSize > maxSizeBytes && remainingAccessed.length > 0) {
      const [key, lastUsed] = remainingAccessed.shift()!;
      const size = sizeMap[key];
      if (size === undefined) {
        continue;
      }
      await env.BUCKET.delete(key);
      await env.KV.delete("LAST_USED_" + key);
      totalSize -= size;
    }
  },
} satisfies ExportedHandler<Env>;
