const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = process.env.PORT || 3000;
const TOKEN = process.env.LACC_TOKEN;
const PAYLOAD_VERSION = Number(process.env.LACC_PAYLOAD_VERSION || 3);

if (!TOKEN) {
  throw new Error("LACC_TOKEN is not set");
}

const server = http.createServer((req, res) => {
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("HardReset LACC relay online");
    return;
  }

  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server });
const clients = new Map();

wss.on("connection", (ws) => {
  let authenticated = false;
  let clientInfo = null;

  const authTimeout = setTimeout(() => {
    if (!authenticated) {
      ws.close(4001, "Authentication timeout");
    }
  }, 10000);

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!authenticated) {
      if (!data.name || !data.token || data.version === undefined || !data.clusterKey) {
        ws.close(4002, "Invalid handshake");
        return;
      }

      if (data.token !== TOKEN) {
        ws.send("bad_auth");
        ws.close();
        return;
      }

      if (Number(data.version) > PAYLOAD_VERSION) {
        ws.send("fail_server");
        ws.close();
        return;
      }

      if (Number(data.version) < PAYLOAD_VERSION) {
        ws.send("fail_client");
        ws.close();
        return;
      }

      authenticated = true;
      clearTimeout(authTimeout);

      clientInfo = {
        name: data.name,
        clusterKey: String(data.clusterKey)
      };

      clients.set(ws, clientInfo);

      ws.send("success");

      console.log(
        `[LACC] Connected: ${clientInfo.name} | cluster=${clientInfo.clusterKey}`
      );

      return;
    }

    if (data.sendMode === undefined || data.message === undefined) {
      return;
    }

    data.clusterKey = clientInfo.clusterKey;

    const payload = JSON.stringify(data);

    for (const [client, info] of clients.entries()) {
      if (
        client !== ws &&
        client.readyState === WebSocket.OPEN &&
        info.clusterKey === clientInfo.clusterKey
      ) {
        client.send(payload);
      }
    }
  });

  ws.on("close", () => {
    clearTimeout(authTimeout);

    if (clientInfo) {
      console.log(
        `[LACC] Disconnected: ${clientInfo.name} | cluster=${clientInfo.clusterKey}`
      );
    }

    clients.delete(ws);
  });

  ws.on("error", (err) => {
    console.error("[LACC] WebSocket error:", err.message);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`HardReset LACC relay listening on port ${PORT}`);
});
