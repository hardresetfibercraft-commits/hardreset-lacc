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

server.on("upgrade", (req) => {
  console.log(`[WS] Upgrade request received | path=${req.url}`);
});

const wss = new WebSocketServer({ server });
const clients = new Map();

wss.on("connection", (ws, req) => {
  console.log(`[WS] SOCKET OPENED | path=${req.url}`);

  let authenticated = false;
  let clientInfo = null;

  const authTimeout = setTimeout(() => {
    if (!authenticated) {
      console.log("[WS] Authentication timeout");
      ws.close(4001, "Authentication timeout");
    }
  }, 10000);

  ws.on("message", (raw) => {
    const message = raw.toString();

    if (!authenticated) {
      console.log(`[WS] First frame received | bytes=${Buffer.byteLength(message)}`);

      let data;

      try {
        data = JSON.parse(message);
      } catch (err) {
        console.log("[WS] HANDSHAKE IS NOT VALID JSON");
        console.log(`[WS] Parse error: ${err.message}`);
        return;
      }

      console.log(
        `[WS] Handshake | name=${data.name || "MISSING"} | version=${data.version ?? "MISSING"} | cluster=${data.clusterKey || "MISSING"} | tokenPresent=${Boolean(data.token)} | tokenMatch=${data.token === TOKEN}`
      );

      if (
        !data.name ||
        !data.token ||
        data.version === undefined ||
        !data.clusterKey
      ) {
        console.log("[WS] INVALID HANDSHAKE - missing required field");
        ws.close(4002, "Invalid handshake");
        return;
      }

      if (data.token !== TOKEN) {
        console.log("[WS] BAD AUTH TOKEN");
        ws.send("bad_auth");
        ws.close();
        return;
      }

      if (Number(data.version) > PAYLOAD_VERSION) {
        console.log(
          `[WS] CLIENT PAYLOAD NEWER | client=${data.version} server=${PAYLOAD_VERSION}`
        );
        ws.send("fail_server");
        ws.close();
        return;
      }

      if (Number(data.version) < PAYLOAD_VERSION) {
        console.log(
          `[WS] CLIENT PAYLOAD OLDER | client=${data.version} server=${PAYLOAD_VERSION}`
        );
        ws.send("fail_client");
        ws.close();
        return;
      }

      authenticated = true;
      clearTimeout(authTimeout);

      clientInfo = {
        name: String(data.name),
        clusterKey: String(data.clusterKey),
      };

      clients.set(ws, clientInfo);

      ws.send("success");

      console.log(
        `[LACC] AUTHENTICATED: ${clientInfo.name} | cluster=${clientInfo.clusterKey}`
      );

      return;
    }

    let data;

    try {
      data = JSON.parse(message);
    } catch {
      console.log(`[LACC] Non-JSON frame from ${clientInfo.name}`);
      return;
    }

    if (data.sendMode === undefined || data.message === undefined) {
      console.log(`[LACC] Unknown frame from ${clientInfo.name}`);
      return;
    }

    data.clusterKey = clientInfo.clusterKey;

    const payload = JSON.stringify(data);
    let relayed = 0;

    for (const [client, info] of clients.entries()) {
      if (
        client !== ws &&
        client.readyState === WebSocket.OPEN &&
        info.clusterKey === clientInfo.clusterKey
      ) {
        client.send(payload);
        relayed++;
      }
    }

    console.log(
      `[CHAT] ${clientInfo.name} | message received | relayed=${relayed}`
    );
  });

  ws.on("close", (code, reason) => {
    clearTimeout(authTimeout);

    console.log(
      `[WS] SOCKET CLOSED | code=${code} | reason=${reason.toString() || "none"}`
    );

    if (clientInfo) {
      console.log(
        `[LACC] Disconnected: ${clientInfo.name} | cluster=${clientInfo.clusterKey}`
      );
    }

    clients.delete(ws);
  });

  ws.on("error", (err) => {
    console.error(`[WS] ERROR: ${err.message}`);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`HardReset LACC relay listening on port ${PORT}`);
  console.log(`Expected LACC payload version: ${PAYLOAD_VERSION}`);
});
