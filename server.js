const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

// =============================================================================
// Configuration
// =============================================================================
const PORT = process.env.PORT || 5000;
const HEARTBEAT_INTERVAL = 30000; // 30 seconds ping interval
const HEARTBEAT_TIMEOUT = 60000;  // 60 seconds timeout for pong response

// =============================================================================
// Express App Setup
// =============================================================================
const app = express();

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type']
}));

app.use(express.json());

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

// =============================================================================
// HTTP Server & WebSocket Server Setup
// =============================================================================
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// =============================================================================
// Connection Management - Organized by busId
// =============================================================================
const busConnections = new Map();

function addConnection(busId, ws) {
  if (!busConnections.has(busId)) {
    busConnections.set(busId, new Set());
  }
  busConnections.get(busId).add(ws);
  console.log(`[WS] Client connected to track bus: ${busId}. Total clients: ${busConnections.get(busId).size}`);
}

function removeConnection(busId, ws) {
  if (busConnections.has(busId)) {
    busConnections.get(busId).delete(ws);
    
    if (busConnections.get(busId).size === 0) {
      busConnections.delete(busId);
      console.log(`[WS] No more clients tracking bus: ${busId}. Group removed.`);
    } else {
      console.log(`[WS] Client disconnected from bus: ${busId}. Remaining clients: ${busConnections.get(busId).size}`);
    }
  }
}

/**
 * FIXED: Added senderWs parameter to function signature to resolve scope crash
 */
function broadcastToBus(busId, data, senderWs = null) {
  const clients = busConnections.get(busId);
  
  if (!clients || clients.size === 0) {
    console.log(`[TELEMETRY] No active clients tracking bus: ${busId}`);
    return 0;
  }

  const message = JSON.stringify(data);
  let sentCount = 0;

  clients.forEach(ws => {
    // Safely skip echoing data back to the transmitting device
    if (senderWs && ws === senderWs) return;

    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(message);
        sentCount++;
      } catch (err) {
        console.log(`[WS BROADCAST WARNING] Client dropped during transmission frame`);
      }
    }
  });

  console.log(`[TELEMETRY] Broadcast to bus ${busId}: ${sentCount}/${clients.size} clients received update`);
  return sentCount;
}

// =============================================================================
// API Endpoints
// =============================================================================
app.post('/api/telemetry', (req, res) => {
  try {
    const { busId, latitude, longitude, speed, nextStop, timestamp, heading, altitude } = req.body;

    if (!busId) return res.status(400).json({ error: 'Missing required field: busId' });
    if (typeof latitude !== 'number' || typeof longitude !== 'number') {
      return res.status(400).json({ error: 'Invalid coordinates' });
    }

    const telemetryData = {
      busId,
      latitude,
      longitude,
      speed: speed || 0,
      nextStop: nextStop || null,
      heading: heading || null,
      altitude: altitude || null,
      timestamp: timestamp || new Date().toISOString(),
      receivedAt: new Date().toISOString()
    };

    const clientCount = broadcastToBus(busId, telemetryData);
    res.status(200).json({ success: true, clientsNotified: clientCount });

  } catch (error) {
    console.error('[API ERROR]', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// =============================================================================
// WebSocket Connection Handling
// =============================================================================
wss.on('connection', (ws, req) => {
  let decodedBusId = 'unknown';
  try {
    const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathParts = parsedUrl.pathname.split('/').filter(Boolean);
    const busId = pathParts[1];

    if (pathParts[0] !== 'track' || !busId) {
      ws.close(4000, 'Invalid routing path.');
      return;
    }

    decodedBusId = decodeURIComponent(busId);
    console.log(`[WS] Handshake approved for bus: ${decodedBusId}`);
    addConnection(decodedBusId, ws);

    ws.send(JSON.stringify({
      type: 'connected',
      busId: decodedBusId,
      message: `Stream tunnel established for tracking target: ${decodedBusId}`,
      timestamp: new Date().toISOString()
    }));

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message) => {
      try {
        const messageString = message.toString().trim();

        // Absorb browser keep-alive frames smoothly
        if (messageString === 'ping' || messageString === 'pong' || !messageString) {
          if (messageString === 'ping') ws.send('pong');
          return;
        }
        
        const incomingData = JSON.parse(messageString);

        if (incomingData.latitude === undefined || incomingData.longitude === undefined) {
          console.log(`[WS SYSTEM MESSAGE] Client control frame on channel ${decodedBusId}`);
          return; 
        }
        
        const telemetryPayload = {
          busId: decodedBusId,
          latitude: Number(incomingData.latitude),
          longitude: Number(incomingData.longitude),
          speed: Number(incomingData.speed) || 0,
          nextStop: incomingData.nextStop || null,
          heading: incomingData.heading || null,
          timestamp: incomingData.timestamp || new Date().toISOString(),
          receivedAt: new Date().toISOString()
        };

        if (isNaN(telemetryPayload.latitude) || isNaN(telemetryPayload.longitude)) {
          console.log(`[WS WARNING] Corrupted coordinate payload skipped on ${decodedBusId}`);
          return;
        }

        // Fixed signature invocation passes context reference cleanly
        broadcastToBus(decodedBusId, telemetryPayload, ws);
        
      } catch (parseError) {
        // IMPROVED: Log the actual processing exception message to make debugging clear
        console.log(`[WS ERROR] Data processing exception on channel ${decodedBusId}:`, parseError.message);
      }
    });

    ws.on('close', (code) => {
      console.log(`[WS] Connection closed for bus ${decodedBusId}. Code: ${code}`);
      removeConnection(decodedBusId, ws);
    });

    ws.on('error', (error) => {
      console.error(`[WS SYSTEM ERROR] ${decodedBusId}:`, error.message);
      removeConnection(decodedBusId, ws);
    });

  } catch (err) {
    console.error('[WS HANDSHAKE CRASH]', err.message);
    ws.close(1011, 'Internal Server Handshake Error');
  }
});

// =============================================================================
// Heartbeat Supervisor
// =============================================================================
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) {
      console.log('[HEARTBEAT] Terminating dead connection');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

wss.on('close', () => { clearInterval(heartbeatInterval); });

// =============================================================================
// Start Server Context
// =============================================================================
server.listen(PORT, () => {
  console.log(`🚌 Server active on port ${PORT}`);
});

// =============================================================================
// Graceful Shutdown Handler
// =============================================================================
process.on('SIGTERM', () => gracefulShutdown());
process.on('SIGINT', () => gracefulShutdown());

function gracefulShutdown() {
  wss.clients.forEach(client => client.close(1001, 'Server shutting down'));
  server.close(() => {
    clearInterval(heartbeatInterval);
    busConnections.clear();
    process.exit(0);
  });
}
