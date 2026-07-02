const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

// =============================================================================
// Configuration
// =============================================================================
const PORT = process.env.PORT || 8080;
const HEARTBEAT_INTERVAL = 30000; // 30 seconds ping interval
const HEARTBEAT_TIMEOUT = 60000;  // 60 seconds timeout for pong response

// =============================================================================
// Express App Setup
// =============================================================================
const app = express();

// CORS configuration - allow all origins for frontend connectivity
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type']
}));

// JSON body parser middleware
app.use(express.json());

// Health check endpoint (useful for deployment platforms)
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

// =============================================================================
// HTTP Server & WebSocket Server Setup
// =============================================================================
const server = http.createServer(app);

// Create WebSocket server attached to the HTTP server
const wss = new WebSocket.Server({ 
  server,
  path: '/track'
});

// =============================================================================
// Connection Management - Organized by busId
// =============================================================================
// Map structure: busId -> Set of WebSocket connections
const busConnections = new Map();

/**
 * Add a WebSocket connection to a specific bus tracking group
 */
function addConnection(busId, ws) {
  if (!busConnections.has(busId)) {
    busConnections.set(busId, new Set());
  }
  busConnections.get(busId).add(ws);
  console.log(`[WS] Client connected to track bus: ${busId}. Total clients: ${busConnections.get(busId).size}`);
}

/**
 * Remove a WebSocket connection from its bus tracking group
 */
function removeConnection(busId, ws) {
  if (busConnections.has(busId)) {
    busConnections.get(busId).delete(ws);
    
    // Clean up empty bus groups to free memory
    if (busConnections.get(busId).size === 0) {
      busConnections.delete(busId);
      console.log(`[WS] No more clients tracking bus: ${busId}. Group removed.`);
    } else {
      console.log(`[WS] Client disconnected from bus: ${busId}. Remaining clients: ${busConnections.get(busId).size}`);
    }
  }
}

/**
 * Broadcast telemetry data to all clients tracking a specific bus
 */
function broadcastToBus(busId, data) {
  const clients = busConnections.get(busId);
  
  if (!clients || clients.size === 0) {
    console.log(`[TELEMETRY] No active clients tracking bus: ${busId}`);
    return 0;
  }

  const message = JSON.stringify(data);
  let sentCount = 0;

  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
      sentCount++;
    }
  });

  console.log(`[TELEMETRY] Broadcast to bus ${busId}: ${sentCount}/${clients.size} clients received update`);
  return sentCount;
}

// =============================================================================
// API Endpoints
// =============================================================================

/**
 * POST /api/telemetry
 * Receive GPS telemetry data from bus tracking devices
 * Immediately broadcasts to all WebSocket clients tracking that bus
 */
app.post('/api/telemetry', (req, res) => {
  try {
    const { busId, latitude, longitude, speed, nextStop, timestamp, heading, altitude } = req.body;

    // Validate required fields
    if (!busId) {
      return res.status(400).json({ error: 'Missing required field: busId' });
    }
    if (typeof latitude !== 'number' || typeof longitude !== 'number') {
      return res.status(400).json({ error: 'Invalid coordinates: latitude and longitude must be numbers' });
    }

    // Validate coordinate ranges
    if (latitude < -90 || latitude > 90) {
      return res.status(400).json({ error: 'Invalid latitude: must be between -90 and 90' });
    }
    if (longitude < -180 || longitude > 180) {
      return res.status(400).json({ error: 'Invalid longitude: must be between -180 and 180' });
    }

    // Build telemetry payload with server timestamp if not provided
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

    // Broadcast to all connected clients tracking this bus
    const clientCount = broadcastToBus(busId, telemetryData);

    // Respond with success
    res.status(200).json({
      success: true,
      message: 'Telemetry received and broadcasted',
      busId,
      clientsNotified: clientCount,
      data: telemetryData
    });

  } catch (error) {
    console.error('[API ERROR]', error.message);
    res.status(500).json({ error: 'Internal server error', details: error.message });
  }
});

// =============================================================================
// WebSocket Connection Handling
// =============================================================================

wss.on('connection', (ws, req) => {
  // Extract busId from URL path
  // URL pattern: /track/:busId
  const urlParts = req.url.split('/');
  const busId = urlParts[urlParts.length - 1];

  if (!busId || busId === 'track') {
    ws.close(4000, 'Invalid busId. Use: ws://server/track/:busId');
    return;
  }

  // Decode URL-encoded busId (handles special characters)
  const decodedBusId = decodeURIComponent(busId);

  console.log(`[WS] New connection request for bus: ${decodedBusId} from ${req.socket.remoteAddress}`);

  // Add connection to the bus tracking group
  addConnection(decodedBusId, ws);

  // Send initial acknowledgment to client
  ws.send(JSON.stringify({
    type: 'connected',
    busId: decodedBusId,
    message: `Successfully connected to track bus ${decodedBusId}`,
    timestamp: new Date().toISOString()
  }));

  // ---------------------------------------------------------------------------
  // Heartbeat Mechanism - Keep connection alive and detect dead connections
  // ---------------------------------------------------------------------------
  ws.isAlive = true;
  ws.lastPongTime = Date.now();

  // Handle pong responses from client
  ws.on('pong', () => {
    ws.isAlive = true;
    ws.lastPongTime = Date.now();
  });

  // Handle incoming messages (if needed for future features)
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      console.log(`[WS MESSAGE] Received from bus ${decodedBusId}:`, data);
      // Can handle client-to-server messages here if needed
    } catch (e) {
      console.log(`[WS MESSAGE] Non-JSON message from ${decodedBusId}:`, message.toString());
    }
  });

  // Handle connection close
  ws.on('close', (code, reason) => {
    console.log(`[WS] Connection closed for bus ${decodedBusId}. Code: ${code}, Reason: ${reason || 'No reason provided'}`);
    removeConnection(decodedBusId, ws);
  });

  // Handle errors
  ws.on('error', (error) => {
    console.error(`[WS ERROR] Connection error for bus ${decodedBusId}:`, error.message);
    removeConnection(decodedBusId, ws);
  });
});

// =============================================================================
// Heartbeat Interval - Clean up dead connections
// =============================================================================
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) {
      // Connection is dead, terminate it
      console.log('[HEARTBEAT] Terminating dead connection');
      return ws.terminate();
    }

    // Mark as not alive, next pong will set it back to true
    ws.isAlive = false;
    ws.ping(); // Send ping to check if connection is still alive
  });
}, HEARTBEAT_INTERVAL);

// Clean up interval on server shutdown
wss.on('close', () => {
  clearInterval(heartbeatInterval);
});

// =============================================================================
// Start Server
// =============================================================================
server.listen(PORT, () => {
  console.log('='.repeat(70));
  console.log('🚌 School Bus Tracking Server Started');
  console.log('='.repeat(70));
  console.log(`HTTP Server running on port: ${PORT}`);
  console.log(`WebSocket Server available at: ws://localhost:${PORT}/track/:busId`);
  console.log('');
  console.log('Endpoints:');
  console.log(`  GET  /health              - Health check`);
  console.log(`  POST /api/telemetry       - Submit GPS telemetry data`);
  console.log(`  WS   /track/:busId        - Subscribe to bus location updates`);
  console.log('');
  console.log('Example telemetry payload:');
  console.log(JSON.stringify({
    busId: 'bus-123',
    latitude: 27.7172,
    longitude: 85.3240,
    speed: 40,
    nextStop: 'Main Gate'
  }, null, 2));
  console.log('='.repeat(70));
});

// =============================================================================
// Graceful Shutdown
// =============================================================================
process.on('SIGTERM', () => {
  console.log('\n[SIGTERM] Received shutdown signal...');
  gracefulShutdown();
});

process.on('SIGINT', () => {
  console.log('\n[SIGINT] Received interrupt signal...');
  gracefulShutdown();
});

function gracefulShutdown() {
  // Close all WebSocket connections
  wss.clients.forEach(client => {
    client.close(1001, 'Server shutting down');
  });

  // Close HTTP server
  server.close(() => {
    console.log('[SHUTDOWN] HTTP server closed');
    clearInterval(heartbeatInterval);
    busConnections.clear();
    console.log('[SHUTDOWN] Cleanup complete. Exiting.');
    process.exit(0);
  });

  // Force exit after 10 seconds if graceful shutdown fails
  setTimeout(() => {
    console.error('[SHUTDOWN] Forced exit after timeout');
    process.exit(1);
  }, 10000);
}
