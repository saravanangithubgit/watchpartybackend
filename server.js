const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');

const app = express();
app.use(cors());
app.get('/', (_req, res) => res.send('Watch Party sync server is running'));
app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime() }));
app.get('/api/gifs', async (req, res) => {
  const query = String(req.query.q || '').trim().slice(0, 80);
  const apiKey = process.env.GIPHY_API_KEY;
  if (!query) return res.status(400).json({ error: 'A search query is required.' });
  if (!apiKey) return res.status(503).json({ error: 'GIF search is not configured.' });

  try {
    const upstream = await fetch(
      `https://api.giphy.com/v1/gifs/search?api_key=${encodeURIComponent(apiKey)}&q=${encodeURIComponent(query)}&limit=20&rating=g`,
    );
    if (!upstream.ok) {
      return res.status(502).json({ error: 'GIF provider is unavailable.' });
    }
    const body = await upstream.json();
    const gifs = Array.isArray(body.data)
      ? body.data.map((gif) => gif?.images?.fixed_height?.url).filter(Boolean)
      : [];
    res.json({ gifs });
  } catch (error) {
    console.error('GIF search failed:', error);
    res.status(502).json({ error: 'GIF provider is unavailable.' });
  }
});

if (!process.env.FIREBASE_KEY) {
  throw new Error('Missing FIREBASE_KEY environment variable');
}

const serviceAccount = JSON.parse(process.env.FIREBASE_KEY);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();
const server = http.createServer(app);
const io = new Server(server, {
  // Native apps have no browser Origin header; browser origins can be locked
  // down with CLIENT_ORIGIN in production.
  cors: { origin: process.env.CLIENT_ORIGIN || true, methods: ['GET', 'POST'] },
  maxHttpBufferSize: 1e6,
  transports: ['websocket', 'polling'],
  pingTimeout: 120000,
  pingInterval: 25000,
});

const roomMemory = {};
const roomUsers = {};
const hostBySocket = {};
const hostByRoom = {};
const hostDisconnectTimers = {};
const chatSequenceByRoom = {};

io.engine.on('connection_error', (error) => {
  console.error('Socket engine connection error:', {
    code: error.code,
    message: error.message,
    context: error.context,
  });
});

function normalizeRoomId(roomIdRaw) {
  return String(roomIdRaw || '').trim().toUpperCase();
}

function normalizeSocketId(socketIdRaw) {
  return String(socketIdRaw || '').trim();
}

function getSocketInSameRoom(socket, targetId) {
  const target = io.sockets.sockets.get(targetId);
  return target && target.data.roomId === socket.data.roomId ? target : null;
}

function isVerifiedHost(socket, roomId) {
  return socket.data.isHost === true && socket.data.roomId === roomId;
}

function getHostSocketId(roomId) {
  const hostSocketId = hostByRoom[roomId];
  if (hostSocketId) return hostSocketId;

  const hostUser = (roomUsers[roomId] || []).find((user) => user.isHost);
  return hostUser?.id || null;
}

function emitToTargetOrHost(socket, eventName, data = {}) {
  const targetId = normalizeSocketId(data.targetId);
  if (targetId) {
    // Never let a participant signal, call, or send ICE to a socket in another room.
    if (getSocketInSameRoom(socket, targetId)) {
      io.to(targetId).emit(eventName, { ...data, callerId: socket.id });
    }
    return;
  }

  const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
  const hostSocketId = roomId ? getHostSocketId(roomId) : null;
  if (hostSocketId) {
    io.to(hostSocketId).emit(eventName, {
      ...data,
      roomId,
      targetId: hostSocketId,
      callerId: socket.id,
    });
  }
}

function updateUsers(roomId) {
  io.to(roomId).emit('update-users', roomUsers[roomId] || []);
}

function removeUserFromRoom(socket, roomId) {
  if (!roomId || !roomUsers[roomId]) return;
  roomUsers[roomId] = roomUsers[roomId].filter((user) => user.id !== socket.id);
  updateUsers(roomId);

  if (roomUsers[roomId].length === 0) {
    delete roomUsers[roomId];
    delete chatSequenceByRoom[roomId];
  }
}

function clearHostDisconnectTimer(roomId) {
  if (!hostDisconnectTimers[roomId]) return;
  clearTimeout(hostDisconnectTimers[roomId]);
  delete hostDisconnectTimers[roomId];
}

function closeRoomAfterHostGracePeriod(socketId, roomId) {
  clearHostDisconnectTimer(roomId);
  hostDisconnectTimers[roomId] = setTimeout(() => {
    if (hostByRoom[roomId] && hostByRoom[roomId] !== socketId) {
      delete hostDisconnectTimers[roomId];
      return;
    }

    console.log(`Host did not reconnect. Closing room ${roomId}`);
    db.collection('rooms').doc(roomId).delete().catch((error) => {
      console.error('Error deleting ghost room:', error);
    });
    io.to(roomId).emit('room-closed');
    delete hostBySocket[socketId];
    delete hostByRoom[roomId];
    delete roomMemory[roomId];
    delete roomUsers[roomId];
    delete chatSequenceByRoom[roomId];
    delete hostDisconnectTimers[roomId];
  }, 5 * 60 * 1000);
}

io.use(async (socket, next) => {
  const token = socket.handshake.auth?.token;
  if (!token || typeof token !== 'string') {
    return next(new Error('Authentication required'));
  }
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    socket.data.uid = decoded.uid;
    next();
  } catch (error) {
    console.warn('Rejected socket authentication:', error.code || error.message);
    next(new Error('Invalid authentication token'));
  }
});

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on('join-room', async (data = {}) => {
    const roomId = normalizeRoomId(data.roomId);
    const userName = String(data.userName || 'Guest').trim() || 'Guest';
    const isHost = data.isHost === true;

    if (!roomId) return;

    let room;
    try {
      const roomSnapshot = await db.collection('rooms').doc(roomId).get();
      if (!roomSnapshot.exists) {
        socket.emit('room-error', { code: 'not-found', message: 'Room no longer exists.' });
        return;
      }
      room = roomSnapshot.data();
    } catch (error) {
      console.error('Room lookup failed:', error);
      socket.emit('room-error', { code: 'unavailable', message: 'Could not verify room access.' });
      return;
    }

    if (isHost && room.hostId !== socket.data.uid) {
      socket.emit('room-error', { code: 'not-host', message: 'Only the room owner can host this room.' });
      return;
    }

    console.log(`User ${socket.id} joining room ${roomId} as ${isHost ? 'host' : 'viewer'}`);

    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.userName = userName;
    socket.data.isHost = isHost;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    roomUsers[roomId] = roomUsers[roomId].filter((user) => {
      if (user.id === socket.id) return false;
      if (isHost && user.isHost) return false;
      return true;
    });
    roomUsers[roomId].push({ id: socket.id, name: userName, isHost });

    if (isHost) {
      clearHostDisconnectTimer(roomId);
      Object.keys(hostBySocket).forEach((hostSocketId) => {
        if (hostBySocket[hostSocketId] === roomId) delete hostBySocket[hostSocketId];
      });
      hostBySocket[socket.id] = roomId;
      hostByRoom[roomId] = socket.id;
    }

    updateUsers(roomId);

    if (roomMemory[roomId]?.sync) {
      socket.emit('sync-action', roomMemory[roomId].sync);
    }
    if (roomMemory[roomId]?.videoId) {
      socket.emit('video-changed', roomMemory[roomId].videoId);
    }

    if (!isHost) {
      const hostSocketId = getHostSocketId(roomId);
      if (hostSocketId) {
        io.to(hostSocketId).emit('force-host-sync', { roomId, viewerId: socket.id });
      }
    }
  });

  socket.on('video-action', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
    if (!roomId || !data.action || !isVerifiedHost(socket, roomId)) return;

    roomMemory[roomId] = {
      ...(roomMemory[roomId] || {}),
      sync: { action: data.action, time: data.time || 0 },
    };
    socket.to(roomId).emit('sync-action', data);
  });

  socket.on('change-video', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
    if (!roomId || !data.videoId || !isVerifiedHost(socket, roomId)) return;

    roomMemory[roomId] = {
      ...(roomMemory[roomId] || {}),
      videoId: data.videoId,
      sync: undefined,
    };
    socket.to(roomId).emit('video-changed', data.videoId);
    const hostSocketId = getHostSocketId(roomId);
    if (hostSocketId) {
      io.to(hostSocketId).emit('force-host-sync', { roomId });
    }
  });

  socket.on('request-host-sync', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
    if (!roomId) return;

    if (roomMemory[roomId]?.sync) {
      socket.emit('sync-action', roomMemory[roomId].sync);
    }

    const hostSocketId = getHostSocketId(roomId);
    if (hostSocketId) {
      io.to(hostSocketId).emit('force-host-sync', { roomId, viewerId: socket.id });
    }
  });

  socket.on('end-room', (roomIdRaw) => {
    const roomId = normalizeRoomId(roomIdRaw || socket.data.roomId);
    if (!roomId || !isVerifiedHost(socket, roomId)) return;

    clearHostDisconnectTimer(roomId);
    socket.to(roomId).emit('room-closed');
    delete roomMemory[roomId];
    delete roomUsers[roomId];
    delete hostByRoom[roomId];
    delete hostBySocket[socket.id];
    db.collection('rooms').doc(roomId).delete().catch((error) => {
      console.error('Error deleting ended room:', error);
    });
  });

  socket.on('chat-message', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
    if (!roomId || socket.data.roomId !== roomId) return;
    const text = typeof data.text === 'string' ? data.text.slice(0, 12000) : '';
    const url = typeof data.url === 'string' ? data.url.slice(0, 4000) : null;
    const type = data.type === 'gif' ? 'gif' : 'text';
    const sequence = (chatSequenceByRoom[roomId] || 0) + 1;
    chatSequenceByRoom[roomId] = sequence;
    io.in(roomId).emit('chat-message', {
      roomId,
      id: `${roomId}-${sequence}`,
      sequence,
      sentAt: Date.now(),
      type,
      text,
      url,
      user: socket.data.userName || 'Guest',
    });
  });

  socket.on('call-request', (data = {}) => emitToTargetOrHost(socket, 'call-request', data));
  socket.on('call-response', (data = {}) => emitToTargetOrHost(socket, 'call-response', data));
  socket.on('webrtc-offer', (data = {}) => emitToTargetOrHost(socket, 'webrtc-offer', data));
  socket.on('webrtc-answer', (data = {}) => emitToTargetOrHost(socket, 'webrtc-answer', data));
  socket.on('webrtc-ice-candidate', (data = {}) => emitToTargetOrHost(socket, 'webrtc-ice-candidate', data));
  socket.on('end-call', (data = {}) => emitToTargetOrHost(socket, 'end-call', data));

  socket.on('disconnect', (reason) => {
    console.log(`User disconnected: ${socket.id}. Reason: ${reason}`);
    const roomId = socket.data.roomId || hostBySocket[socket.id];

    if (hostBySocket[socket.id]) {
      console.log(`Host disconnected. Waiting briefly for reconnect in room ${roomId}`);
      delete hostBySocket[socket.id];
      if (hostByRoom[roomId] === socket.id) {
        delete hostByRoom[roomId];
      }
      removeUserFromRoom(socket, roomId);
      closeRoomAfterHostGracePeriod(socket.id, roomId);
      return;
    }

    removeUserFromRoom(socket, roomId);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Watch Party sync server running on port ${PORT}`);
});
