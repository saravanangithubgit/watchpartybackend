const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');

const app = express();
app.use(cors());
app.get('/', (_req, res) => res.send('Watch Party sync server is running'));
app.get('/health', (_req, res) => res.json({ ok: true, uptime: process.uptime() }));

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
  cors: { origin: '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 1e6,
});

const roomMemory = {};
const roomUsers = {};
const hostBySocket = {};
const hostByRoom = {};

function normalizeRoomId(roomIdRaw) {
  return String(roomIdRaw || '').trim().toUpperCase();
}

function normalizeSocketId(socketIdRaw) {
  return String(socketIdRaw || '').trim();
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
    io.to(targetId).emit(eventName, data);
    return;
  }

  const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
  const hostSocketId = roomId ? getHostSocketId(roomId) : null;
  if (hostSocketId) {
    io.to(hostSocketId).emit(eventName, {
      ...data,
      roomId,
      targetId: hostSocketId,
      callerId: data.callerId || socket.id,
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
  }
}

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  socket.on('join-room', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId);
    const userName = String(data.userName || 'Guest').trim() || 'Guest';
    const isHost = data.isHost === true;

    if (!roomId) return;

    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.userName = userName;
    socket.data.isHost = isHost;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    roomUsers[roomId] = roomUsers[roomId].filter((user) => user.id !== socket.id);
    roomUsers[roomId].push({ id: socket.id, name: userName, isHost });

    if (isHost) {
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
    if (!roomId || !data.action) return;

    roomMemory[roomId] = {
      ...(roomMemory[roomId] || {}),
      sync: { action: data.action, time: data.time || 0 },
    };
    socket.to(roomId).emit('sync-action', data);
  });

  socket.on('change-video', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
    if (!roomId || !data.videoId) return;

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
    if (!roomId) return;

    socket.to(roomId).emit('room-closed');
    delete roomMemory[roomId];
    delete roomUsers[roomId];
    delete hostByRoom[roomId];
  });

  socket.on('chat-message', (data = {}) => {
    const roomId = normalizeRoomId(data.roomId || socket.data.roomId);
    if (roomId) io.in(roomId).emit('chat-message', { ...data, roomId });
  });

  socket.on('call-request', (data = {}) => emitToTargetOrHost(socket, 'call-request', data));
  socket.on('call-response', (data = {}) => emitToTargetOrHost(socket, 'call-response', data));
  socket.on('webrtc-offer', (data = {}) => emitToTargetOrHost(socket, 'webrtc-offer', data));
  socket.on('webrtc-answer', (data = {}) => emitToTargetOrHost(socket, 'webrtc-answer', data));
  socket.on('webrtc-ice-candidate', (data = {}) => emitToTargetOrHost(socket, 'webrtc-ice-candidate', data));
  socket.on('end-call', (data = {}) => emitToTargetOrHost(socket, 'end-call', data));

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    const roomId = socket.data.roomId || hostBySocket[socket.id];

    if (hostBySocket[socket.id]) {
      console.log(`Host disconnected. Closing room ${roomId}`);
      db.collection('rooms').doc(roomId).delete().catch((error) => {
        console.error('Error deleting ghost room:', error);
      });
      io.to(roomId).emit('room-closed');
      delete hostBySocket[socket.id];
      delete hostByRoom[roomId];
      delete roomMemory[roomId];
      delete roomUsers[roomId];
      return;
    }

    removeUserFromRoom(socket, roomId);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Watch Party sync server running on port ${PORT}`);
});
