const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

// 🔥 Firebase Admin Setup
const admin = require("firebase-admin");

// NOTE: When hosting on Render.com, use the "Secret Files" feature 
// to upload this JSON file so it isn't exposed publicly on GitHub!
const serviceAccount = JSON.parse(process.env.FIREBASE_KEY);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});
const db = admin.firestore();

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const roomMemory = {};
const roomUsers = {}; 
const hostTracker = {}; 

io.on('connection', (socket) => {
  console.log(`🟢 User connected: ${socket.id}`);

  socket.on('join-room', (data) => {
    const roomId = data.roomId;
    const userName = data.userName || "Guest";
    const isHost = data.isHost || false;
    
    socket.join(roomId);
    console.log(`🏠 ${userName} joined room: ${roomId}`);

    if (isHost) hostTracker[socket.id] = roomId;
    else socket.to(roomId).emit('force-host-sync');

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    roomUsers[roomId].push({ id: socket.id, name: userName });

    io.to(roomId).emit('update-users', roomUsers[roomId]);
    if (roomMemory[roomId]) socket.emit('sync-action', roomMemory[roomId]);
  });

  socket.on('video-action', (data) => {
    roomMemory[data.roomId] = { action: data.action, time: data.time };
    socket.to(data.roomId).emit('sync-action', data);
  });

  socket.on('change-video', (data) => {
    roomMemory[data.roomId] = { action: 'PLAY', time: 0, videoId: data.videoId };
    socket.to(data.roomId).emit('video-changed', data.videoId);
  });

  socket.on('end-room', (roomId) => {
    console.log(`🛑 Room ${roomId} ended normally.`);
    socket.to(roomId).emit('room-closed');
    delete roomMemory[roomId];
    delete roomUsers[roomId];
  });

  socket.on('chat-message', (data) => {
    io.in(data.roomId).emit('chat-message', data); 
  });

  // ==========================================
  // 📞 WEBRTC PRIVATE CALL SIGNALING 
  // ==========================================
  socket.on('call-request', (data) => io.to(data.targetId).emit('call-request', data));
  socket.on('call-response', (data) => io.to(data.targetId).emit('call-response', data));
  socket.on('webrtc-offer', (data) => io.to(data.targetId).emit('webrtc-offer', data));
  socket.on('webrtc-answer', (data) => io.to(data.targetId).emit('webrtc-answer', data));
  socket.on('webrtc-ice-candidate', (data) => io.to(data.targetId).emit('webrtc-ice-candidate', data));
  socket.on('end-call', (data) => io.to(data.targetId).emit('end-call', data));

  // ==========================================

  socket.on('disconnect', () => {
    console.log(`🔴 User disconnected: ${socket.id}`);
    
    if (hostTracker[socket.id]) {
      const roomId = hostTracker[socket.id];
      console.log(`💥 Host closed the tab! Deleting room ${roomId} from Firebase...`);
      
      db.collection('rooms').doc(roomId).delete()
        .then(() => console.log(`✅ Ghost Room ${roomId} successfully destroyed.`))
        .catch((error) => console.error("Error deleting ghost room:", error));

      io.to(roomId).emit('room-closed');
      
      delete hostTracker[socket.id];
      delete roomMemory[roomId];
      delete roomUsers[roomId];
    } else {
      for (const roomId in roomUsers) {
        const index = roomUsers[roomId].findIndex(user => user.id === socket.id);
        if (index !== -1) {
          roomUsers[roomId].splice(index, 1);
          io.to(roomId).emit('update-users', roomUsers[roomId]); 
          break;
        }
      }
    }
  });
});

// 🔥 DYNAMIC PORT FIX: Required for free cloud hosting!
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Sync Engine Server is running on port ${PORT}`);
});