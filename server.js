/**
 * Du Sen Zen Pond - Realtime Socket.io Server (v6.5.0 User Identity Fix)
 * GitHub Repo Server code for Render deployment
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const MAX_SEATS = 17;
let seats = Array.from({ length: MAX_SEATS }, (_, i) => ({
    id: i + 1,
    occupiedBy: null
}));

let totalHearts = 0;
let lanternCounts = {
    'An yên': 0,
    'Bình an': 0,
    'Biết ơn': 0
};
let currentTrackIndex = 0;
let trackStartTime = Date.now();
let activePlaylist = [];

io.on('connection', (socket) => {
    // 1. Kiểm tra vị trí trống & Ghi nhớ tên User thật
    socket.on('request-entry', (userObj) => {
        if (userObj && userObj.name) {
            socket.userName = userObj.name;
            socket.userAvatar = userObj.avatar || '';
        } else {
            socket.userName = 'Lữ khách';
        }

        const occupiedCount = seats.filter(s => s.occupiedBy !== null).length;
        const availableSeats = MAX_SEATS - occupiedCount;
        const isFull = availableSeats <= 0;

        socket.emit('entry-status-checked', {
            isFull: isFull,
            availableSeats: availableSeats,
            maxSeats: MAX_SEATS,
            seats: seats
        });
    });

    // 2. Gửi trạng thái khởi tạo đầy đủ cho Client
    socket.emit('init-state', {
        seats: seats,
        totalHearts: totalHearts,
        lanternCounts: lanternCounts,
        audioState: {
            trackIndex: currentTrackIndex,
            trackStartTime: trackStartTime,
            url: activePlaylist[currentTrackIndex]?.url || '',
            title: activePlaylist[currentTrackIndex]?.title || ''
        }
    });

    // 3. Đăng ký nhận vị trí (Claim Seat) với Tên User thật
    socket.on('claim-seat', (seatId) => {
        seats = seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        const targetIndex = seats.findIndex(s => s.id === seatId);
        if (targetIndex !== -1 && !seats[targetIndex].occupiedBy) {
            seats[targetIndex].occupiedBy = {
                socketId: socket.id,
                name: socket.userName || 'Lữ khách',
                avatar: socket.userAvatar || ''
            };
        }

        io.emit('seat-state-changed', seats);
    });

    // 4. Đồng bộ Nhạc Thiền Realtime
    socket.on('sync-playlist', (data) => {
        if (data && Array.isArray(data.playlist) && data.playlist.length > 0) {
            activePlaylist = data.playlist;
        }
    });

    socket.on('track-ended', (endedIndex) => {
        if (activePlaylist.length === 0) return;
        currentTrackIndex = (endedIndex + 1) % activePlaylist.length;
        trackStartTime = Date.now();

        io.emit('sync-audio-track', {
            trackIndex: currentTrackIndex,
            trackStartTime: trackStartTime,
            url: activePlaylist[currentTrackIndex].url,
            title: activePlaylist[currentTrackIndex].title
        });
    });

    // 5. Gửi Thương (Hearts) Realtime
    socket.on('send-heart', (data) => {
        totalHearts++;
        const senderName = data.userName || socket.userName || 'Lữ khách';
        io.emit('heart-broadcast', {
            seatId: data.seatId,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: totalHearts
        });
    });

    // 6. Thả Hoa Đăng Realtime
    socket.on('send-lantern', (data) => {
        const word = data.word || 'An yên';
        if (!lanternCounts[word]) {
            lanternCounts[word] = 0;
        }
        lanternCounts[word]++;
        const senderName = data.userName || socket.userName || 'Lữ khách';

        io.emit('lantern-broadcast', {
            seatId: data.seatId,
            userName: senderName,
            word: word,
            senderSocketId: socket.id,
            lanternCounts: lanternCounts
        });
    });

    // 7. Khi User ngắt kết nối
    socket.on('disconnect', () => {
        let freedSeat = false;
        seats = seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                freedSeat = true;
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        if (freedSeat) {
            io.emit('seat-state-changed', seats);
        }
        io.emit('update-online-count', io.engine.clientsCount);
    });

    io.emit('update-online-count', io.engine.clientsCount);
});

setInterval(() => {
    if (activePlaylist.length > 0) {
        io.emit('audio-sync-heartbeat', {
            trackIndex: currentTrackIndex,
            trackStartTime: trackStartTime,
            url: activePlaylist[currentTrackIndex]?.url || '',
            title: activePlaylist[currentTrackIndex]?.title || ''
        });
    }
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Zen Pond Realtime Server v6.5.0 running on port ${PORT}`);
});
