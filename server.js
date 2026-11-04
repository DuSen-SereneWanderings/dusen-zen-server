/**
 * Du Sen Zen Pond - Realtime Socket.io Server (v10.0.0 - 3 Sessions Schedule & GMT+7 Timezone Support)
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

// Hàm hỗ trợ trích xuất Giờ & Phút chuẩn múi giờ Việt Nam (Asia/Ho_Chi_Minh)
function getVietnamTimeObj() {
    try {
        const now = new Date();
        const options = { timeZone: 'Asia/Ho_Chi_Minh', hour: 'numeric', minute: 'numeric', second: 'numeric', hour12: false };
        const formatter = new Intl.DateTimeFormat('en-US', options);
        const parts = formatter.formatToParts(now);
        let h = 0, m = 0, s = 0;
        parts.forEach(p => {
            if (p.type === 'hour') h = parseInt(p.value, 10);
            if (p.type === 'minute') m = parseInt(p.value, 10);
            if (p.type === 'second') s = parseInt(p.value, 10);
        });
        if (h === 24) h = 0;
        return { hour: h, minute: m, second: s };
    } catch(e) {
        const now = new Date();
        return { hour: now.getHours(), minute: now.getMinutes(), second: now.getSeconds() };
    }
}

function getExactOnlineCount() {
    return io.of("/").sockets.size;
}

io.on('connection', (socket) => {
    // 1. Ghi nhớ thông tin User khi vào
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
        
        io.emit('update-online-count', getExactOnlineCount());
    });

    // 2. Gửi trạng thái khởi tạo cho Client
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

    // 3. Đăng ký nhận vị trí (Claim Seat)
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

    // 4. Đồng bộ Nhạc Thiền
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

    // 5. Gửi Thương Realtime
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

    // 7. Phát sóng Thiệp Tâm Nguyện Ẩn Danh Realtime cho tất cả Lữ Khách
    socket.on('dzp_send_wish', (data) => {
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;

        io.emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    // 8. Ngắt kết nối & Cập nhật số lữ khách chính xác
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
        io.emit('update-online-count', getExactOnlineCount());
    });

    io.emit('update-online-count', getExactOnlineCount());
});

// Định kỳ phát sóng đồng bộ nhạc
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
    console.log(`Zen Pond Realtime Server v10.0.0 running on port ${PORT} (GMT+7 Supported)`);
});
