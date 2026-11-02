const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const MAX_SEATS = 17;
let totalHearts = 0;

// Tự động tính toán tọa độ 17 vị trí hòn đá thiền xoay quanh hồ theo dạng so le 2 vòng
let seatsState = [];
for (let i = 1; i <= MAX_SEATS; i++) {
    const angle = ((i - 1) * 2 * Math.PI) / MAX_SEATS;
    const radiusX = (i % 2 !== 0) ? 280 : 200;
    const radiusY = (i % 2 !== 0) ? 140 : 100;

    seatsState.push({
        id: i,
        x: Math.round(radiusX * Math.cos(angle)),
        y: Math.round(radiusY * Math.sin(angle)),
        occupiedBy: null
    });
}

// BỘ ĐỒNG BỘ ÂM THANH THỜI GIAN THỰC TOÀN PHÒNG
let audioState = {
    playlist: [],
    trackIndex: 0,
    url: '',
    title: 'Chưa chọn bài phát',
    startTime: Date.now()
};

let activeUsers = new Map();

io.on('connection', (socket) => {
    activeUsers.set(socket.id, { socketId: socket.id, seatId: null, name: 'Lữ khách' });

    // Tính chính xác số giây bài hát đang phát dở tại thời điểm user mới vào phòng
    const elapsedSeconds = audioState.url ? (Date.now() - audioState.startTime) / 1000 : 0;

    socket.emit('init-state', {
        maxSeats: MAX_SEATS,
        seats: seatsState,
        onlineCount: activeUsers.size,
        totalHearts: totalHearts,
        audioState: {
            url: audioState.url,
            title: audioState.title,
            trackIndex: audioState.trackIndex,
            currentTime: elapsedSeconds,
            playlist: audioState.playlist
        }
    });

    io.emit('update-online-count', activeUsers.size);

    // Đồng bộ Playlist từ WordPress lên Server
    socket.on('sync-playlist', (data) => {
        if (data.playlist && Array.isArray(data.playlist) && data.playlist.length > 0) {
            audioState.playlist = data.playlist;
            if (!audioState.url && data.playlist[0]) {
                audioState.url = data.playlist[0].url;
                audioState.title = data.playlist[0].title || 'Bài 1';
                audioState.trackIndex = 0;
                audioState.startTime = Date.now();

                io.emit('sync-audio-track', {
                    url: audioState.url,
                    title: audioState.title,
                    trackIndex: 0,
                    currentTime: 0
                });
            }
        }
    });

    // Xử lý tự động chuyển bài đồng bộ cho cả phòng khi kết thúc bài
    socket.on('track-ended', (endedIndex) => {
        if (audioState.playlist.length > 0 && endedIndex === audioState.trackIndex) {
            audioState.trackIndex = (audioState.trackIndex + 1) % audioState.playlist.length;
            const nextTrack = audioState.playlist[audioState.trackIndex];
            if (nextTrack) {
                audioState.url = nextTrack.url;
                audioState.title = nextTrack.title || `Bài ${audioState.trackIndex + 1}`;
                audioState.startTime = Date.now();

                io.emit('sync-audio-track', {
                    url: audioState.url,
                    title: audioState.title,
                    trackIndex: audioState.trackIndex,
                    currentTime: 0
                });
            }
        }
    });

    socket.on('request-entry', (userData) => {
        const user = activeUsers.get(socket.id);
        if (user) {
            user.name = userData.name || 'Lữ khách';
            user.avatar = userData.avatar || '';
        }

        const occupiedCount = seatsState.filter(s => s.occupiedBy !== null).length;
        
        socket.emit('entry-status-checked', {
            maxSeats: MAX_SEATS,
            occupiedCount: occupiedCount,
            availableSeats: MAX_SEATS - occupiedCount,
            isFull: occupiedCount >= MAX_SEATS,
            seats: seatsState
        });
    });

    socket.on('claim-seat', (seatId) => {
        const targetSeat = seatsState.find(s => s.id === seatId);
        const user = activeUsers.get(socket.id);

        if (targetSeat && !targetSeat.occupiedBy && user) {
            if (user.seatId) {
                const prevSeat = seatsState.find(s => s.id === user.seatId);
                if (prevSeat) prevSeat.occupiedBy = null;
            }

            targetSeat.occupiedBy = {
                socketId: socket.id,
                name: user.name,
                avatar: user.avatar
            };
            user.seatId = seatId;

            io.emit('seat-state-changed', seatsState);
        }
    });

    socket.on('send-heart', (data) => {
        totalHearts++;
        const user = activeUsers.get(socket.id);
        const senderName = (user && user.name) ? user.name : (data && data.userName ? data.userName : 'Lữ khách');
        const senderSeatId = (user && user.seatId) ? user.seatId : (data && data.seatId ? data.seatId : null);

        io.emit('heart-broadcast', {
            seatId: senderSeatId,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: totalHearts
        });
    });

    socket.on('disconnect', () => {
        const user = activeUsers.get(socket.id);
        if (user && user.seatId) {
            const seat = seatsState.find(s => s.id === user.seatId);
            if (seat) seat.occupiedBy = null;
        }
        activeUsers.delete(socket.id);

        io.emit('seat-state-changed', seatsState);
        io.emit('update-online-count', activeUsers.size);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server Realtime running on port ${PORT}`);
});
