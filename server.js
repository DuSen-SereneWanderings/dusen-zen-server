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

let seatsState = Array.from({ length: MAX_SEATS }, (_, i) => {
    const seatId = i + 1;
    const angle = (i * 2 * Math.PI) / MAX_SEATS;
    const radiusX = (seatId % 2 !== 0) ? 280 : 200;
    const radiusY = (seatId % 2 !== 0) ? 140 : 100;
    return {
        id: seatId,
        x: Math.round(radiusX * Math.cos(angle)),
        y: Math.round(radiusY * Math.sin(angle)),
        occupiedBy: null
    };
});

// BỘ DỮ LIỆU BÀI HÁT VÀ MASTER CLOCK
let audioState = {
    playlist: [],
    trackIndex: 0,
    url: '',
    title: 'Chưa chọn bài phát',
    trackStartTime: Date.now()
};

let activeUsers = new Map();

// PHÁT XUNG HEARTBEAT MỖI 4 GIÂY ĐỂ ĐỒNG BỘ ÂM THANH TUYỆT ĐỐI TOÀN PHÒNG
setInterval(() => {
    if (audioState.url) {
        io.emit('audio-sync-heartbeat', {
            trackIndex: audioState.trackIndex,
            url: audioState.url,
            title: audioState.title,
            trackStartTime: audioState.trackStartTime,
            serverTime: Date.now()
        });
    }
}, 4000);

io.on('connection', (socket) => {
    activeUsers.set(socket.id, { socketId: socket.id, seatId: null, name: 'Lữ khách' });

    socket.emit('init-state', {
        maxSeats: MAX_SEATS,
        seats: seatsState,
        onlineCount: activeUsers.size,
        totalHearts: totalHearts,
        audioState: {
            url: audioState.url,
            title: audioState.title,
            trackIndex: audioState.trackIndex,
            trackStartTime: audioState.trackStartTime,
            serverTime: Date.now(),
            playlist: audioState.playlist
        }
    });

    io.emit('update-online-count', activeUsers.size);

    socket.on('sync-playlist', (data) => {
        if (data.playlist && Array.isArray(data.playlist) && data.playlist.length > 0) {
            const isFirstLoad = (audioState.playlist.length === 0 || !audioState.url);
            audioState.playlist = data.playlist;

            if (isFirstLoad && data.playlist[0]) {
                audioState.url = data.playlist[0].url;
                audioState.title = data.playlist[0].title || 'Bài 1';
                audioState.trackIndex = 0;
                audioState.trackStartTime = Date.now();

                io.emit('sync-audio-track', {
                    url: audioState.url,
                    title: audioState.title,
                    trackIndex: 0,
                    trackStartTime: audioState.trackStartTime,
                    serverTime: Date.now()
                });
            }
        }
    });

    socket.on('track-ended', (endedIndex) => {
        if (audioState.playlist.length > 0 && endedIndex === audioState.trackIndex) {
            audioState.trackIndex = (audioState.trackIndex + 1) % audioState.playlist.length;
            const nextTrack = audioState.playlist[audioState.trackIndex];
            if (nextTrack) {
                audioState.url = nextTrack.url;
                audioState.title = nextTrack.title || `Bài ${audioState.trackIndex + 1}`;
                audioState.trackStartTime = Date.now();

                io.emit('sync-audio-track', {
                    url: audioState.url,
                    title: audioState.title,
                    trackIndex: audioState.trackIndex,
                    trackStartTime: audioState.trackStartTime,
                    serverTime: Date.now()
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
