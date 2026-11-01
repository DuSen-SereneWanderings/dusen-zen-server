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

// Tự động tính toán tọa độ 17 vị trí hòn đá thiền xoay quanh hồ
let seatsState = [];
for (let i = 1; i <= MAX_SEATS; i++) {
    const angle = ((i - 1) * 2 * Math.PI) / MAX_SEATS;
    seatsState.push({
        id: i,
        x: Math.round(220 * Math.cos(angle)),
        y: Math.round(110 * Math.sin(angle)),
        occupiedBy: null
    });
}

let currentTrack = {
    url: '',
    title: 'Chưa chọn bài phát',
    currentTime: 0,
    isPlaying: false,
    timestamp: Date.now()
};

let activeUsers = new Map();

io.on('connection', (socket) => {
    activeUsers.set(socket.id, { socketId: socket.id, seatId: null, name: 'Lữ khách' });

    socket.emit('init-state', {
        maxSeats: MAX_SEATS,
        seats: seatsState,
        track: currentTrack,
        onlineCount: activeUsers.size
    });

    io.emit('update-online-count', activeUsers.size);

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
