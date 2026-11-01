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

const MAX_SEATS = 8;

let seatsState = [
    { id: 1, x: -140, y: -60, occupiedBy: null },
    { id: 2, x: 0,    y: -110, occupiedBy: null },
    { id: 3, x: 140,  y: -60, occupiedBy: null },
    { id: 4, x: 200,  y: 40,  occupiedBy: null },
    { id: 5, x: 140,  y: 140, occupiedBy: null },
    { id: 6, x: 0,    y: 190, occupiedBy: null },
    { id: 7, x: -140, y: 140, occupiedBy: null },
    { id: 8, x: -200, y: 40,  occupiedBy: null }
];

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
        socket.emit('entry-granted', {
            role: occupiedCount < MAX_SEATS ? 'seated' : 'spectator',
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

    socket.on('admin-control-audio', (audioData) => {
        currentTrack = {
            ...audioData,
            timestamp: Date.now()
        };
        io.emit('sync-audio-track', currentTrack);
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
