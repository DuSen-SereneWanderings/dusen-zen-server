/**
 * Du Sen Zen Pond - Realtime Multi-Room Server (v10.0.2 - Clean Hybrid Server)
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

function createRoomState() {
    return {
        seats: Array.from({ length: MAX_SEATS }, (_, i) => ({
            id: i + 1,
            occupiedBy: null
        })),
        totalHearts: 0,
        lanternCounts: {
            'An yên': 0,
            'Bình an': 0,
            'Biết ơn': 0
        },
        currentTrackIndex: 0,
        trackStartTime: Date.now(),
        activePlaylist: []
    };
}

const room1State = createRoomState();
const room2State = createRoomState();

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

function setupRoomEvents(targetIo, state) {
    function getExactOnlineCount() {
        return targetIo.sockets.size;
    }

    targetIo.on('connection', (socket) => {
        socket.on('request-entry', (userObj) => {
            if (userObj && userObj.name) {
                socket.userName = userObj.name;
                socket.userAvatar = userObj.avatar || '';
            } else {
                socket.userName = 'Lữ khách';
            }

            const occupiedCount = state.seats.filter(s => s.occupiedBy !== null).length;
            const availableSeats = MAX_SEATS - occupiedCount;
            const isFull = availableSeats <= 0;

            socket.emit('entry-status-checked', {
                isFull: isFull,
                availableSeats: availableSeats,
                maxSeats: MAX_SEATS,
                seats: state.seats
            });
            
            targetIo.emit('update-online-count', getExactOnlineCount());
        });

        socket.emit('init-state', {
            seats: state.seats,
            totalHearts: state.totalHearts,
            lanternCounts: state.lanternCounts,
            audioState: {
                trackIndex: state.currentTrackIndex,
                trackStartTime: state.trackStartTime,
                url: state.activePlaylist[state.currentTrackIndex]?.url || '',
                title: state.activePlaylist[state.currentTrackIndex]?.title || ''
            }
        });

        socket.on('claim-seat', (seatId) => {
            state.seats = state.seats.map(s => {
                if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                    return { ...s, occupiedBy: null };
                }
                return s;
            });

            const targetIndex = state.seats.findIndex(s => s.id === seatId);
            if (targetIndex !== -1 && !state.seats[targetIndex].occupiedBy) {
                state.seats[targetIndex].occupiedBy = {
                    socketId: socket.id,
                    name: socket.userName || 'Lữ khách',
                    avatar: socket.userAvatar || ''
                };
            }

            targetIo.emit('seat-state-changed', state.seats);
        });

        socket.on('sync-playlist', (data) => {
            if (data && Array.isArray(data.playlist) && data.playlist.length > 0) {
                state.activePlaylist = data.playlist;
            }
        });

        socket.on('track-ended', (endedIndex) => {
            if (state.activePlaylist.length === 0) return;
            state.currentTrackIndex = (endedIndex + 1) % state.activePlaylist.length;
            state.trackStartTime = Date.now();

            targetIo.emit('sync-audio-track', {
                trackIndex: state.currentTrackIndex,
                trackStartTime: state.trackStartTime,
                url: state.activePlaylist[state.currentTrackIndex].url,
                title: state.activePlaylist[state.currentTrackIndex].title
            });
        });

        socket.on('send-heart', (data) => {
            state.totalHearts++;
            const senderName = data.userName || socket.userName || 'Lữ khách';
            targetIo.emit('heart-broadcast', {
                seatId: data.seatId,
                userName: senderName,
                senderSocketId: socket.id,
                totalHearts: state.totalHearts
            });
        });

        socket.on('send-lantern', (data) => {
            const word = data.word || 'An yên';
            if (!state.lanternCounts[word]) {
                state.lanternCounts[word] = 0;
            }
            state.lanternCounts[word]++;
            const senderName = data.userName || socket.userName || 'Lữ khách';

            targetIo.emit('lantern-broadcast', {
                seatId: data.seatId,
                userName: senderName,
                word: word,
                senderSocketId: socket.id,
                lanternCounts: state.lanternCounts
            });
        });

        socket.on('dzp_send_wish', (data) => {
            const wishText = (data && data.text) ? String(data.text).trim() : '';
            if (!wishText) return;

            targetIo.emit('dzp_receive_wish', {
                text: wishText,
                senderSocketId: socket.id,
                timestamp: Date.now()
            });
        });

        socket.on('disconnect', () => {
            let freedSeat = false;
            state.seats = state.seats.map(s => {
                if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                    freedSeat = true;
                    return { ...s, occupiedBy: null };
                }
                return s;
            });

            if (freedSeat) {
                targetIo.emit('seat-state-changed', state.seats);
            }
            targetIo.emit('update-online-count', getExactOnlineCount());
        });

        targetIo.emit('update-online-count', getExactOnlineCount());
    });
}

setupRoomEvents(io, room1State);

const room2Nsp = io.of('/room-2');
setupRoomEvents(room2Nsp, room2State);

setInterval(() => {
    if (room1State.activePlaylist.length > 0) {
        io.emit('audio-sync-heartbeat', {
            trackIndex: room1State.currentTrackIndex,
            trackStartTime: room1State.trackStartTime,
            url: room1State.activePlaylist[room1State.currentTrackIndex]?.url || '',
            title: room1State.activePlaylist[room1State.currentTrackIndex]?.title || ''
        });
    }
    if (room2State.activePlaylist.length > 0) {
        room2Nsp.emit('audio-sync-heartbeat', {
            trackIndex: room2State.currentTrackIndex,
            trackStartTime: room2State.trackStartTime,
            url: room2State.activePlaylist[room2State.currentTrackIndex]?.url || '',
            title: room2State.activePlaylist[room2State.currentTrackIndex]?.title || ''
        });
    }
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Zen Pond Clean Server running on port ${PORT} (Room 1 at root, Room 2 at /room-2)`);
});
