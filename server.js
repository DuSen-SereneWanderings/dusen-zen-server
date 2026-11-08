/**
 * Du Sen Zen Pond - Realtime Socket.io Server
 * v10.3.4 - Robust Seat Claim & Dynamic ID Parsing & Multi-attribute User Fix
 * GitHub Repo Server code for Render deployment
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// 1. CẤU HÌNH SỐ LƯỢNG CHỖ NGỒI THEO TỪNG PHÒNG
const ROOM_SEATS_LIMIT = {
    'main': 17,
    'room_2': 37
};

// LƯU TRỮ TRẠNG THÁI THEO TỪNG ROOM ID
const rooms = {};

function getRoomMaxSeats(roomId) {
    const rId = roomId || 'main';
    return ROOM_SEATS_LIMIT[rId] || 17;
}

function getRoomData(roomId) {
    const rId = roomId || 'main';

    if (!rooms[rId]) {
        const maxSeats = getRoomMaxSeats(rId);

        rooms[rId] = {
            maxSeats: maxSeats,
            seats: Array.from({ length: maxSeats }, (_, i) => ({
                id: i + 1,
                occupiedBy: null
            })),
            totalHearts: 0,
            lanternCounts: {
                'An yên': 0,
                'Bình an': 0,
                'Biết ơn': 0
            },
            sessionKey: null,
            currentTrackIndex: 0,
            trackStartTime: Date.now(),
            activePlaylist: []
        };
    }

    return rooms[rId];
}

function getRoomOnlineCount(roomId) {
    const rId = roomId || 'main';
    const roomAdapter = io.sockets.adapter.rooms.get(rId);
    return roomAdapter ? roomAdapter.size : 0;
}

function getSessionStartTimestamp(sessionKey) {
    if (typeof sessionKey !== 'string') return null;
    const parts = sessionKey.split('|');
    if (parts.length !== 4) return null;

    const dateMatch = parts[1].match(/^(\d{4})-(\d{2})-(\d{2})$/);
    const timeMatch = parts[2].match(/^(\d{2}):(\d{2})$/);
    if (!dateMatch || !timeMatch) return null;

    const year = parseInt(dateMatch[1], 10);
    const month = parseInt(dateMatch[2], 10);
    const day = parseInt(dateMatch[3], 10);
    const hour = parseInt(timeMatch[1], 10);
    const minute = parseInt(timeMatch[2], 10);

    if (month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return null;
    }

    return Date.UTC(year, month - 1, day, hour, minute);
}

function applyRoomSession(roomId, sessionKey) {
    const rId = roomId || 'main';
    if (typeof sessionKey !== 'string' || !sessionKey.trim()) return false;

    const normalizedKey = sessionKey.trim();
    const parts = normalizedKey.split('|');
    if (parts.length !== 4 || parts[0] !== rId) return false;

    const incomingTimestamp = getSessionStartTimestamp(normalizedKey);
    if (incomingTimestamp === null) return false;

    const room = getRoomData(rId);
    if (room.sessionKey === normalizedKey) return false;

    if (room.sessionKey) {
        const currentTimestamp = getSessionStartTimestamp(room.sessionKey);
        if (currentTimestamp !== null && incomingTimestamp < currentTimestamp) {
            return false;
        }
    }

    room.sessionKey = normalizedKey;
    room.totalHearts = 0;
    Object.keys(room.lanternCounts).forEach(word => {
        room.lanternCounts[word] = 0;
    });

    io.to(rId).emit('session-counters-reset', {
        sessionKey: normalizedKey,
        totalHearts: 0,
        lanternCounts: { ...room.lanternCounts }
    });

    console.log(`[Zen Pond] New session: ${rId} | ${normalizedKey} | Counters reset`);
    return true;
}

// 4. SOCKET CONNECTION
io.on('connection', (socket) => {
    let currentRoomId = socket.handshake.query.roomId || 'main';
    socket.roomId = currentRoomId;
    socket.join(currentRoomId);

    function syncSocketRoom(incomingRoomId) {
        if (incomingRoomId && incomingRoomId !== socket.roomId) {
            socket.leave(socket.roomId);
            socket.roomId = incomingRoomId;
            socket.join(incomingRoomId);
        }
    }

    // 5. NHẬN YÊU CẦU VÀO KHÔNG GIAN
    socket.on('request-entry', (data) => {
        const userObj = typeof data === 'object' && data !== null ? data : {};
        const targetRoomId = userObj.roomId || socket.roomId || 'main';
        syncSocketRoom(targetRoomId);

        socket.userName = userObj.name || userObj.userName || 'Lữ khách';
        socket.userAvatar = userObj.avatar || userObj.userAvatar || '';

        applyRoomSession(socket.roomId, userObj.sessionKey);

        const currentRoom = getRoomData(socket.roomId);
        const occupiedCount = currentRoom.seats.filter(s => s.occupiedBy !== null).length;
        const availableSeats = currentRoom.maxSeats - occupiedCount;
        const isFull = availableSeats <= 0;

        socket.emit('entry-status-checked', {
            isFull: isFull,
            availableSeats: availableSeats,
            maxSeats: currentRoom.maxSeats,
            seats: currentRoom.seats
        });

        socket.emit('init-state', {
            seats: currentRoom.seats,
            totalHearts: currentRoom.totalHearts,
            lanternCounts: currentRoom.lanternCounts,
            audioState: {
                trackIndex: currentRoom.currentTrackIndex,
                trackStartTime: currentRoom.trackStartTime,
                url: currentRoom.activePlaylist[currentRoom.currentTrackIndex]?.url || '',
                title: currentRoom.activePlaylist[currentRoom.currentTrackIndex]?.title || ''
            }
        });

        io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
    });

    socket.on('session-sync', (data) => {
        if (!data || !data.sessionKey) return;
        if (data.roomId) syncSocketRoom(data.roomId);
        applyRoomSession(socket.roomId, data.sessionKey);
    });

    // 7. ĐĂNG KÝ NHẬN VỊ TRÍ (CLAIM SEAT) - ĐÃ ĐƯỢC TỐI ƯU HÓA HOÀN TOÀN
    socket.on('claim-seat', (data) => {
        const targetRoomId = (typeof data === 'object' && data.roomId) ? data.roomId : socket.roomId;
        syncSocketRoom(targetRoomId);

        const rawSeatId = typeof data === 'object' ? data.seatId : data;
        
        // Trích xuất số nguyên kể cả khi client gửi "seat-1", "seat_5", hoặc số 1
        let seatId = NaN;
        if (typeof rawSeatId === 'number') {
            seatId = rawSeatId;
        } else if (typeof rawSeatId === 'string') {
            const extractedNum = rawSeatId.replace(/\D/g, '');
            if (extractedNum !== '') {
                seatId = parseInt(extractedNum, 10);
            }
        }

        if (isNaN(seatId)) {
            console.warn(`[Zen Pond] Warning: Invalid seatId received:`, rawSeatId);
            return;
        }

        // Đồng bộ dữ liệu tên & avatar nếu Client gửi kèm trong claim-seat
        if (typeof data === 'object') {
            if (data.name || data.userName) socket.userName = data.name || data.userName;
            if (data.avatar || data.userAvatar) socket.userAvatar = data.avatar || data.userAvatar;
        }

        const currentRoom = getRoomData(socket.roomId);

        // 1. Giải phóng ghế cũ của chính Client này trong phòng hiện tại
        currentRoom.seats = currentRoom.seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        // 2. Tìm ghế mục tiêu (Hỗ trợ cả 1-indexed id:1..N và 0-indexed index:0..N-1)
        let targetIndex = currentRoom.seats.findIndex(s => s.id === seatId);
        if (targetIndex === -1 && seatId >= 0 && seatId < currentRoom.seats.length) {
            targetIndex = seatId; // Fallback nếu client gửi mảng số thứ tự 0-indexed
        }

        if (targetIndex !== -1 && !currentRoom.seats[targetIndex].occupiedBy) {
            currentRoom.seats[targetIndex].occupiedBy = {
                socketId: socket.id,
                name: socket.userName || 'Lữ khách',
                avatar: socket.userAvatar || ''
            };
            console.log(`[Zen Pond] Seat claimed: Seat ${currentRoom.seats[targetIndex].id} in room '${socket.roomId}' by '${socket.userName}' (${socket.id})`);
        } else {
            console.warn(`[Zen Pond] Claim failed: Seat ${seatId} in room '${socket.roomId}' is occupied or not found.`);
        }

        // 3. Broadcast lại mảng ghế mới cho TOÀN BỘ Client trong phòng
        io.to(socket.roomId).emit('seat-state-changed', currentRoom.seats);
    });

    socket.on('sync-playlist', (data) => {
        if (data && data.roomId) syncSocketRoom(data.roomId);
        const currentRoom = getRoomData(socket.roomId);
        if (data && Array.isArray(data.playlist) && data.playlist.length > 0) {
            currentRoom.activePlaylist = data.playlist;
        }
    });

    socket.on('track-ended', (data) => {
        if (data && data.roomId) syncSocketRoom(data.roomId);
        const currentRoom = getRoomData(socket.roomId);
        const endedIndex = typeof data === 'object' ? data.trackIndex : data;
        if (currentRoom.activePlaylist.length === 0) return;

        currentRoom.currentTrackIndex = (endedIndex + 1) % currentRoom.activePlaylist.length;
        currentRoom.trackStartTime = Date.now();

        io.to(socket.roomId).emit('sync-audio-track', {
            trackIndex: currentRoom.currentTrackIndex,
            trackStartTime: currentRoom.trackStartTime,
            url: currentRoom.activePlaylist[currentRoom.currentTrackIndex].url,
            title: currentRoom.activePlaylist[currentRoom.currentTrackIndex].title
        });
    });

    socket.on('send-heart', (data) => {
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (data && data.sessionKey) applyRoomSession(socket.roomId, data.sessionKey);

        const currentRoom = getRoomData(socket.roomId);
        currentRoom.totalHearts++;

        const senderName = (data && (data.userName || data.name)) || socket.userName || 'Lữ khách';

        io.to(socket.roomId).emit('heart-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: currentRoom.totalHearts
        });
    });

    socket.on('send-lantern', (data) => {
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (data && data.sessionKey) applyRoomSession(socket.roomId, data.sessionKey);

        const currentRoom = getRoomData(socket.roomId);
        const word = (data && data.word) || 'An yên';

        if (currentRoom.lanternCounts[word] === undefined) {
            currentRoom.lanternCounts[word] = 0;
        }
        currentRoom.lanternCounts[word]++;

        const senderName = (data && (data.userName || data.name)) || socket.userName || 'Lữ khách';

        io.to(socket.roomId).emit('lantern-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            word: word,
            senderSocketId: socket.id,
            lanternCounts: currentRoom.lanternCounts
        });
    });

    socket.on('dzp_send_wish', (data) => {
        if (data && data.roomId) syncSocketRoom(data.roomId);
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;

        io.to(socket.roomId).emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    socket.on('disconnect', () => {
        const currentRoom = getRoomData(socket.roomId);
        let freedSeat = false;

        currentRoom.seats = currentRoom.seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                freedSeat = true;
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        if (freedSeat) {
            io.to(socket.roomId).emit('seat-state-changed', currentRoom.seats);
        }

        io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
    });

    io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
});

setInterval(() => {
    Object.keys(rooms).forEach(rId => {
        const room = rooms[rId];
        if (room.activePlaylist && room.activePlaylist.length > 0) {
            io.to(rId).emit('audio-sync-heartbeat', {
                trackIndex: room.currentTrackIndex,
                trackStartTime: room.trackStartTime,
                url: room.activePlaylist[room.currentTrackIndex]?.url || '',
                title: room.activePlaylist[room.currentTrackIndex]?.title || ''
            });
        }
    });
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Zen Pond Realtime Server v10.3.4 running on port ${PORT}`);
});
