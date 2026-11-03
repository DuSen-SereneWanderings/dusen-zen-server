/**
 * Du Sen Zen Pond - Realtime Socket.io Server (v9.1.0 Multi-Room Support)
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

// Quản lý bộ nhớ các phòng thiền độc lập
const rooms = new Map();

/**
 * Trích xuất hoặc khởi tạo thông tin cho phòng theo roomId
 */
function getOrCreateRoom(roomId) {
    if (!rooms.has(roomId)) {
        rooms.set(roomId, {
            id: roomId,
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
        });
    }
    return rooms.get(roomId);
}

/**
 * Lấy số lượng client online thực tế trong phòng
 */
function getRoomOnlineCount(roomId) {
    const roomSockets = io.sockets.adapter.rooms.get(roomId);
    return roomSockets ? roomSockets.size : 0;
}

io.on('connection', (socket) => {
    
    // 1. Ghi nhớ thông tin User và tham gia phòng (Room Join)
    socket.on('request-entry', (payload) => {
        const roomId = (payload && payload.roomId) ? String(payload.roomId) : 'phong-mac-dinh';
        const userObj = (payload && payload.user) ? payload.user : payload;

        socket.roomId = roomId;
        socket.join(roomId);

        if (userObj && userObj.name) {
            socket.userName = userObj.name;
            socket.userAvatar = userObj.avatar || '';
        } else {
            socket.userName = 'Lữ khách';
        }

        const room = getOrCreateRoom(roomId);
        const occupiedCount = room.seats.filter(s => s.occupiedBy !== null).length;
        const availableSeats = MAX_SEATS - occupiedCount;
        const isFull = availableSeats <= 0;

        // Trả trạng thái phòng riêng cho user kết nối
        socket.emit('entry-status-checked', {
            isFull: isFull,
            availableSeats: availableSeats,
            maxSeats: MAX_SEATS,
            seats: room.seats
        });

        // Trả trạng thái khởi tạo riêng cho user kết nối
        socket.emit('init-state', {
            seats: room.seats,
            totalHearts: room.totalHearts,
            lanternCounts: room.lanternCounts,
            audioState: {
                trackIndex: room.currentTrackIndex,
                trackStartTime: room.trackStartTime,
                url: room.activePlaylist[room.currentTrackIndex]?.url || '',
                title: room.activePlaylist[room.currentTrackIndex]?.title || ''
            }
        });

        // Phát sóng số người online cho duy nhất phòng đó
        io.to(roomId).emit('update-online-count', getRoomOnlineCount(roomId));
    });

    // 2. Đăng ký vị trí (Claim Seat) theo Room
    socket.on('claim-seat', (payload) => {
        const roomId = (payload && payload.roomId) ? payload.roomId : (socket.roomId || 'phong-mac-dinh');
        const seatId = (typeof payload === 'object' && payload !== null) ? payload.seatId : payload;

        const room = getOrCreateRoom(roomId);

        // Giải phóng ghế cũ của socket trong phòng
        room.seats = room.seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        // Giữ vị trí ghế mới
        const targetIndex = room.seats.findIndex(s => s.id === seatId);
        if (targetIndex !== -1 && !room.seats[targetIndex].occupiedBy) {
            room.seats[targetIndex].occupiedBy = {
                socketId: socket.id,
                name: socket.userName || 'Lữ khách',
                avatar: socket.userAvatar || ''
            };
        }

        io.to(roomId).emit('seat-state-changed', room.seats);
    });

    // 3. Đồng bộ Nhạc Thiền theo Room
    socket.on('sync-playlist', (data) => {
        const roomId = (data && data.roomId) ? data.roomId : (socket.roomId || 'phong-mac-dinh');
        const playlist = (data && Array.isArray(data.playlist)) ? data.playlist : (Array.isArray(data) ? data : []);
        
        const room = getOrCreateRoom(roomId);
        if (playlist.length > 0) {
            room.activePlaylist = playlist;
        }
    });

    socket.on('track-ended', (data) => {
        const roomId = (data && data.roomId) ? data.roomId : (socket.roomId || 'phong-mac-dinh');
        const endedIndex = (typeof data === 'object' && data !== null) ? data.trackIndex : data;
        const room = getOrCreateRoom(roomId);

        if (room.activePlaylist.length === 0) return;
        room.currentTrackIndex = (endedIndex + 1) % room.activePlaylist.length;
        room.trackStartTime = Date.now();

        io.to(roomId).emit('sync-audio-track', {
            trackIndex: room.currentTrackIndex,
            trackStartTime: room.trackStartTime,
            url: room.activePlaylist[room.currentTrackIndex].url,
            title: room.activePlaylist[room.currentTrackIndex].title
        });
    });

    // 4. Gửi Thương Realtime theo Room
    socket.on('send-heart', (data) => {
        const roomId = (data && data.roomId) ? data.roomId : (socket.roomId || 'phong-mac-dinh');
        const room = getOrCreateRoom(roomId);

        room.totalHearts++;
        const senderName = (data && data.userName) || socket.userName || 'Lữ khách';

        io.to(roomId).emit('heart-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: room.totalHearts
        });
    });

    // 5. Thả Hoa Đăng Realtime theo Room
    socket.on('send-lantern', (data) => {
        const roomId = (data && data.roomId) ? data.roomId : (socket.roomId || 'phong-mac-dinh');
        const room = getOrCreateRoom(roomId);

        const word = (data && data.word) || 'An yên';
        if (!room.lanternCounts[word]) {
            room.lanternCounts[word] = 0;
        }
        room.lanternCounts[word]++;
        const senderName = (data && data.userName) || socket.userName || 'Lữ khách';

        io.to(roomId).emit('lantern-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            word: word,
            senderSocketId: socket.id,
            lanternCounts: room.lanternCounts
        });
    });

    // 6. Phát sóng Thiệp Tâm Nguyện Ẩn Danh theo Room
    socket.on('dzp_send_wish', (data) => {
        const roomId = (data && data.roomId) ? data.roomId : (socket.roomId || 'phong-mac-dinh');
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;

        io.to(roomId).emit('dzp_receive_wish', {
            roomId: roomId,
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    // 7. Ngắt kết nối & Cập nhật số lữ khách theo Room
    socket.on('disconnect', () => {
        const roomId = socket.roomId;
        if (!roomId || !rooms.has(roomId)) return;

        const room = rooms.get(roomId);
        let freedSeat = false;

        room.seats = room.seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                freedSeat = true;
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        if (freedSeat) {
            io.to(roomId).emit('seat-state-changed', room.seats);
        }

        io.to(roomId).emit('update-online-count', getRoomOnlineCount(roomId));
    });
});

// Timed Heartbeat đồng bộ nhạc cho từng phòng
setInterval(() => {
    rooms.forEach((room, roomId) => {
        if (room.activePlaylist.length > 0) {
            io.to(roomId).emit('audio-sync-heartbeat', {
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
    console.log(`Zen Pond Realtime Server v9.1.0 Multi-Room running on port ${PORT}`);
});
