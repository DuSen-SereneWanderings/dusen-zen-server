/**
 * Du Sen Zen Pond - Realtime Socket.io Server (v10.0.3 - Full Multi-Room Support)
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

// LƯU TRỮ TRẠNG THÁI THEO TỪNG ROOM ID
const rooms = {};

// Hàm lấy hoặc khởi tạo dữ liệu cho một phòng
function getRoomData(roomId) {
    const rId = roomId || 'main';
    if (!rooms[rId]) {
        rooms[rId] = {
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
    return rooms[rId];
}

// Đếm số người online chính xác theo từng Room
function getRoomOnlineCount(roomId) {
    const rId = roomId || 'main';
    const roomAdapter = io.sockets.adapter.rooms.get(rId);
    return roomAdapter ? roomAdapter.size : 0;
}

io.on('connection', (socket) => {
    // 1. Trích xuất roomId từ Client (mặc định 'main')
    const roomId = socket.handshake.query.roomId || 'main';
    socket.roomId = roomId;
    socket.join(roomId);

    const room = getRoomData(roomId);

    // 2. Ghi nhớ thông tin User khi vào phòng
    socket.on('request-entry', (data) => {
        const userObj = typeof data === 'object' ? data : {};
        if (userObj && userObj.name) {
            socket.userName = userObj.name;
            socket.userAvatar = userObj.avatar || '';
        } else {
            socket.userName = 'Lữ khách';
        }

        const occupiedCount = room.seats.filter(s => s.occupiedBy !== null).length;
        const availableSeats = MAX_SEATS - occupiedCount;
        const isFull = availableSeats <= 0;

        socket.emit('entry-status-checked', {
            isFull: isFull,
            availableSeats: availableSeats,
            maxSeats: MAX_SEATS,
            seats: room.seats
        });
        
        io.to(roomId).emit('update-online-count', getRoomOnlineCount(roomId));
    });

    // 3. Gửi trạng thái khởi tạo cho Client vừa kết nối
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

    // 4. Đăng ký nhận vị trí (Claim Seat)
    socket.on('claim-seat', (data) => {
        const seatId = typeof data === 'object' ? data.seatId : data;
        const currentRoom = getRoomData(socket.roomId);

        currentRoom.seats = currentRoom.seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        const targetIndex = currentRoom.seats.findIndex(s => s.id === seatId);
        if (targetIndex !== -1 && !currentRoom.seats[targetIndex].occupiedBy) {
            currentRoom.seats[targetIndex].occupiedBy = {
                socketId: socket.id,
                name: socket.userName || 'Lữ khách',
                avatar: socket.userAvatar || ''
            };
        }

        io.to(socket.roomId).emit('seat-state-changed', currentRoom.seats);
    });

    // 5. Đồng bộ Nhạc Thiền theo Room
    socket.on('sync-playlist', (data) => {
        const currentRoom = getRoomData(socket.roomId);
        if (data && Array.isArray(data.playlist) && data.playlist.length > 0) {
            currentRoom.activePlaylist = data.playlist;
        }
    });

    socket.on('track-ended', (data) => {
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

    // 6. Gửi Thương Realtime theo Room
    socket.on('send-heart', (data) => {
        const currentRoom = getRoomData(socket.roomId);
        currentRoom.totalHearts++;
        const senderName = (data && data.userName) || socket.userName || 'Lữ khách';

        io.to(socket.roomId).emit('heart-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: currentRoom.totalHearts
        });
    });

    // 7. Thả Hoa Đăng Realtime theo Room
    socket.on('send-lantern', (data) => {
        const currentRoom = getRoomData(socket.roomId);
        const word = (data && data.word) || 'An yên';
        if (!currentRoom.lanternCounts[word]) {
            currentRoom.lanternCounts[word] = 0;
        }
        currentRoom.lanternCounts[word]++;
        const senderName = (data && data.userName) || socket.userName || 'Lữ khách';

        io.to(socket.roomId).emit('lantern-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            word: word,
            senderSocketId: socket.id,
            lanternCounts: currentRoom.lanternCounts
        });
    });

    // 8. Phát sóng Lá Tâm Nguyệt Realtime theo Room
    socket.on('dzp_send_wish', (data) => {
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;

        io.to(socket.roomId).emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    // 9. Ngắt kết nối & Cập nhật vị trí ghế và số người online trong Room
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

    io.to(roomId).emit('update-online-count', getRoomOnlineCount(roomId));
});

// Định kỳ phát sóng đồng bộ nhạc theo từng Room riêng biệt
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
    console.log(`Zen Pond Realtime Server v10.0.3 running on port ${PORT} (Multi-Room Supported)`);
});
