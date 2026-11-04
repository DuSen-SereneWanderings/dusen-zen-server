/**
 * Du Sen Zen Pond - Realtime Multi-Room Server (v10.0.2 - Safe Hybrid: Root for Old Room & /room-2 for Cloned Room)
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

// Hàm khởi tạo bộ trạng thái độc lập cho từng phòng
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

// Khởi tạo 2 trạng thái riêng biệt cho 2 phòng
const room1State = createRoomState(); // Dành cho phòng cũ (chạy ở gốc /)
const room2State = createRoomState(); // Dành cho phòng mới clone (chạy ở /room-2)

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

// Hàm cài đặt toàn bộ sự kiện Socket chuẩn xác cho từng target (IO gốc hoặc Namespace)
function setupRoomEvents(targetIo, state) {
    function getExactOnlineCount() {
        return targetIo.sockets.size;
    }

    targetIo.on('connection', (socket) => {
        // 1. Ghi nhớ thông tin User khi vào phòng
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

        // 2. Gửi trạng thái khởi tạo riêng cho Client trong phòng
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

        // 3. Đăng ký nhận vị trí (Claim Seat)
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

        // 4. Đồng bộ Nhạc Thiền riêng cho phòng
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

        // 5. Gửi Thương Realtime trong phòng
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

        // 6. Thả Hoa Đăng Realtime trong phòng (hỗ trợ linh hoạt mọi từ khóa)
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

        // 7. Phát sóng Thiệp Tâm Nguyện Ẩn Danh Realtime trong phòng
        socket.on('dzp_send_wish', (data) => {
            const wishText = (data && data.text) ? String(data.text).trim() : '';
            if (!wishText) return;

            targetIo.emit('dzp_receive_wish', {
                text: wishText,
                senderSocketId: socket.id,
                timestamp: Date.now()
            });
        });

        // 8. Ngắt kết nối & Cập nhật số lữ khách online trong phòng
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

// ----------------------------------------------------
// ĐĂNG KÝ 2 PHÒNG RIÊNG BIỆT TRÊN SERVER
// ----------------------------------------------------

// 1. PHÒNG CŨ (Gốc): Gắn trực tiếp vào `io` gốc -> Plugin cũ không bị ảnh hưởng 100%
setupRoomEvents(io, room1State);

// 2. PHÒNG MỚI (CLONE): Gắn vào namespace `/room-2` -> Khớp hoàn toàn với plugin clone
const room2Nsp = io.of('/room-2');
setupRoomEvents(room2Nsp, room2State);

// ----------------------------------------------------
// ĐỊNH KỲ ĐỒNG BỘ NHẠC CHO CẢ 2 PHÒNG
// ----------------------------------------------------
setInterval(() => {
    // Phát cho phòng cũ (gốc)
    if (room1State.activePlaylist.length > 0) {
        io.emit('audio-sync-heartbeat', {
            trackIndex: room1State.currentTrackIndex,
            trackStartTime: room1State.trackStartTime,
            url: room1State.activePlaylist[room1State.currentTrackIndex]?.url || '',
            title: room1State.activePlaylist[room1State.currentTrackIndex]?.title || ''
        });
    }
    // Phát cho phòng mới (/room-2)
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
    console.log(`Zen Pond Multi-Room Server v10.0.2 running on port ${PORT} (Room 1 at root, Room 2 at /room-2)`);
});
