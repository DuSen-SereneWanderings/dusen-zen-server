
/**
 * Du Sen Zen Pond - Realtime Socket.io Server
 * v10.3.2 - Session Counters Reset & Dynamic Seats & Multi-Room Safe
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

// Hàm lấy giới hạn số chỗ của 1 phòng
function getRoomMaxSeats(roomId) {
    const rId = roomId || 'main';
    return ROOM_SEATS_LIMIT[rId] || 17;
}

// Hàm lấy hoặc khởi tạo dữ liệu cho một phòng
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

            // Phiên hoạt động hiện tại của phòng
            sessionKey: null,

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

// 2. XÁC ĐỊNH THỜI ĐIỂM BẮT ĐẦU CỦA SESSION
// Định dạng sessionKey từ Client:
// roomId|YYYY-MM-DD|HH:mm|HH:mm
//
// Ví dụ:
// main|2026-11-06|08:00|12:00
//
// Dùng thời điểm mở phiên để tránh Client cũ
// vô tình đưa phòng quay lại phiên trước.

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

    if (
        month < 1 || month > 12 ||
        day < 1 || day > 31 ||
        hour < 0 || hour > 23 ||
        minute < 0 || minute > 59
    ) {
        return null;
    }

    return Date.UTC(
        year,
        month - 1,
        day,
        hour,
        minute
    );
}

// 3. ÁP DỤNG SESSION MỚI VÀ RESET BỘ ĐẾM REALTIME
function applyRoomSession(roomId, sessionKey) {
    const rId = roomId || 'main';

    if (
        typeof sessionKey !== 'string' ||
        !sessionKey.trim()
    ) {
        return false;
    }

    const normalizedKey = sessionKey.trim();
    const parts = normalizedKey.split('|');

    // Session phải thuộc đúng phòng đang xử lý
    if (parts.length !== 4 || parts[0] !== rId) {
        return false;
    }

    const incomingTimestamp = getSessionStartTimestamp(normalizedKey);

    if (incomingTimestamp === null) {
        return false;
    }

    const room = getRoomData(rId);

    // Không reset nếu vẫn đang cùng một phiên
    if (room.sessionKey === normalizedKey) {
        return false;
    }

    // Không cho Client thuộc phiên cũ ghi đè phiên mới hơn
    if (room.sessionKey) {
        const currentTimestamp = getSessionStartTimestamp(room.sessionKey);

        if (
            currentTimestamp !== null &&
            incomingTimestamp < currentTimestamp
        ) {
            return false;
        }
    }

    // Xác nhận phiên mới
    room.sessionKey = normalizedKey;

    // RESET TỔNG THƯƠNG
    room.totalHearts = 0;

    // RESET TẤT CẢ HOA ĐĂNG
    Object.keys(room.lanternCounts).forEach(word => {
        room.lanternCounts[word] = 0;
    });

    // Thông báo cho tất cả Client đang ở trong phòng
    io.to(rId).emit('session-counters-reset', {
        sessionKey: normalizedKey,
        totalHearts: 0,
        lanternCounts: { ...room.lanternCounts }
    });

    console.log(
        `[Zen Pond] New session: ${rId} | ${normalizedKey} | Counters reset`
    );

    return true;
}

// 4. SOCKET CONNECTION
io.on('connection', (socket) => {

    // Trích xuất roomId từ Client
    const roomId = socket.handshake.query.roomId || 'main';

    socket.roomId = roomId;
    socket.join(roomId);

    const room = getRoomData(roomId);

    // 5. NHẬN YÊU CẦU VÀO KHÔNG GIAN
    socket.on('request-entry', (data) => {

        const userObj = typeof data === 'object' && data !== null
            ? data
            : {};

        if (userObj.name) {
            socket.userName = userObj.name;
            socket.userAvatar = userObj.avatar || '';
        } else {
            socket.userName = 'Lữ khách';
        }

        // Kiểm tra và áp dụng phiên hiện tại.
        // Không đồng bộ số liệu thống kê theo ngày từ PHP vào đây.
        applyRoomSession(roomId, userObj.sessionKey);

        const currentRoom = getRoomData(roomId);

        const occupiedCount = currentRoom.seats.filter(
            s => s.occupiedBy !== null
        ).length;

        const availableSeats = currentRoom.maxSeats - occupiedCount;
        const isFull = availableSeats <= 0;

        socket.emit('entry-status-checked', {
            isFull: isFull,
            availableSeats: availableSeats,
            maxSeats: currentRoom.maxSeats,
            seats: currentRoom.seats
        });

        // Gửi trạng thái khởi tạo cho Client
        socket.emit('init-state', {
            seats: currentRoom.seats,
            totalHearts: currentRoom.totalHearts,
            lanternCounts: currentRoom.lanternCounts,

            audioState: {
                trackIndex: currentRoom.currentTrackIndex,
                trackStartTime: currentRoom.trackStartTime,
                url: currentRoom.activePlaylist[
                    currentRoom.currentTrackIndex
                ]?.url || '',
                title: currentRoom.activePlaylist[
                    currentRoom.currentTrackIndex
                ]?.title || ''
            }
        });

        io.to(roomId).emit(
            'update-online-count',
            getRoomOnlineCount(roomId)
        );
    });

    // 6. ĐỒNG BỘ SESSION KHI CLIENT ĐANG MỞ TRANG
    socket.on('session-sync', (data) => {

        if (!data || !data.sessionKey) return;

        applyRoomSession(
            socket.roomId,
            data.sessionKey
        );
    });

    // 7. ĐĂNG KÝ NHẬN VỊ TRÍ (CLAIM SEAT)
    socket.on('claim-seat', (data) => {

        const seatId = typeof data === 'object'
            ? data.seatId
            : data;

        const currentRoom = getRoomData(socket.roomId);

        // Giải phóng ghế cũ của chính Client này
        currentRoom.seats = currentRoom.seats.map(s => {
            if (
                s.occupiedBy &&
                s.occupiedBy.socketId === socket.id
            ) {
                return {
                    ...s,
                    occupiedBy: null
                };
            }

            return s;
        });

        const targetIndex = currentRoom.seats.findIndex(
            s => s.id === seatId
        );

        if (
            targetIndex !== -1 &&
            !currentRoom.seats[targetIndex].occupiedBy
        ) {
            currentRoom.seats[targetIndex].occupiedBy = {
                socketId: socket.id,
                name: socket.userName || 'Lữ khách',
                avatar: socket.userAvatar || ''
            };
        }

        io.to(socket.roomId).emit(
            'seat-state-changed',
            currentRoom.seats
        );
    });

    // 8. ĐỒNG BỘ NHẠC THIỀN THEO ROOM
    socket.on('sync-playlist', (data) => {

        const currentRoom = getRoomData(socket.roomId);

        if (
            data &&
            Array.isArray(data.playlist) &&
            data.playlist.length > 0
        ) {
            currentRoom.activePlaylist = data.playlist;
        }
    });

    socket.on('track-ended', (data) => {

        const currentRoom = getRoomData(socket.roomId);

        const endedIndex = typeof data === 'object'
            ? data.trackIndex
            : data;

        if (currentRoom.activePlaylist.length === 0) return;

        currentRoom.currentTrackIndex =
            (endedIndex + 1) % currentRoom.activePlaylist.length;

        currentRoom.trackStartTime = Date.now();

        io.to(socket.roomId).emit('sync-audio-track', {
            trackIndex: currentRoom.currentTrackIndex,
            trackStartTime: currentRoom.trackStartTime,
            url: currentRoom.activePlaylist[
                currentRoom.currentTrackIndex
            ].url,
            title: currentRoom.activePlaylist[
                currentRoom.currentTrackIndex
            ].title
        });
    });

    // 9. GỬI THƯƠNG REALTIME THEO ROOM
    socket.on('send-heart', (data) => {

        // Kiểm tra Session trước khi cộng thương
        if (data && data.sessionKey) {
            applyRoomSession(
                socket.roomId,
                data.sessionKey
            );
        }

        const currentRoom = getRoomData(socket.roomId);

        // Chỉ cộng thêm 1 lượt thương.
        // Không lấy tổng tim từ PHP hoặc Client để ghi đè.
        currentRoom.totalHearts++;

        const senderName =
            (data && data.userName) ||
            socket.userName ||
            'Lữ khách';

        io.to(socket.roomId).emit('heart-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: currentRoom.totalHearts
        });
    });

    // 10. THẢ HOA ĐĂNG REALTIME THEO ROOM
    socket.on('send-lantern', (data) => {

        // Kiểm tra Session trước khi cộng hoa đăng
        if (data && data.sessionKey) {
            applyRoomSession(
                socket.roomId,
                data.sessionKey
            );
        }

        const currentRoom = getRoomData(socket.roomId);

        const word = (data && data.word) || 'An yên';

        if (currentRoom.lanternCounts[word] === undefined) {
            currentRoom.lanternCounts[word] = 0;
        }

        // Chỉ cộng thêm 1 hoa đăng
        // Không lấy tổng hoa đăng từ Client để ghi đè
        currentRoom.lanternCounts[word]++;

        const senderName =
            (data && data.userName) ||
            socket.userName ||
            'Lữ khách';

        io.to(socket.roomId).emit('lantern-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            word: word,
            senderSocketId: socket.id,
            lanternCounts: currentRoom.lanternCounts
        });
    });

    // 11. PHÁT SÓNG LÁ TÂM NGUYỆN REALTIME THEO ROOM
    socket.on('dzp_send_wish', (data) => {

        const wishText = (data && data.text)
            ? String(data.text).trim()
            : '';

        if (!wishText) return;

        io.to(socket.roomId).emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    // 12. NGẮT KẾT NỐI & CẬP NHẬT GHẾ
    socket.on('disconnect', () => {

        const currentRoom = getRoomData(socket.roomId);

        let freedSeat = false;

        currentRoom.seats = currentRoom.seats.map(s => {

            if (
                s.occupiedBy &&
                s.occupiedBy.socketId === socket.id
            ) {
                freedSeat = true;

                return {
                    ...s,
                    occupiedBy: null
                };
            }

            return s;
        });

        if (freedSeat) {
            io.to(socket.roomId).emit(
                'seat-state-changed',
                currentRoom.seats
            );
        }

        io.to(socket.roomId).emit(
            'update-online-count',
            getRoomOnlineCount(socket.roomId)
        );
    });

    io.to(roomId).emit(
        'update-online-count',
        getRoomOnlineCount(roomId)
    );
});

// 13. ĐỊNH KỲ PHÁT SÓNG ĐỒNG BỘ NHẠC THEO TỪNG ROOM
setInterval(() => {

    Object.keys(rooms).forEach(rId => {

        const room = rooms[rId];

        if (
            room.activePlaylist &&
            room.activePlaylist.length > 0
        ) {
            io.to(rId).emit('audio-sync-heartbeat', {
                trackIndex: room.currentTrackIndex,
                trackStartTime: room.trackStartTime,
                url: room.activePlaylist[
                    room.currentTrackIndex
                ]?.url || '',
                title: room.activePlaylist[
                    room.currentTrackIndex
                ]?.title || ''
            });
        }
    });

}, 5000);

// 14. KHỞI ĐỘNG SERVER
const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
    console.log(
        `Zen Pond Realtime Server v10.3.2 running on port ${PORT} (Dynamic Seats: Room 2 = 37)`
    );
});
