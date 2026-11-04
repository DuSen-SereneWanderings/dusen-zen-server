/**
 * Du Sen Zen Pond - Realtime Socket.io Server (v9.4.0 - Vietnam Timezone & Stable Room Schedule)
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
let seats = Array.from({ length: MAX_SEATS }, (_, i) => ({
    id: i + 1,
    occupiedBy: null
}));

let totalHearts = 0;
let lanternCounts = {
    'An yên': 0,
    'Bình an': 0,
    'Biết ơn': 0
};
let currentTrackIndex = 0;
let trackStartTime = Date.now();
let activePlaylist = [];

// --------------------------------------------------------------------------
// 1. HÀM TÍNH GIỜ VIỆT NAM (UTC+7) - GIẢI QUYẾT LỆCH GIỜ TRÊN MÁY CHỦ RENDER
// --------------------------------------------------------------------------
function getVietnamTimeObj() {
    try {
        const now = new Date();
        const options = { timeZone: 'Asia/Ho_Chi_Minh', hour: 'numeric', minute: 'numeric', second: 'numeric', hour12: false };
        const formatter = new Intl.DateTimeFormat('en-US', options);
        const parts = formatter.formatToParts(now);
        let h = null, m = null, s = null;
        parts.forEach(p => {
            if (p.type === 'hour') h = parseInt(p.value, 10);
            if (p.type === 'minute') m = parseInt(p.value, 10);
            if (p.type === 'second') s = parseInt(p.value, 10);
        });
        if (h !== null && !isNaN(h)) {
            if (h === 24) h = 0;
            return { hour: h, minute: m || 0, second: s || 0 };
        }
    } catch (e) {}

    // Fallback tính toán cộng bù 7 tiếng chuẩn UTC+7 cho mọi môi trường
    const now = new Date();
    const utcMs = now.getTime() + (now.getTimezoneOffset() * 60000);
    const vnDate = new Date(utcMs + (7 * 3600000));
    return { hour: vnDate.getHours(), minute: vnDate.getMinutes(), second: vnDate.getSeconds() };
}

// --------------------------------------------------------------------------
// 2. HÀM KIỂM TRA TRẠNG THÁI MỞ/ĐÓNG PHÒNG CHUẨN XÁC THEO GIỜ VIỆT NAM
// --------------------------------------------------------------------------
function checkServerRoomStatus(settings) {
    const s = settings || {};

    // Kiểm tra cờ đóng phòng thủ công từ WordPress/Settings (Chống lỗi ép kiểu string '0' / 'false')
    const isServerClosed = (
        s.isClosedTime === true || 
        s.isClosedTime === 1 || 
        s.isClosedTime === '1' || 
        s.isClosedTime === 'true'
    );

    // Lấy thông số khung giờ (Mặc định mở 00:00 - 23:57 nếu không truyền)
    const openFromH = (s.openFromH !== undefined && s.openFromH !== null) ? parseInt(s.openFromH, 10) : 0;
    const openFromM = (s.openFromM !== undefined && s.openFromM !== null) ? parseInt(s.openFromM, 10) : 0;
    const openToH   = (s.openToH !== undefined && s.openToH !== null)   ? parseInt(s.openToH, 10)   : 23;
    const openToM   = (s.openToM !== undefined && s.openToM !== null)   ? parseInt(s.openToM, 10)   : 57;

    const vnTime = getVietnamTimeObj();
    const currTotal  = vnTime.hour * 60 + vnTime.minute;
    const openTotal  = openFromH * 60 + openFromM;
    const closeTotal = openToH * 60 + openToM;

    let isOpen = true;
    if (openTotal < closeTotal) {
        isOpen = (currTotal >= openTotal && currTotal < closeTotal);
    } else if (openTotal > closeTotal) {
        isOpen = (currTotal >= openTotal || currTotal < closeTotal);
    }

    if (isServerClosed) {
        isOpen = false;
    }

    const pad = (n) => String(n).padStart(2, '0');
    const scheduleStr = s.scheduleStr || `${pad(openFromH)}:${pad(openFromM)} đến ${pad(openToH)}:${pad(openToM)}`;

    return {
        isOpen: isOpen,
        isClosed: !isOpen,
        scheduleStr: scheduleStr
    };
}

function getExactOnlineCount() {
    return io.of("/").sockets.size;
}

// --------------------------------------------------------------------------
// 3. XỬ LÝ LẮNG NGHE SỰ KIỆN REALTIME CỦA SOCKET.IO
// --------------------------------------------------------------------------
io.on('connection', (socket) => {

    // 1. Ghi nhớ thông tin User & Kiểm tra Trạng thái Vào phòng
    socket.on('request-entry', (userObj) => {
        if (userObj && userObj.name) {
            socket.userName = userObj.name;
            socket.userAvatar = userObj.avatar || '';
        } else {
            socket.userName = 'Lữ khách';
        }

        // Tính toán trạng thái lịch mở/đóng phòng dựa trên dữ liệu Client/WordPress gửi lên
        const clientSettings = (userObj && userObj.settings) ? userObj.settings : (userObj || {});
        const roomStatus = checkServerRoomStatus(clientSettings);

        const occupiedCount = seats.filter(s => s.occupiedBy !== null).length;
        const availableSeats = MAX_SEATS - occupiedCount;
        const isFull = availableSeats <= 0;

        // Phát phản hồi kiểm tra cổng vào phòng về Client
        socket.emit('entry-status-checked', {
            isClosed: roomStatus.isClosed,
            scheduleStr: roomStatus.scheduleStr,
            isFull: isFull,
            availableSeats: availableSeats,
            maxSeats: MAX_SEATS,
            seats: seats
        });
        
        io.emit('update-online-count', getExactOnlineCount());
    });

    // 2. Gửi trạng thái khởi tạo cho Client
    socket.emit('init-state', {
        seats: seats,
        totalHearts: totalHearts,
        lanternCounts: lanternCounts,
        audioState: {
            trackIndex: currentTrackIndex,
            trackStartTime: trackStartTime,
            url: activePlaylist[currentTrackIndex]?.url || '',
            title: activePlaylist[currentTrackIndex]?.title || ''
        }
    });

    // 3. Đăng ký nhận vị trí an tọa (Claim Seat)
    socket.on('claim-seat', (seatId) => {
        seats = seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        const targetIndex = seats.findIndex(s => s.id === seatId);
        if (targetIndex !== -1 && !seats[targetIndex].occupiedBy) {
            seats[targetIndex].occupiedBy = {
                socketId: socket.id,
                name: socket.userName || 'Lữ khách',
                avatar: socket.userAvatar || ''
            };
        }

        io.emit('seat-state-changed', seats);
    });

    // 4. Đồng bộ Nhạc Thiền
    socket.on('sync-playlist', (data) => {
        if (data && Array.isArray(data.playlist) && data.playlist.length > 0) {
            activePlaylist = data.playlist;
        }
    });

    socket.on('track-ended', (endedIndex) => {
        if (activePlaylist.length === 0) return;
        currentTrackIndex = (endedIndex + 1) % activePlaylist.length;
        trackStartTime = Date.now();

        io.emit('sync-audio-track', {
            trackIndex: currentTrackIndex,
            trackStartTime: trackStartTime,
            url: activePlaylist[currentTrackIndex].url,
            title: activePlaylist[currentTrackIndex].title
        });
    });

    // 5. Gửi Thương Realtime
    socket.on('send-heart', (data) => {
        totalHearts++;
        const senderName = data.userName || socket.userName || 'Lữ khách';
        io.emit('heart-broadcast', {
            seatId: data.seatId,
            userName: senderName,
            senderSocketId: socket.id,
            totalHearts: totalHearts
        });
    });

    // 6. Thả Hoa Đăng Realtime
    socket.on('send-lantern', (data) => {
        const word = data.word || 'An yên';
        if (!lanternCounts[word]) {
            lanternCounts[word] = 0;
        }
        lanternCounts[word]++;
        const senderName = data.userName || socket.userName || 'Lữ khách';

        io.emit('lantern-broadcast', {
            seatId: data.seatId,
            userName: senderName,
            word: word,
            senderSocketId: socket.id,
            lanternCounts: lanternCounts
        });
    });

    // 7. Phát sóng Thiệp Tâm Nguyện Ẩn Danh Realtime
    socket.on('dzp_send_wish', (data) => {
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;

        io.emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    // 8. Ngắt kết nối & Cập nhật số lữ khách chính xác
    socket.on('disconnect', () => {
        let freedSeat = false;
        seats = seats.map(s => {
            if (s.occupiedBy && s.occupiedBy.socketId === socket.id) {
                freedSeat = true;
                return { ...s, occupiedBy: null };
            }
            return s;
        });

        if (freedSeat) {
            io.emit('seat-state-changed', seats);
        }
        io.emit('update-online-count', getExactOnlineCount());
    });

    io.emit('update-online-count', getExactOnlineCount());
});

// --------------------------------------------------------------------------
// 4. HEARTBEAT ĐỒNG BỘ ÂM THANH MỖI 5 GIÂY
// --------------------------------------------------------------------------
setInterval(() => {
    if (activePlaylist.length > 0) {
        io.emit('audio-sync-heartbeat', {
            trackIndex: currentTrackIndex,
            trackStartTime: trackStartTime,
            url: activePlaylist[currentTrackIndex]?.url || '',
            title: activePlaylist[currentTrackIndex]?.title || ''
        });
    }
}, 5000);

// --------------------------------------------------------------------------
// 5. KHỞI CHẠY SERVER
// --------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Zen Pond Realtime Server v9.4.0 running on port ${PORT}`);
});
