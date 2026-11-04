/**
 * Du Sen Zen Pond - Realtime Socket.io Server (v9.5.1 - Anti-Kickout & Schedule Sync)
 * GitHub Repo Server code for Render deployment
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

app.get('/', (req, res) => {
    res.status(200).send('Zen Pond Realtime Server v9.5.1 is Running OK');
});

app.get('/health', (req, res) => {
    res.status(200).json({ status: 'ok', time: new Date().toISOString() });
});

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    pingTimeout: 30000,
    pingInterval: 10000,
    allowEIO3: true
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

function getVietnamTimeObj() {
    const now = new Date();
    const vnMs = now.getTime() + (7 * 3600000);
    const vnDate = new Date(vnMs);
    return {
        hour: vnDate.getUTCHours(),
        minute: vnDate.getUTCMinutes(),
        second: vnDate.getUTCSeconds()
    };
}

function safeParseInt(val, fallback) {
    if (val === undefined || val === null || val === '') return fallback;
    const parsed = parseInt(val, 10);
    return isNaN(parsed) ? fallback : parsed;
}

// 3. KIỂM TRA TRẠNG THÁI ĐÓNG/MỞ PHÒNG CHUẨN XÁC TUYỆT ĐỐI (HỖ TRỢ SCHEDULEMODE 'CLOSED' & 'OPEN')
function checkServerRoomStatus(settings) {
    const s = settings || {};

    const isServerClosed = (
        s.isClosedTime === true || 
        s.isClosedTime === 1 || 
        s.isClosedTime === '1' || 
        s.isClosedTime === 'true'
    );

    if (isServerClosed) {
        return { isOpen: false, isClosed: true, scheduleStr: 'Tạm đóng dưỡng nuôi' };
    }

    const fromH = safeParseInt(s.openFromH, 0);
    const fromM = safeParseInt(s.openFromM, 0);
    const toH   = safeParseInt(s.openToH, 23);
    const toM   = safeParseInt(s.openToM, 59);

    const scheduleMode = s.scheduleMode || 'open'; // Mặc định là 'open' khi có thiết lập giờ mở

    const vnTime = getVietnamTimeObj();
    const currTotal = vnTime.hour * 60 + vnTime.minute;
    const fromTotal = fromH * 60 + fromM;
    const toTotal   = toH * 60 + toM; // Giữ nguyên mốc kết thúc chính xác, không cộng thêm 1

    let isInTimeRange = false;
    if (fromTotal <= toTotal) {
        // Dùng dấu <= để tính trọn vẹn đến phút cuối cùng (ví dụ đến đúng 11:00 vẫn mở)
        isInTimeRange = (currTotal >= fromTotal && currTotal <= toTotal);
    } else {
        // Trường hợp qua đêm (ví dụ từ 22:00 đến 04:00 sáng hôm sau)
        isInTimeRange = (currTotal >= fromTotal || currTotal <= toTotal);
    }

    let isOpen = true;
    if (scheduleMode === 'closed') {
        isOpen = !isInTimeRange;
    } else {
        isOpen = isInTimeRange;
    }

    const pad = (n) => String(n).padStart(2, '0');
    const scheduleStr = (scheduleMode === 'closed')
        ? `Dưỡng nuôi từ ${pad(fromH)}:${pad(fromM)} đến ${pad(toH)}:${pad(toM)}`
        : `Mở cửa từ ${pad(fromH)}:${pad(fromM)} đến ${pad(toH)}:${pad(toM)}`;

    return {
        isOpen: isOpen,
        isClosed: !isOpen,
        scheduleStr: scheduleStr
    };
}

function getExactOnlineCount() {
    return io.of("/").sockets.size;
}

io.on('connection', (socket) => {

    socket.on('request-entry', (payload) => {
        let userObj = {};
        let settingsObj = {};

        if (payload && payload.user) {
            userObj = payload.user;
            settingsObj = payload.settings || {};
        } else if (payload) {
            userObj = payload;
            settingsObj = payload;
        }

        if (userObj && userObj.name) {
            socket.userName = userObj.name;
            socket.userAvatar = userObj.avatar || '';
        } else {
            socket.userName = 'Lữ khách';
        }

        const roomStatus = checkServerRoomStatus(settingsObj);
        const occupiedCount = seats.filter(s => s.occupiedBy !== null).length;
        const availableSeats = MAX_SEATS - occupiedCount;
        const isFull = availableSeats <= 0;

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

    socket.on('dzp_send_wish', (data) => {
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;

        io.emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Zen Pond Realtime Server v9.5.1 running on port ${PORT}`);
});
