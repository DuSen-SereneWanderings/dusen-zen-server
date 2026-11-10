/**
 * Du Sen Zen Pond - Realtime Socket.io Server
 * v10.4.2 - Verified Identity, Seat Snapshot & Authoritative Audio Sync
 * GitHub Repo Server code for Render deployment
 */

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

// 1. CẤU HÌNH SỐ LƯỢNG CHỖ NGỒI THEO TỪNG PHÒNG
const ROOM_SEATS_LIMIT = {
    main: 17,
    room_2: 37
};

// Thời gian giữ ghế tạm khi mất mạng đột ngột, cho phép cùng thiết bị kết nối lại.
const DISCONNECT_GRACE_MS = 30 * 1000;
const IDENTITY_SECRET = String(process.env.DZP_SOCKET_IDENTITY_SECRET || '');
const seatReleaseTimers = new Map();

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
            maxSeats,
            seats: Array.from({ length: maxSeats }, (_, i) => ({ id: i + 1, occupiedBy: null })),
            totalHearts: 0,
            lanternCounts: { 'An yên': 0, 'Bình an': 0, 'Biết ơn': 0 },
            sessionKey: null,
            currentTrackIndex: 0,
            trackStartTime: Date.now(),
            activePlaylist: [],
            playlistFingerprint: ''
        };
    }
    return rooms[rId];
}

function getPublicSeats(room) {
    return room.seats.map(seat => ({
        id: seat.id,
        occupiedBy: seat.occupiedBy ? {
            socketId: seat.occupiedBy.socketId,
            name: seat.occupiedBy.name || 'Lữ khách',
            avatar: seat.occupiedBy.avatar || ''
        } : null
    }));
}

function getRoomOnlineCount(roomId) {
    const rId = roomId || 'main';
    const roomAdapter = io.sockets.adapter.rooms.get(rId);
    return roomAdapter ? roomAdapter.size : 0;
}

function verifyIdentityToken(token) {
    if (!IDENTITY_SECRET || typeof token !== 'string' || token.length > 2048) return null;
    const parts = token.split('.');
    if (parts.length !== 2) return null;

    const payloadPart = parts[0];
    const signature = parts[1];
    const expected = crypto.createHmac('sha256', IDENTITY_SECRET).update(payloadPart).digest('hex');
    if (!/^[a-f0-9]{64}$/i.test(signature)) return null;

    const receivedBuffer = Buffer.from(signature, 'hex');
    const expectedBuffer = Buffer.from(expected, 'hex');
    if (receivedBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(receivedBuffer, expectedBuffer)) return null;

    try {
        const normalized = payloadPart.replace(/-/g, '+').replace(/_/g, '/');
        const padded = normalized + '='.repeat((4 - normalized.length % 4) % 4);
        const payload = JSON.parse(Buffer.from(padded, 'base64').toString('utf8'));
        const userId = Number(payload.uid);
        const expiresAt = Number(payload.exp);
        if (!Number.isSafeInteger(userId) || userId <= 0) return null;
        if (!Number.isFinite(expiresAt) || expiresAt < Math.floor(Date.now() / 1000)) return null;
        return {
            userId,
            identityKey: crypto.createHash('sha256').update(String(userId)).digest('hex')
        };
    } catch (error) {
        return null;
    }
}

function cleanDeviceId(value) {
    const id = String(value || '').trim();
    if (!id || id.length > 128 || !/^[A-Za-z0-9_-]+$/.test(id)) return '';
    return id;
}

function getSeatTimerKey(roomId, seatId) {
    return `${roomId}:${seatId}`;
}

function clearSeatReleaseTimer(roomId, seatId) {
    const key = getSeatTimerKey(roomId, seatId);
    const timer = seatReleaseTimers.get(key);
    if (timer) clearTimeout(timer);
    seatReleaseTimers.delete(key);
}

function emitSeatState(roomId) {
    const room = getRoomData(roomId);
    io.to(roomId).emit('seat-state-changed', getPublicSeats(room));
}

// Gửi snapshot trạng thái phòng cho client mới kết nối.
// Cả client bị chặn do đăng nhập trên thiết bị khác vẫn cần snapshot
// để hiển thị những người đang an tọa; việc này KHÔNG cấp quyền giữ ghế.
function sendRoomInitialState(socket, roomId) {
    const room = getRoomData(roomId);
    const seats = getPublicSeats(room);
    const occupiedCount = room.seats.filter(seat => seat.occupiedBy !== null).length;

    socket.emit('entry-status-checked', {
        isFull: (room.maxSeats - occupiedCount) <= 0,
        availableSeats: room.maxSeats - occupiedCount,
        maxSeats: room.maxSeats,
        seats
    });

    socket.emit('init-state', {
        seats,
        totalHearts: room.totalHearts,
        lanternCounts: room.lanternCounts,
        audioState: {
            trackIndex: room.currentTrackIndex,
            trackStartTime: room.trackStartTime,
            url: room.activePlaylist[room.currentTrackIndex]?.url || '',
            title: room.activePlaylist[room.currentTrackIndex]?.title || ''
        }
    });
}

function releaseSocketSeat(roomId, socketId) {
    if (!roomId) return false;
    const room = getRoomData(roomId);
    let changed = false;
    room.seats.forEach(seat => {
        if (seat.occupiedBy && seat.occupiedBy.socketId === socketId) {
            clearSeatReleaseTimer(roomId, seat.id);
            seat.occupiedBy = null;
            changed = true;
        }
    });
    if (changed) emitSeatState(roomId);
    return changed;
}

function showDuplicateSession(socket, message) {
    socket.identityBlocked = true;
    if (socket.duplicateNoticeSent) return;
    socket.duplicateNoticeSent = true;
    socket.emit('duplicate-session', { message });
}

function blockIfUserIsSeatedElsewhere(socket, roomId) {
    if (!socket.identityKey) return false;
    const room = getRoomData(roomId || socket.roomId);
    const seat = room.seats.find(item => item.occupiedBy && item.occupiedBy.identityKey === socket.identityKey);
    if (!seat || seat.occupiedBy.socketId === socket.id) return false;

    const previousSocket = io.sockets.sockets.get(seat.occupiedBy.socketId);
    const isActive = !!(previousSocket && previousSocket.connected);
    const message = isActive
        ? 'Bạn đang tham gia không gian trên một thiết bị khác. Vui lòng tiếp tục sử dụng thiết bị đang giữ chỗ ngồi hiện tại.'
        : 'Tài khoản của bạn vừa mất kết nối trên một thiết bị khác. Vui lòng chờ tối đa 30 giây để hệ thống xác nhận phiên cũ đã kết thúc rồi thử lại.';
    showDuplicateSession(socket, message);
    return true;
}

function resolveExistingUserSeat(socket, roomId) {
    const room = getRoomData(roomId);
    const existingSeat = room.seats.find(seat =>
        seat.occupiedBy && seat.occupiedBy.identityKey === socket.identityKey
    );
    if (!existingSeat) return { allowed: true, reclaimed: false };

    const owner = existingSeat.occupiedBy;
    if (owner.socketId === socket.id) return { allowed: true, reclaimed: false, seatId: existingSeat.id };

    const previousSocket = io.sockets.sockets.get(owner.socketId);
    if (previousSocket && previousSocket.connected) {
        return { allowed: false, reason: 'active', seatId: existingSeat.id };
    }

    const disconnectedAt = Number(owner.disconnectedAt || 0);
    const withinGrace = disconnectedAt > 0 && (Date.now() - disconnectedAt) < DISCONNECT_GRACE_MS;
    if (withinGrace && owner.deviceId && owner.deviceId === socket.deviceId) {
        clearSeatReleaseTimer(roomId, existingSeat.id);
        owner.socketId = socket.id;
        owner.disconnectedAt = 0;
        owner.deviceId = socket.deviceId;
        socket.mySeatId = existingSeat.id;
        emitSeatState(roomId);
        socket.emit('seat-reclaimed', { seatId: existingSeat.id });
        return { allowed: true, reclaimed: true, seatId: existingSeat.id };
    }

    if (withinGrace) {
        return { allowed: false, reason: 'grace', seatId: existingSeat.id };
    }

    clearSeatReleaseTimer(roomId, existingSeat.id);
    existingSeat.occupiedBy = null;
    emitSeatState(roomId);
    return { allowed: true, reclaimed: false };
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
    if (month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
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
        if (currentTimestamp !== null && incomingTimestamp < currentTimestamp) return false;
    }

    room.sessionKey = normalizedKey;
    room.totalHearts = 0;
    Object.keys(room.lanternCounts).forEach(word => { room.lanternCounts[word] = 0; });
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

    const handshakeAuth = socket.handshake.auth || {};
    const identity = verifyIdentityToken(handshakeAuth.identityToken);
    socket.userId = identity ? identity.userId : null;
    socket.identityKey = identity ? identity.identityKey : null;
    socket.deviceId = cleanDeviceId(handshakeAuth.deviceId);
    socket.identityBlocked = false;
    socket.duplicateNoticeSent = false;

    function syncSocketRoom(incomingRoomId) {
        if (incomingRoomId && incomingRoomId !== socket.roomId) {
            socket.leave(socket.roomId);
            socket.roomId = incomingRoomId;
            socket.join(incomingRoomId);
            socket.identityBlocked = false;
            socket.duplicateNoticeSent = false;
            socket.mySeatId = null;
        }
    }

    // 5. NHẬN YÊU CẦU VÀO KHÔNG GIAN
    socket.on('request-entry', (data) => {
        const userObj = typeof data === 'object' && data !== null ? data : {};
        const targetRoomId = userObj.roomId || socket.roomId || 'main';
        syncSocketRoom(targetRoomId);

        // Có thể xác thực lại token từ payload khi socket reconnect bằng client cũ.
        if (!socket.userId && userObj.identityToken) {
            const refreshedIdentity = verifyIdentityToken(userObj.identityToken);
            if (refreshedIdentity) {
                socket.userId = refreshedIdentity.userId;
                socket.identityKey = refreshedIdentity.identityKey;
            }
        }
        if (!socket.userId || !socket.identityKey || !IDENTITY_SECRET) {
            socket.identityBlocked = true;
            socket.emit('identity-auth-required', {
                message: 'Không thể xác thực tài khoản Du Sen. Vui lòng tải lại trang hoặc đăng nhập lại trước khi chọn chỗ ngồi.'
            });
            return;
        }

        socket.deviceId = cleanDeviceId(userObj.deviceId || socket.deviceId);
        if (!socket.deviceId) {
            socket.identityBlocked = true;
            socket.emit('identity-auth-required', {
                message: 'Trình duyệt chưa tạo được mã thiết bị an toàn. Vui lòng bật bộ nhớ trình duyệt và tải lại trang.'
            });
            return;
        }

        socket.userName = userObj.name || userObj.userName || 'Lữ khách';
        socket.userAvatar = userObj.avatar || userObj.userAvatar || '';
        const wasIdentityBlocked = socket.identityBlocked;
        socket.identityBlocked = false;

        const existing = resolveExistingUserSeat(socket, socket.roomId);
        if (!existing.allowed) {
            // Vẫn gửi trạng thái ghế để client vẽ đầy đủ người đang có mặt.
            // Client này không được claim-seat; snapshot chỉ phục vụ hiển thị.
            sendRoomInitialState(socket, socket.roomId);

            const message = existing.reason === 'grace'
                ? 'Tài khoản của bạn vừa mất kết nối trên một thiết bị khác. Vui lòng chờ tối đa 30 giây để hệ thống xác nhận phiên cũ đã kết thúc rồi thử lại.'
                : 'Bạn đang tham gia không gian trên một thiết bị khác. Vui lòng tiếp tục sử dụng thiết bị đang giữ chỗ ngồi hiện tại.';
            showDuplicateSession(socket, message);
            io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
            return;
        }

        socket.duplicateNoticeSent = false;
        if (wasIdentityBlocked) socket.emit('identity-session-available', {});

        applyRoomSession(socket.roomId, userObj.sessionKey);
        sendRoomInitialState(socket, socket.roomId);
        io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
    });

    socket.on('session-sync', (data) => {
        if (!data || !data.sessionKey || socket.identityBlocked) return;
        if (data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        applyRoomSession(socket.roomId, data.sessionKey);
    });

    // 7. ĐĂNG KÝ NHẬN VỊ TRÍ — MỖI TÀI KHOẢN CHỈ GIỮ MỘT GHẾ TRONG MỖI PHÒNG
    socket.on('claim-seat', (data) => {
        if (socket.identityBlocked || !socket.userId || !socket.identityKey || !socket.deviceId) {
            socket.emit('identity-auth-required', {
                message: 'Phiên tham gia chưa được xác thực. Vui lòng tải lại trang và đăng nhập lại.'
            });
            return;
        }

        const targetRoomId = (typeof data === 'object' && data && data.roomId) ? data.roomId : socket.roomId;
        syncSocketRoom(targetRoomId);
        const rawSeatId = typeof data === 'object' && data ? data.seatId : data;

        let seatId = NaN;
        if (typeof rawSeatId === 'number') seatId = rawSeatId;
        else if (typeof rawSeatId === 'string') {
            const extractedNum = rawSeatId.replace(/\D/g, '');
            if (extractedNum !== '') seatId = parseInt(extractedNum, 10);
        }
        if (isNaN(seatId)) {
            socket.emit('claim-denied', { code: 'invalid-seat', message: 'Vị trí không hợp lệ. Vui lòng thử lại.' });
            return;
        }

        if (typeof data === 'object' && data) {
            if (data.name || data.userName) socket.userName = data.name || data.userName;
            if (data.avatar || data.userAvatar) socket.userAvatar = data.avatar || data.userAvatar;
        }

        const currentRoom = getRoomData(socket.roomId);
        const currentUserSeat = currentRoom.seats.find(seat =>
            seat.occupiedBy && seat.occupiedBy.identityKey === socket.identityKey
        );

        if (currentUserSeat && currentUserSeat.occupiedBy.socketId !== socket.id) {
            const previousSocket = io.sockets.sockets.get(currentUserSeat.occupiedBy.socketId);
            if (previousSocket && previousSocket.connected) {
                showDuplicateSession(socket, 'Bạn đang tham gia không gian trên một thiết bị khác. Vui lòng tiếp tục sử dụng thiết bị đang giữ chỗ ngồi hiện tại.');
                socket.emit('claim-denied', { code: 'duplicate-session', message: 'Bạn đang tham gia không gian trên một thiết bị khác. Vui lòng tiếp tục sử dụng thiết bị đang giữ chỗ ngồi hiện tại.' });
                return;
            }

            const owner = currentUserSeat.occupiedBy;
            const disconnectedAt = Number(owner.disconnectedAt || 0);
            const withinGrace = disconnectedAt > 0 && (Date.now() - disconnectedAt) < DISCONNECT_GRACE_MS;
            if (withinGrace && owner.deviceId === socket.deviceId) {
                clearSeatReleaseTimer(socket.roomId, currentUserSeat.id);
                owner.socketId = socket.id;
                owner.disconnectedAt = 0;
                socket.mySeatId = currentUserSeat.id;
                emitSeatState(socket.roomId);
                socket.emit('seat-reclaimed', { seatId: currentUserSeat.id });
                return;
            }
            if (withinGrace) {
                showDuplicateSession(socket, 'Tài khoản của bạn vừa mất kết nối trên một thiết bị khác. Vui lòng chờ tối đa 30 giây để hệ thống xác nhận phiên cũ đã kết thúc rồi thử lại.');
                socket.emit('claim-denied', { code: 'duplicate-session', message: 'Tài khoản của bạn vừa mất kết nối trên một thiết bị khác. Vui lòng chờ tối đa 30 giây rồi thử lại.' });
                return;
            }
            clearSeatReleaseTimer(socket.roomId, currentUserSeat.id);
            currentUserSeat.occupiedBy = null;
        }

        // Hỗ trợ cả 1-indexed id:1..N và 0-indexed index:0..N-1.
        let targetIndex = currentRoom.seats.findIndex(seat => seat.id === seatId);
        if (targetIndex === -1 && seatId >= 0 && seatId < currentRoom.seats.length) targetIndex = seatId;
        if (targetIndex === -1) {
            socket.emit('claim-denied', { code: 'invalid-seat', message: 'Vị trí không tồn tại trong không gian này.' });
            return;
        }

        const targetSeat = currentRoom.seats[targetIndex];
        if (targetSeat.occupiedBy && targetSeat.occupiedBy.socketId !== socket.id) {
            socket.emit('claim-denied', { code: 'seat-occupied' });
            return;
        }

        // Chỉ giải phóng ghế cũ sau khi đã xác nhận ghế đích hợp lệ và còn trống.
        currentRoom.seats.forEach(seat => {
            if (seat.occupiedBy && seat.occupiedBy.socketId === socket.id && seat.id !== targetSeat.id) {
                clearSeatReleaseTimer(socket.roomId, seat.id);
                seat.occupiedBy = null;
            }
        });

        targetSeat.occupiedBy = {
            socketId: socket.id,
            name: socket.userName || 'Lữ khách',
            avatar: socket.userAvatar || '',
            identityKey: socket.identityKey,
            deviceId: socket.deviceId,
            disconnectedAt: 0
        };
        socket.mySeatId = targetSeat.id;
        console.log(`[Zen Pond] Seat claimed: Seat ${targetSeat.id} in room '${socket.roomId}' by '${socket.userName}' (${socket.id})`);
        emitSeatState(socket.roomId);

        // Nếu cùng tài khoản đã mở phòng trên tab/thiết bị khác, báo ngay khi ghế đầu tiên được giữ.
        for (const otherSocket of io.sockets.sockets.values()) {
            if (otherSocket.id !== socket.id && otherSocket.roomId === socket.roomId && otherSocket.identityKey === socket.identityKey) {
                showDuplicateSession(otherSocket, 'Bạn đang tham gia không gian trên một thiết bị khác. Vui lòng tiếp tục sử dụng thiết bị đang giữ chỗ ngồi hiện tại.');
            }
        }
    });

    // Thoát chủ động: giải phóng ghế ngay, không cần chờ grace period.
    socket.on('leave-room', (data, acknowledge) => {
        const targetRoomId = (data && data.roomId) || socket.roomId;
        releaseSocketSeat(targetRoomId, socket.id);
        socket.mySeatId = null;
        if (typeof acknowledge === 'function') acknowledge({ ok: true });
    });

    // Playlist được khởi tạo một lần cho mỗi phòng. Thiết bị kết nối sau
    // không thể ghi đè danh sách phát đang được cả phòng sử dụng.
    socket.on('sync-playlist', (data) => {
        if (socket.identityBlocked) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;

        const currentRoom = getRoomData(socket.roomId);
        if (!data || !Array.isArray(data.playlist) || data.playlist.length === 0) return;
        if (currentRoom.activePlaylist.length > 0) return;

        const normalizedPlaylist = data.playlist.map((item) => {
            if (!item || typeof item !== 'object') return null;
            const url = String(item.url || '').trim();
            if (!/^https?:\/\//i.test(url)) return null;
            return {
                url: url.slice(0, 2048),
                title: String(item.title || 'Du Sen').trim().slice(0, 200)
            };
        }).filter(Boolean);

        if (!normalizedPlaylist.length) return;
        currentRoom.activePlaylist = normalizedPlaylist;
        currentRoom.currentTrackIndex = 0;
        // Bắt đầu mốc thời gian tại lúc playlist chuẩn được khởi tạo,
        // tránh tính nhầm thời gian từ lúc room object được tạo trước đó.
        currentRoom.trackStartTime = Date.now();
        currentRoom.playlistFingerprint = crypto.createHash('sha256')
            .update(JSON.stringify(normalizedPlaylist)).digest('hex');

        // Phát trạng thái chuẩn ngay khi playlist đầu tiên được đăng ký.
        const firstTrack = currentRoom.activePlaylist[currentRoom.currentTrackIndex];
        io.to(socket.roomId).emit('sync-audio-track', {
            trackIndex: currentRoom.currentTrackIndex,
            trackStartTime: currentRoom.trackStartTime,
            url: firstTrack.url,
            title: firstTrack.title
        });
        console.log(`[Zen Pond] Audio playlist initialized: room '${socket.roomId}' (${normalizedPlaylist.length} tracks)`);
    });

    // Chỉ chấp nhận một lần kết thúc cho đúng phiên phát hiện tại.
    // Mọi thiết bị gửi cùng trackIndex + trackStartTime; sau lần đầu,
    // trackStartTime đổi nên các sự kiện đến trễ đều bị loại bỏ.
    socket.on('track-ended', (data) => {
        if (socket.identityBlocked) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;

        const currentRoom = getRoomData(socket.roomId);
        if (!data || typeof data !== 'object' || currentRoom.activePlaylist.length === 0) return;

        const endedIndex = Number(data.trackIndex);
        const endedStartTime = Number(data.trackStartTime);
        if (!Number.isInteger(endedIndex) || endedIndex !== currentRoom.currentTrackIndex) return;
        if (!Number.isFinite(endedStartTime) || endedStartTime !== Number(currentRoom.trackStartTime)) return;

        const nextIndex = (currentRoom.currentTrackIndex + 1) % currentRoom.activePlaylist.length;
        const nextTrack = currentRoom.activePlaylist[nextIndex];
        if (!nextTrack || !nextTrack.url) return;

        currentRoom.currentTrackIndex = nextIndex;
        currentRoom.trackStartTime = Date.now();
        io.to(socket.roomId).emit('sync-audio-track', {
            trackIndex: currentRoom.currentTrackIndex,
            trackStartTime: currentRoom.trackStartTime,
            url: nextTrack.url,
            title: nextTrack.title
        });
    });

    socket.on('send-heart', (data) => {
        if (socket.identityBlocked) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
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
        if (socket.identityBlocked) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        if (data && data.sessionKey) applyRoomSession(socket.roomId, data.sessionKey);
        const currentRoom = getRoomData(socket.roomId);
        const word = (data && data.word) || 'An yên';
        if (currentRoom.lanternCounts[word] === undefined) currentRoom.lanternCounts[word] = 0;
        currentRoom.lanternCounts[word]++;
        const senderName = (data && (data.userName || data.name)) || socket.userName || 'Lữ khách';
        io.to(socket.roomId).emit('lantern-broadcast', {
            seatId: data ? data.seatId : null,
            userName: senderName,
            word,
            senderSocketId: socket.id,
            lanternCounts: currentRoom.lanternCounts
        });
    });

    socket.on('dzp_send_wish', (data) => {
        if (socket.identityBlocked) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;
        io.to(socket.roomId).emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    socket.on('disconnect', () => {
        const roomId = socket.roomId || currentRoomId;
        const currentRoom = getRoomData(roomId);
        currentRoom.seats.forEach(seat => {
            if (seat.occupiedBy && seat.occupiedBy.socketId === socket.id) {
                const disconnectedAt = Date.now();
                seat.occupiedBy.disconnectedAt = disconnectedAt;
                const timerKey = getSeatTimerKey(roomId, seat.id);
                clearSeatReleaseTimer(roomId, seat.id);
                const timer = setTimeout(() => {
                    const latestRoom = getRoomData(roomId);
                    const latestSeat = latestRoom.seats.find(item => item.id === seat.id);
                    if (latestSeat && latestSeat.occupiedBy && latestSeat.occupiedBy.socketId === socket.id && latestSeat.occupiedBy.disconnectedAt === disconnectedAt) {
                        latestSeat.occupiedBy = null;
                        seatReleaseTimers.delete(timerKey);
                        emitSeatState(roomId);
                    }
                }, DISCONNECT_GRACE_MS);
                seatReleaseTimers.set(timerKey, timer);
            }
        });
        io.to(roomId).emit('update-online-count', getRoomOnlineCount(roomId));
    });

    io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
});

setInterval(() => {
    Object.keys(rooms).forEach(roomId => {
        const room = rooms[roomId];
        if (room.activePlaylist && room.activePlaylist.length > 0) {
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
    console.log(`Zen Pond Realtime Server v10.4.1 running on port ${PORT}`);
    if (!IDENTITY_SECRET) {
        console.error('[Zen Pond] Missing DZP_SOCKET_IDENTITY_SECRET. Account identity validation is disabled; seat claims will be rejected.');
    }
});
