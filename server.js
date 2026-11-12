/**
 * Du Sen Zen Pond - Realtime Socket.io Server
 * v10.5.0 - Verified Room Presence & Private Messaging Relay
 * GitHub Repo Server code for Render deployment
 */

const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');
const cors = require('cors');

// Dùng để nhận biết tiến trình vừa khởi động lại (ví dụ dịch vụ vừa thức dậy).
const SERVER_STARTED_AT = Date.now();

const app = express();
app.use(express.json({ verify: (req, res, buf) => { req.rawBody = Buffer.from(buf); } }));
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
// Khóa phiên tham gia theo từng tài khoản + từng phòng, không khóa đăng nhập toàn website.
const roomEntryOwners = new Map();
const roomEntryReleaseTimers = new Map();

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
            sessionEndAt: 0,
            sessionEnding: false,
            sessionClosed: false,
            sessionEndTimer: null,
            sessionForceExitTimer: null,
            currentTrackIndex: 0,
            trackStartTime: Date.now(),
            activePlaylist: [],
            playlistFingerprint: '',
            audioMode: 'playlist',
            audioResumeMode: 'playlist',
            audioPaused: false,
            audioPausedPosition: 0,
            specialAudioId: '',
            specialAudioStartTime: 0,
            specialAudioUrl: '',
            specialAudioTitle: '',
            specialInterruptedTrackIndex: 0,
            specialAudioDuration: 0,
            specialAudioEndTimer: null
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

function completeSpecialAudio(roomId, expectedAudioId, expectedStartTime) {
    const room = getRoomData(roomId);
    if (room.audioMode !== 'special' || room.audioPaused ||
        room.specialAudioId !== expectedAudioId || Number(room.specialAudioStartTime) !== Number(expectedStartTime)) return false;
    if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
    room.specialAudioEndTimer = null;
    if (!room.activePlaylist.length) {
        room.audioMode = 'paused';
        room.audioResumeMode = 'playlist';
        room.audioPaused = true;
        room.specialAudioId = '';
        room.specialAudioUrl = '';
        room.specialAudioTitle = '';
        room.specialAudioStartTime = 0;
        room.specialAudioDuration = 0;
        io.to(roomId).emit('audio-pause', { roomId, fadeOutMs: 0, audioState: getRoomAudioState(room) });
        return false;
    }
    room.currentTrackIndex = (Number(room.specialInterruptedTrackIndex || 0) + 1) % room.activePlaylist.length;
    room.trackStartTime = Date.now();
    room.audioMode = 'playlist';
    room.audioResumeMode = 'playlist';
    room.audioPaused = false;
    room.audioPausedPosition = 0;
    room.specialAudioId = '';
    room.specialAudioUrl = '';
    room.specialAudioTitle = '';
    room.specialAudioStartTime = 0;
    room.specialAudioDuration = 0;
    io.to(roomId).emit('sync-audio-track', { ...getRoomAudioState(room), fadeInMs: 2200 });
    console.log(`[Zen Pond] Special audio completed; playlist resumed: ${roomId}`);
    return true;
}

function scheduleSpecialAudioEnd(roomId) {
    const room = getRoomData(roomId);
    if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
    room.specialAudioEndTimer = null;
    if (room.audioMode !== 'special' || room.audioPaused || !(room.specialAudioDuration > 0)) return;
    const expectedId = room.specialAudioId;
    const expectedStart = room.specialAudioStartTime;
    const endAt = expectedStart + room.specialAudioDuration * 1000;
    const delay = Math.max(0, endAt - Date.now());
    room.specialAudioEndTimer = setTimeout(() => {
        completeSpecialAudio(roomId, expectedId, expectedStart);
    }, delay);
}

function getRoomAudioState(room) {
    const mode = room.audioMode || 'playlist';
    let url = '';
    let title = '';
    let trackStartTime = room.trackStartTime;
    if (mode === 'special') {
        url = room.specialAudioUrl || '';
        title = room.specialAudioTitle || 'Audio đặc biệt';
        trackStartTime = room.specialAudioStartTime || Date.now();
    } else {
        const track = room.activePlaylist[room.currentTrackIndex];
        url = track?.url || '';
        title = track?.title || '';
        if (mode === 'paused') {
            const resumeMode = room.audioResumeMode || 'playlist';
            if (resumeMode === 'special') {
                url = room.specialAudioUrl || '';
                title = room.specialAudioTitle || 'Audio đặc biệt';
                trackStartTime = Date.now() - (Number(room.audioPausedPosition) || 0) * 1000;
            } else {
                trackStartTime = Date.now() - (Number(room.audioPausedPosition) || 0) * 1000;
            }
        }
    }
    return {
        mode,
        paused: mode === 'paused',
        trackIndex: room.currentTrackIndex,
        trackStartTime,
        url,
        title,
        specialAudioId: room.specialAudioId || '',
        fadeInMs: 0
    };
}

// Gửi snapshot trạng thái phòng cho client mới kết nối.
// Cả client bị chặn do đăng nhập trên thiết bị khác vẫn cần snapshot
// để hiển thị những người đang an tọa; việc này KHÔNG cấp quyền giữ ghế.
// Snapshot CHỈ ĐỌC số vị trí còn trống cho popup cổng vào.
// Không gửi init-state, không cấp ghế và không thay đổi trạng thái tham gia.
function sendSeatAvailabilitySnapshot(socket, roomId) {
    const room = getRoomData(roomId);
    const occupiedCount = room.seats.filter(seat => seat.occupiedBy !== null).length;
    const availableSeats = Math.max(0, room.maxSeats - occupiedCount);

    socket.emit('seat-availability-snapshot', {
        isFull: availableSeats <= 0,
        availableSeats,
        maxSeats: room.maxSeats
    });
}

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
        audioState: getRoomAudioState(room)
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

function getRoomEntryKey(roomId, identityKey) {
    return `${roomId || 'main'}:${identityKey || ''}`;
}

function releaseRoomEntry(roomId, socketId) {
    if (!roomId || !socketId) return;
    const prefix = `${roomId}:`;
    for (const [key, owner] of roomEntryOwners.entries()) {
        if (key.startsWith(prefix) && owner.socketId === socketId) {
            const timer = roomEntryReleaseTimers.get(key);
            if (timer) clearTimeout(timer);
            roomEntryReleaseTimers.delete(key);
            roomEntryOwners.delete(key);
        }
    }
}

function reserveRoomEntry(socket, roomId) {
    const key = getRoomEntryKey(roomId, socket.identityKey);
    const owner = roomEntryOwners.get(key);
    if (!owner || owner.socketId === socket.id) {
        roomEntryOwners.set(key, { socketId: socket.id, deviceId: socket.deviceId, disconnectedAt: 0 });
        return { allowed: true };
    }

    const ownerSocket = io.sockets.sockets.get(owner.socketId);
    if (ownerSocket && ownerSocket.connected) {
        // Cùng deviceId (ví dụ mở lại tab trên cùng trình duyệt) không phải thiết bị khác.
        if (owner.deviceId && owner.deviceId === socket.deviceId) {
            ownerSocket.identityBlocked = true;
            ownerSocket.mySeatId = null;
            ownerSocket.emit('same-device-session-replaced', {});
            roomEntryOwners.set(key, { socketId: socket.id, deviceId: socket.deviceId, disconnectedAt: 0 });
            return { allowed: true, replacedSameDevice: true };
        }
        return { allowed: false, reason: 'active' };
    }

    const disconnectedAt = Number(owner.disconnectedAt || 0);
    const withinGrace = disconnectedAt > 0 && (Date.now() - disconnectedAt) < DISCONNECT_GRACE_MS;
    if (withinGrace && owner.deviceId === socket.deviceId) {
        const timer = roomEntryReleaseTimers.get(key);
        if (timer) clearTimeout(timer);
        roomEntryReleaseTimers.delete(key);
        roomEntryOwners.set(key, { socketId: socket.id, deviceId: socket.deviceId, disconnectedAt: 0 });
        return { allowed: true, reclaimed: true };
    }
    if (withinGrace) return { allowed: false, reason: 'grace' };

    const timer = roomEntryReleaseTimers.get(key);
    if (timer) clearTimeout(timer);
    roomEntryReleaseTimers.delete(key);
    roomEntryOwners.set(key, { socketId: socket.id, deviceId: socket.deviceId, disconnectedAt: 0 });
    return { allowed: true };
}

function markRoomEntryDisconnected(socket) {
    for (const [key, owner] of roomEntryOwners.entries()) {
        if (owner.socketId !== socket.id) continue;
        const disconnectedAt = Date.now();
        owner.disconnectedAt = disconnectedAt;
        const previousTimer = roomEntryReleaseTimers.get(key);
        if (previousTimer) clearTimeout(previousTimer);
        const timer = setTimeout(() => {
            const latest = roomEntryOwners.get(key);
            if (latest && latest.socketId === socket.id && latest.disconnectedAt === disconnectedAt) {
                roomEntryOwners.delete(key);
                roomEntryReleaseTimers.delete(key);
            }
        }, DISCONNECT_GRACE_MS);
        roomEntryReleaseTimers.set(key, timer);
    }
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
        if (owner.deviceId && owner.deviceId === socket.deviceId) {
            previousSocket.identityBlocked = true;
            previousSocket.mySeatId = null;
            previousSocket.emit('same-device-session-replaced', {});
            owner.socketId = socket.id;
            owner.disconnectedAt = 0;
            socket.mySeatId = existingSeat.id;
            emitSeatState(roomId);
            return { allowed: true, reclaimed: true, seatId: existingSeat.id };
        }
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

function getSessionEndTimestamp(sessionKey) {
    if (typeof sessionKey !== 'string') return null;
    const parts = sessionKey.split('|');
    if (parts.length !== 4) return null;

    const dateMatch = parts[1].match(/^(\d{4})-(\d{2})-(\d{2})$/);
    const openMatch = parts[2].match(/^(\d{2}):(\d{2})$/);
    const closeMatch = parts[3].match(/^(\d{2}):(\d{2})$/);
    if (!dateMatch || !openMatch || !closeMatch) return null;

    const year = Number(dateMatch[1]);
    const month = Number(dateMatch[2]);
    const day = Number(dateMatch[3]);
    const openHour = Number(openMatch[1]);
    const openMinute = Number(openMatch[2]);
    const closeHour = Number(closeMatch[1]);
    const closeMinute = Number(closeMatch[2]);
    if (month < 1 || month > 12 || day < 1 || day > 31 ||
        openHour > 23 || openMinute > 59 || closeHour > 23 || closeMinute > 59) return null;

    const openMinutes = openHour * 60 + openMinute;
    const closeMinutes = closeHour * 60 + closeMinute;
    const closeDate = new Date(Date.UTC(year, month - 1, day));
    if (openMinutes >= closeMinutes) closeDate.setUTCDate(closeDate.getUTCDate() + 1);

    // Lịch phiên được cấu hình theo giờ Việt Nam (UTC+7).
    return Date.UTC(
        closeDate.getUTCFullYear(), closeDate.getUTCMonth(), closeDate.getUTCDate(),
        closeHour, closeMinute
    ) - (7 * 60 * 60 * 1000);
}

function clearRoomSessionTimers(room) {
    if (room.sessionEndTimer) clearTimeout(room.sessionEndTimer);
    if (room.sessionForceExitTimer) clearTimeout(room.sessionForceExitTimer);
    room.sessionEndTimer = null;
    room.sessionForceExitTimer = null;
}

function releaseAllRoomEntryOwners(roomId) {
    const prefix = `${roomId}:`;
    for (const [key, owner] of roomEntryOwners.entries()) {
        if (!key.startsWith(prefix)) continue;
        const timer = roomEntryReleaseTimers.get(key);
        if (timer) clearTimeout(timer);
        roomEntryReleaseTimers.delete(key);
        roomEntryOwners.delete(key);
    }
}

function forceEndRoomSession(roomId, expectedSessionKey) {
    const rId = roomId || 'main';
    const room = getRoomData(rId);
    if (room.sessionKey !== expectedSessionKey || room.sessionClosed) return;

    room.sessionClosed = true;
    clearRoomSessionTimers(room);
    if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
    room.specialAudioEndTimer = null;

    room.seats.forEach(seat => {
        clearSeatReleaseTimer(rId, seat.id);
        seat.occupiedBy = null;
    });
    releaseAllRoomEntryOwners(rId);

    // Thông báo cho toàn bộ trình duyệt trước khi tách socket khỏi phòng.
    emitSeatState(rId);
    io.to(rId).emit('session-force-exit', {
        roomId: rId,
        sessionKey: expectedSessionKey,
        message: 'Phiên đã khép lại. Không gian Du Sen hẹn gặp lại bạn trong phiên tiếp theo.'
    });
    io.to(rId).emit('update-online-count', 0);

    const members = Array.from(io.sockets.adapter.rooms.get(rId) || []);
    members.forEach(socketId => {
        const memberSocket = io.sockets.sockets.get(socketId);
        if (memberSocket) {
            memberSocket.mySeatId = null;
            memberSocket.roomEntryAccepted = false;
            memberSocket.entryAcceptedAt = 0;
            memberSocket.leave(rId);
        }
    });

    console.log(`[Zen Pond] Session force-closed: ${rId} | ${expectedSessionKey}`);
}

function scheduleRoomSessionEnd(roomId, sessionKey, endAt) {
    const rId = roomId || 'main';
    const room = getRoomData(rId);
    const announceDelay = Math.max(0, endAt - Date.now());

    room.sessionEndTimer = setTimeout(() => {
        if (room.sessionKey !== sessionKey || room.sessionClosed || room.sessionEnding) return;
        room.sessionEnding = true;
        io.to(rId).emit('session-ending', {
            roomId: rId,
            sessionKey,
            endsAt: endAt,
            message: 'Phiên an tọa đã đến giờ kết thúc. Cảm ơn bạn đã cùng Du Sen giữ gìn khoảng lặng. Không gian sẽ khép lại sau 1 phút.'
        });
        console.log(`[Zen Pond] Session ending notice: ${rId} | ${sessionKey}`);

        const forceDelay = Math.max(0, endAt + 60000 - Date.now());
        room.sessionForceExitTimer = setTimeout(() => {
            forceEndRoomSession(rId, sessionKey);
        }, forceDelay);
    }, announceDelay);
}

function applyRoomSession(roomId, sessionKey) {
    const rId = roomId || 'main';
    if (typeof sessionKey !== 'string' || !sessionKey.trim()) return false;
    const normalizedKey = sessionKey.trim();
    const parts = normalizedKey.split('|');
    if (parts.length !== 4 || parts[0] !== rId) return false;
    const incomingTimestamp = getSessionStartTimestamp(normalizedKey);
    const endAt = getSessionEndTimestamp(normalizedKey);
    if (incomingTimestamp === null || endAt === null) return false;

    const room = getRoomData(rId);
    if (room.sessionKey === normalizedKey) return false;
    // Do not let a new session cancel the previous session's one-minute closure window.
    if (room.sessionKey && room.sessionEndAt && Date.now() >= room.sessionEndAt && !room.sessionClosed) return false;
    if (room.sessionEnding && !room.sessionClosed) return false;
    if (room.sessionKey) {
        const currentTimestamp = getSessionStartTimestamp(room.sessionKey);
        if (currentTimestamp !== null && incomingTimestamp < currentTimestamp) return false;
    }

    clearRoomSessionTimers(room);
    room.sessionKey = normalizedKey;
    room.sessionEndAt = endAt;
    room.sessionEnding = false;
    room.sessionClosed = false;
    if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
    room.specialAudioEndTimer = null;
    room.audioMode = 'playlist';
    room.audioResumeMode = 'playlist';
    room.audioPaused = false;
    room.audioPausedPosition = 0;
    room.specialAudioId = '';
    room.specialAudioUrl = '';
    room.specialAudioTitle = '';
    room.specialAudioStartTime = 0;
    room.specialAudioDuration = 0;
    room.trackStartTime = Date.now();
    room.totalHearts = 0;
    Object.keys(room.lanternCounts).forEach(word => { room.lanternCounts[word] = 0; });
    io.to(rId).emit('session-counters-reset', {
        sessionKey: normalizedKey,
        totalHearts: 0,
        lanternCounts: { ...room.lanternCounts }
    });
    scheduleRoomSessionEnd(rId, normalizedKey, endAt);
    console.log(`[Zen Pond] New session: ${rId} | ${normalizedKey} | Counters reset | Ends ${new Date(endAt).toISOString()}`);
    return true;
}

// Xác thực các yêu cầu nội bộ ký HMAC từ WordPress.
function verifyWordPressSignedRequest(req, res) {
    const timestamp = String(req.get('X-DZP-Timestamp') || '');
    const signature = String(req.get('X-DZP-Signature') || '');
    const rawBody = Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.from('');
    const requestTime = Number(timestamp);
    if (!IDENTITY_SECRET || !Number.isFinite(requestTime) || Math.abs(Math.floor(Date.now() / 1000) - requestTime) > 90 || !/^[a-f0-9]{64}$/i.test(signature)) {
        res.status(401).json({ ok: false, message: 'Unauthorized request.' });
        return null;
    }
    const expected = crypto.createHmac('sha256', IDENTITY_SECRET).update(timestamp + '.' + rawBody.toString('utf8')).digest('hex');
    const receivedBuffer = Buffer.from(signature, 'hex');
    const expectedBuffer = Buffer.from(expected, 'hex');
    if (receivedBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(receivedBuffer, expectedBuffer)) {
        res.status(401).json({ ok: false, message: 'Invalid signature.' });
        return null;
    }
    return req.body && typeof req.body === 'object' ? req.body : {};
}

function getAcceptedRoomUser(roomId, userId) {
    const wantedRoom = String(roomId || 'main');
    const wantedUser = Number(userId);
    for (const candidate of io.sockets.sockets.values()) {
        const members = io.sockets.adapter.rooms.get(wantedRoom);
        if (!candidate.connected || candidate.identityBlocked || !candidate.userId ||
            Number(candidate.userId) !== wantedUser || candidate.roomId !== wantedRoom ||
            candidate.roomEntryAccepted !== true || !candidate.entryAcceptedAt || !members || !members.has(candidate.id)) continue;
        return candidate;
    }
    return null;
}

// Presence is authoritative only after a successful request-entry acknowledgement.
app.post('/chat/room-users', (req, res) => {
    const data = verifyWordPressSignedRequest(req, res);
    if (!data) return;
    const requesterId = Number(data.requesterId);
    const roomId = String(data.roomId || '');
    if (!Number.isSafeInteger(requesterId) || requesterId <= 0 || !['main', 'room_2'].includes(roomId)) {
        return res.status(400).json({ ok: false, message: 'Invalid requester or room.' });
    }
    if (!getAcceptedRoomUser(roomId, requesterId)) {
        return res.status(403).json({ ok: false, message: 'Requester is not an accepted participant in this room.' });
    }
    const users = new Map();
    for (const candidate of io.sockets.sockets.values()) {
        const members = io.sockets.adapter.rooms.get(roomId);
        if (!candidate.connected || candidate.identityBlocked || !candidate.userId ||
            Number(candidate.userId) === requesterId || candidate.roomId !== roomId ||
            candidate.roomEntryAccepted !== true || !candidate.entryAcceptedAt || !members || !members.has(candidate.id)) continue;
        if (!users.has(Number(candidate.userId))) users.set(Number(candidate.userId), {
            userId: Number(candidate.userId),
            name: String(candidate.userName || 'Lữ khách').slice(0, 80),
            avatar: String(candidate.userAvatar || '').slice(0, 1024)
        });
    }
    return res.json({ ok: true, users: Array.from(users.values()) });
});

app.post('/chat/presence-check', (req, res) => {
    const data = verifyWordPressSignedRequest(req, res);
    if (!data) return;
    const requesterId = Number(data.requesterId);
    const targetUserId = Number(data.targetUserId);
    const roomId = String(data.roomId || '');
    if (!Number.isSafeInteger(requesterId) || requesterId <= 0 || !Number.isSafeInteger(targetUserId) || targetUserId <= 0 || requesterId === targetUserId || !['main', 'room_2'].includes(roomId)) {
        return res.status(400).json({ ok: false, message: 'Invalid presence query.' });
    }
    const requester = getAcceptedRoomUser(roomId, requesterId);
    const target = getAcceptedRoomUser(roomId, targetUserId);
    if (!requester || !target) return res.status(409).json({ ok: false, message: 'Both users must be accepted participants in the same room.' });
    return res.json({ ok: true, roomId, requesterId, targetUserId, checkedAt: Date.now() });
});

app.post('/chat/notify', (req, res) => {
    const data = verifyWordPressSignedRequest(req, res);
    if (!data) return;
    const ids = Array.isArray(data.recipientIds) ? data.recipientIds.map(Number).filter(id => Number.isSafeInteger(id) && id > 0).slice(0, 100) : [];
    const conversationId = Number(data.conversationId);
    if (!ids.length || !Number.isSafeInteger(conversationId) || conversationId <= 0) return res.status(400).json({ ok: false, message: 'Invalid notification payload.' });
    const recipients = new Set(ids);
    let delivered = 0;
    for (const candidate of io.sockets.sockets.values()) {
        if (!candidate.connected || !candidate.userId || !recipients.has(Number(candidate.userId))) continue;
        candidate.emit('dzp-chat-notification', { conversationId, at: Date.now() });
        delivered++;
    }
    return res.json({ ok: true, delivered });
});

// Admin control endpoint: chỉ nhận lệnh ký HMAC từ WordPress đã xác thực.
app.post('/admin/audio-control', (req, res) => {
    const timestamp = String(req.get('X-DZP-Timestamp') || '');
    const signature = String(req.get('X-DZP-Signature') || '');
    const rawBody = Buffer.isBuffer(req.rawBody) ? req.rawBody : Buffer.from('');
    const requestTime = Number(timestamp);
    if (!IDENTITY_SECRET || !Number.isFinite(requestTime) || Math.abs(Math.floor(Date.now() / 1000) - requestTime) > 90 || !/^[a-f0-9]{64}$/i.test(signature)) {
        return res.status(401).json({ ok: false, message: 'Unauthorized request.' });
    }
    const expected = crypto.createHmac('sha256', IDENTITY_SECRET).update(timestamp + '.' + rawBody.toString('utf8')).digest('hex');
    const receivedBuffer = Buffer.from(signature, 'hex');
    const expectedBuffer = Buffer.from(expected, 'hex');
    if (receivedBuffer.length !== expectedBuffer.length || !crypto.timingSafeEqual(receivedBuffer, expectedBuffer)) {
        return res.status(401).json({ ok: false, message: 'Invalid signature.' });
    }

    const data = req.body && typeof req.body === 'object' ? req.body : {};
    const roomId = String(data.roomId || '');
    const action = String(data.action || '');
    if (!['main', 'room_2'].includes(roomId) || !['pause', 'resume', 'special', 'stop-special', 'update-playlist'].includes(action)) {
        return res.status(400).json({ ok: false, message: 'Invalid room or action.' });
    }

    const room = getRoomData(roomId);
    if (Array.isArray(data.playlist)) {
        const normalized = data.playlist.map(item => ({
            title: String(item?.title || '').slice(0, 180),
            url: String(item?.url || '').trim()
        })).filter(item => /^https?:\/\//i.test(item.url));
        if (normalized.length) {
            const hadPlaylist = room.activePlaylist.length > 0;
            room.activePlaylist = normalized;
            room.playlistFingerprint = crypto.createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
            room.currentTrackIndex = Math.min(room.currentTrackIndex, normalized.length - 1);
            if (!hadPlaylist) room.trackStartTime = Date.now();
        }
    }

    if (action === 'update-playlist') {
        const normalized = Array.isArray(data.playlist) ? data.playlist.map(item => ({
            title: String(item?.title || '').slice(0, 180),
            url: String(item?.url || '').trim()
        })).filter(item => /^https?:\/\//i.test(item.url)) : [];
        if (!normalized.length) return res.status(400).json({ ok: false, message: 'Playlist is empty.' });
        room.activePlaylist = normalized;
        room.playlistFingerprint = crypto.createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
        room.currentTrackIndex = 0;
        if (room.audioMode === 'special') {
            room.specialInterruptedTrackIndex = 0;
        } else if (room.audioMode === 'paused') {
            room.audioResumeMode = 'playlist';
            room.audioPausedPosition = 0;
        } else {
            room.audioMode = 'playlist';
            room.audioPaused = false;
            room.trackStartTime = Date.now();
            io.to(roomId).emit('sync-audio-track', { ...getRoomAudioState(room), fadeInMs: 1800 });
        }
        console.log(`[Zen Pond] Admin updated playlist: ${roomId} (${normalized.length} tracks)`);
        return res.json({ ok: true, roomId, action });
    }

    if (action === 'stop-special') {
        const specialIsActive = room.audioMode === 'special' ||
            (room.audioMode === 'paused' && room.audioResumeMode === 'special');
        if (!specialIsActive) {
            return res.status(409).json({ ok: false, roomId, action, message: 'No special audio is currently playing.' });
        }
        if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
        room.specialAudioEndTimer = null;
        if (room.activePlaylist.length) {
            room.currentTrackIndex = (Number(room.specialInterruptedTrackIndex || 0) + 1) % room.activePlaylist.length;
        }
        room.trackStartTime = Date.now();
        room.audioResumeMode = 'playlist';
        room.audioPausedPosition = 0;
        room.audioMode = 'paused';
        room.audioPaused = true;
        room.specialAudioId = '';
        room.specialAudioUrl = '';
        room.specialAudioTitle = '';
        room.specialAudioStartTime = 0;
        room.specialAudioDuration = 0;
        io.to(roomId).emit('audio-pause', { roomId, fadeOutMs: 1200, audioState: getRoomAudioState(room) });
        console.log(`[Zen Pond] Admin stopped special audio: ${roomId}`);
        return res.json({ ok: true, roomId, action });
    }

    if (action === 'pause') {
        if (room.audioMode !== 'paused') {
            // Khi Admin dừng audio đặc biệt, hủy hẳn phần audio override.
            // Lần tiếp tục sau đó phải quay về playlist, không phát lại audio đặc biệt.
            if (room.audioMode === 'special') {
                if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
                room.specialAudioEndTimer = null;
                if (room.activePlaylist.length) {
                    room.currentTrackIndex = (Number(room.specialInterruptedTrackIndex || 0) + 1) % room.activePlaylist.length;
                }
                room.trackStartTime = Date.now();
                room.audioResumeMode = 'playlist';
                room.audioPausedPosition = 0;
                room.specialAudioId = '';
                room.specialAudioUrl = '';
                room.specialAudioTitle = '';
                room.specialAudioStartTime = 0;
                room.specialAudioDuration = 0;
            } else {
                const state = getRoomAudioState(room);
                room.audioResumeMode = 'playlist';
                room.audioPausedPosition = Math.max(0, (Date.now() - Number(state.trackStartTime || Date.now())) / 1000);
            }
            room.audioMode = 'paused';
            room.audioPaused = true;
        }
        io.to(roomId).emit('audio-pause', { roomId, fadeOutMs: 1600, audioState: getRoomAudioState(room) });
        console.log(`[Zen Pond] Admin paused room audio: ${roomId}`);
        return res.json({ ok: true, roomId, action });
    }

    if (action === 'resume') {
        if (room.audioMode === 'paused') {
            const resumeMode = room.audioResumeMode || 'playlist';
            const startTime = Date.now() - Math.max(0, Number(room.audioPausedPosition) || 0) * 1000;
            room.audioMode = resumeMode;
            room.audioPaused = false;
            if (resumeMode === 'special') {
                room.specialAudioStartTime = startTime;
                scheduleSpecialAudioEnd(roomId);
            } else room.trackStartTime = startTime;
        }
        io.to(roomId).emit('audio-resume', { roomId, audioState: getRoomAudioState(room), fadeInMs: 1800 });
        console.log(`[Zen Pond] Admin resumed room audio: ${roomId}`);
        return res.json({ ok: true, roomId, action });
    }

    const special = data.specialAudio && typeof data.specialAudio === 'object' ? data.specialAudio : {};
    const specialUrl = String(special.url || '').trim();
    if (!/^https?:\/\//i.test(specialUrl) || specialUrl.length > 2048) {
        return res.status(400).json({ ok: false, message: 'Invalid special audio URL.' });
    }
    room.specialInterruptedTrackIndex = room.currentTrackIndex;
    if (room.specialAudioEndTimer) clearTimeout(room.specialAudioEndTimer);
    room.specialAudioEndTimer = null;
    room.specialAudioDuration = 0;
    room.specialAudioId = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex');
    room.specialAudioUrl = specialUrl;
    room.specialAudioTitle = String(special.title || 'Audio đặc biệt').slice(0, 180);
    room.specialAudioStartTime = Date.now() + 1600;
    room.audioMode = 'special';
    room.audioResumeMode = 'playlist';
    room.audioPaused = false;
    room.audioPausedPosition = 0;
    io.to(roomId).emit('audio-special-start', {
        roomId,
        specialAudioId: room.specialAudioId,
        url: room.specialAudioUrl,
        title: room.specialAudioTitle,
        trackStartTime: room.specialAudioStartTime,
        fadeOutMs: 1400,
        fadeInMs: 1600
    });
    console.log(`[Zen Pond] Admin started special audio: ${roomId} | ${room.specialAudioId}`);
    return res.json({ ok: true, roomId, action, specialAudioId: room.specialAudioId });
});

// 4. SOCKET CONNECTION
io.on('connection', (socket) => {
    socket.connectedAt = Date.now();
    socket.entryAcceptedAt = 0;
    socket.roomEntryAccepted = false;
    let currentRoomId = socket.handshake.query.roomId || 'main';
    socket.roomId = currentRoomId;
    socket.join(currentRoomId);

    // Chỉ ghi metadata thời gian/kết nối, không ghi email hay token người dùng.
    console.log(`[Zen Pond] Socket connected | room=${currentRoomId} | transport=${socket.conn.transport.name} | serverUptimeMs=${Date.now() - SERVER_STARTED_AT} | socket=${socket.id.slice(0, 8)}`);
    socket.conn.on('upgrade', (transport) => {
        console.log(`[Zen Pond] Socket transport upgraded | room=${socket.roomId} | transport=${transport.name} | socket=${socket.id.slice(0, 8)}`);
    });

    // Chỉ trả số vị trí X/Y khi kết nối; không gọi request-entry và không claim ghế.
    sendSeatAvailabilitySnapshot(socket, currentRoomId);

    const handshakeAuth = socket.handshake.auth || {};
    const identity = verifyIdentityToken(handshakeAuth.identityToken);
    socket.userId = identity ? identity.userId : null;
    socket.identityKey = identity ? identity.identityKey : null;
    socket.deviceId = cleanDeviceId(handshakeAuth.deviceId);
    socket.identityBlocked = false;
    socket.duplicateNoticeSent = false;

    function syncSocketRoom(incomingRoomId) {
        const targetRoomId = incomingRoomId || socket.roomId || 'main';
        if (targetRoomId !== socket.roomId) {
            releaseRoomEntry(socket.roomId, socket.id);
            socket.leave(socket.roomId);
            socket.roomId = targetRoomId;
            socket.roomEntryAccepted = false;
            socket.entryAcceptedAt = 0;
            socket.identityBlocked = false;
            socket.duplicateNoticeSent = false;
            socket.mySeatId = null;
        }
        // Session closure can remove a socket from the room without disconnecting it.
        // Rejoin only after an explicit request-entry from the user.
        const members = io.sockets.adapter.rooms.get(targetRoomId);
        if (!members || !members.has(socket.id)) socket.join(targetRoomId);
    }

    // 5. NHẬN YÊU CẦU VÀO KHÔNG GIAN
    socket.on('request-entry', (data, acknowledge) => {
        const reply = (payload) => { if (typeof acknowledge === 'function') acknowledge(payload); };
        const userObj = typeof data === 'object' && data !== null ? data : {};
        const targetRoomId = userObj.roomId || socket.roomId || 'main';
        socket.roomEntryAccepted = false;
        socket.entryAcceptedAt = 0;
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
            const message = 'Không thể xác thực tài khoản Du Sen. Vui lòng tải lại trang hoặc đăng nhập lại trước khi chọn chỗ ngồi.';
            socket.emit('identity-auth-required', { message });
            reply({ ok: false, code: 'identity-auth-required', message });
            return;
        }

        socket.deviceId = cleanDeviceId(userObj.deviceId || socket.deviceId);
        if (!socket.deviceId) {
            socket.identityBlocked = true;
            const message = 'Trình duyệt chưa tạo được mã thiết bị an toàn. Vui lòng bật bộ nhớ trình duyệt và tải lại trang.';
            socket.emit('identity-auth-required', { message });
            reply({ ok: false, code: 'device-id-missing', message });
            return;
        }

        socket.userName = userObj.name || userObj.userName || 'Lữ khách';
        socket.userAvatar = userObj.avatar || userObj.userAvatar || '';
        const wasIdentityBlocked = socket.identityBlocked;
        socket.identityBlocked = false;

        // The first accepted entrant registers this room's session end with the server.
        if (userObj.sessionKey) applyRoomSession(socket.roomId, userObj.sessionKey);
        const roomSession = getRoomData(socket.roomId);
        if (roomSession.sessionEnding || roomSession.sessionClosed ||
            (roomSession.sessionEndAt && Date.now() >= roomSession.sessionEndAt)) {
            const message = 'Phiên hiện tại đã kết thúc. Vui lòng chờ đến khung giờ mở cửa tiếp theo.';
            reply({ ok: false, code: 'session-ended', message });
            socket.emit('session-force-exit', {
                roomId: socket.roomId,
                sessionKey: roomSession.sessionKey,
                message
            });
            socket.roomEntryAccepted = false;
            socket.entryAcceptedAt = 0;
            socket.leave(socket.roomId);
            return;
        }

        const existing = resolveExistingUserSeat(socket, socket.roomId);
        const roomEntry = existing.allowed ? reserveRoomEntry(socket, socket.roomId) : { allowed: false, reason: existing.reason };
        if (!existing.allowed || !roomEntry.allowed) {
            // Snapshot chỉ để vẽ trạng thái phòng; không cấp quyền tham gia.
            sendRoomInitialState(socket, socket.roomId);

            const reason = !existing.allowed ? existing.reason : roomEntry.reason;
            const message = reason === 'grace'
                ? 'Tài khoản của bạn vừa mất kết nối trên một thiết bị khác. Vui lòng chờ tối đa 30 giây để hệ thống xác nhận phiên cũ đã kết thúc rồi thử lại.'
                : 'Tài khoản của bạn đang tham gia không gian này trên một thiết bị khác. Vui lòng tiếp tục trên thiết bị đang sử dụng phòng.';
            showDuplicateSession(socket, message);
            reply({ ok: false, code: 'duplicate-session', message });
            io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
            return;
        }

        socket.duplicateNoticeSent = false;
        if (wasIdentityBlocked) socket.emit('identity-session-available', {});

        socket.entryAcceptedAt = Date.now();
        socket.roomEntryAccepted = true;
        console.log(`[Zen Pond] Room entry accepted | room=${socket.roomId} | sinceConnectMs=${socket.entryAcceptedAt - socket.connectedAt} | serverUptimeMs=${socket.entryAcceptedAt - SERVER_STARTED_AT} | socket=${socket.id.slice(0, 8)}`);
        sendRoomInitialState(socket, socket.roomId);
        reply({ ok: true });
        io.to(socket.roomId).emit('update-online-count', getRoomOnlineCount(socket.roomId));
    });

    socket.on('session-sync', (data) => {
        if (!data || !data.sessionKey || socket.identityBlocked) return;
        // Legacy clients may still emit this event. It must never roll an active
        // room into a new session; only an explicit request-entry can do that.
        if (data.roomId && data.roomId !== socket.roomId) return;
        const hasRoomEntry = Array.from(roomEntryOwners.values()).some(owner => owner.socketId === socket.id);
        if (!hasRoomEntry) return;
        const room = getRoomData(socket.roomId);
        if (room.sessionKey && room.sessionKey !== data.sessionKey) return;
        applyRoomSession(socket.roomId, data.sessionKey);
        if (room.sessionClosed) {
            socket.emit('session-force-exit', { roomId: socket.roomId, sessionKey: room.sessionKey });
        } else if (room.sessionEnding) {
            socket.emit('session-ending', {
                roomId: socket.roomId,
                sessionKey: room.sessionKey,
                endsAt: room.sessionEndAt,
                message: 'Phiên an tọa đã đến giờ kết thúc. Cảm ơn bạn đã cùng Du Sen giữ gìn khoảng lặng. Không gian sẽ khép lại sau 1 phút.'
            });
        }
    });

    // Reconnect-only lifecycle recovery. This never claims/reclaims a seat or
    // reserves room entry; it only restores the server timer for the same session.
    socket.on('session-watch', (data) => {
        if (!data || !data.sessionKey || socket.identityBlocked || !socket.userId) return;
        if (data.roomId && data.roomId !== socket.roomId) return;

        const room = getRoomData(socket.roomId);
        if (room.sessionKey && room.sessionKey !== data.sessionKey) {
            socket.emit('session-force-exit', { roomId: socket.roomId, sessionKey: room.sessionKey });
            return;
        }

        applyRoomSession(socket.roomId, data.sessionKey);
        if (room.sessionClosed) {
            socket.emit('session-force-exit', { roomId: socket.roomId, sessionKey: room.sessionKey });
        } else if (room.sessionEnding) {
            socket.emit('session-ending', {
                roomId: socket.roomId,
                sessionKey: room.sessionKey,
                endsAt: room.sessionEndAt,
                message: 'Phiên an tọa đã đến giờ kết thúc. Cảm ơn bạn đã cùng Du Sen giữ gìn khoảng lặng. Không gian sẽ khép lại sau 1 phút.'
            });
        }
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
        if (getRoomData(targetRoomId).sessionClosed) {
            socket.emit('claim-denied', { code: 'session-ended', message: 'Phiên đã kết thúc. Vui lòng chờ phiên tiếp theo.' });
            return;
        }
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
                if (currentUserSeat.occupiedBy.deviceId && currentUserSeat.occupiedBy.deviceId === socket.deviceId) {
                    previousSocket.identityBlocked = true;
                    previousSocket.mySeatId = null;
                    previousSocket.emit('same-device-session-replaced', {});
                    currentUserSeat.occupiedBy.socketId = socket.id;
                    currentUserSeat.occupiedBy.disconnectedAt = 0;
                    socket.mySeatId = currentUserSeat.id;
                    emitSeatState(socket.roomId);
                    socket.emit('seat-reclaimed', { seatId: currentUserSeat.id });
                    return;
                }
                const message = 'Bạn đang tham gia không gian trên một thiết bị khác. Vui lòng tiếp tục sử dụng thiết bị đang giữ chỗ ngồi hiện tại.';
                showDuplicateSession(socket, message);
                socket.emit('claim-denied', { code: 'duplicate-session', message });
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
        const claimAt = Date.now();
        const sinceConnectMs = socket.connectedAt ? claimAt - socket.connectedAt : -1;
        const sinceEntryMs = socket.entryAcceptedAt ? claimAt - socket.entryAcceptedAt : -1;
        console.log(`[Zen Pond] Seat claimed | seat=${targetSeat.id} | room=${socket.roomId} | sinceConnectMs=${sinceConnectMs} | sinceEntryMs=${sinceEntryMs} | hasDisplayName=${Boolean(socket.userName && socket.userName !== 'Lữ khách')} | socket=${socket.id.slice(0, 8)}`);
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
        releaseRoomEntry(targetRoomId, socket.id);
        socket.roomEntryAccepted = false;
        socket.entryAcceptedAt = 0;
        socket.mySeatId = null;
        if (typeof acknowledge === 'function') acknowledge({ ok: true });
    });

    // Playlist được khởi tạo một lần cho mỗi phòng. Thiết bị kết nối sau
    // không thể ghi đè danh sách phát đang được cả phòng sử dụng.
    socket.on('sync-playlist', (data) => {
        if (socket.identityBlocked) return;
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;

        const currentRoom = getRoomData(socket.roomId);
        if (currentRoom.sessionClosed) return;
        if (!data || !Array.isArray(data.playlist) || data.playlist.length === 0) return;

        // Playlist đã được khởi tạo bởi người vào trước: không ghi đè danh sách
        // đang chạy, nhưng phải gửi snapshot hiện tại riêng cho người vừa vào.
        // Nếu chỉ return ở đây, client mới sẽ không nhận lại lệnh phát vì init-state
        // được gửi trước thao tác vào phòng và bị frontend bỏ qua khi popup còn mở.
        if (currentRoom.activePlaylist.length > 0) {
            socket.emit('sync-audio-track', getRoomAudioState(currentRoom));
            return;
        }

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
        currentRoom.playlistFingerprint = crypto.createHash('sha256')
            .update(JSON.stringify(normalizedPlaylist)).digest('hex');

        // Không để client vừa vào ghi đè trạng thái pause/special do Admin đang điều khiển.
        const preservingAdminOverride = currentRoom.audioMode === 'special' ||
            (currentRoom.audioMode === 'paused' && currentRoom.audioResumeMode === 'special');
        if (preservingAdminOverride) return;

        currentRoom.currentTrackIndex = 0;
        // Bắt đầu mốc thời gian tại lúc playlist chuẩn được khởi tạo,
        // tránh tính nhầm thời gian từ lúc room object được tạo trước đó.
        currentRoom.trackStartTime = Date.now();

        // Phát trạng thái chuẩn ngay khi playlist đầu tiên được đăng ký.
        io.to(socket.roomId).emit('sync-audio-track', getRoomAudioState(currentRoom));
        console.log(`[Zen Pond] Audio playlist initialized: room '${socket.roomId}' (${normalizedPlaylist.length} tracks)`);
    });

    // Chỉ chấp nhận một lần kết thúc cho đúng phiên phát hiện tại.
    // Mọi thiết bị gửi cùng trackIndex + trackStartTime; sau lần đầu,
    // trackStartTime đổi nên các sự kiện đến trễ đều bị loại bỏ.
    socket.on('track-ended', (data) => {
        if (socket.identityBlocked) return;
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;

        const currentRoom = getRoomData(socket.roomId);
        if (currentRoom.sessionClosed || currentRoom.audioMode !== 'playlist') return;
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
        io.to(socket.roomId).emit('sync-audio-track', { ...getRoomAudioState(currentRoom), fadeInMs: 0 });
    });

    socket.on('special-audio-metadata', (data) => {
        if (socket.identityBlocked) return;
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        const room = getRoomData(socket.roomId);
        const duration = Number(data && data.duration);
        if (room.audioMode !== 'special' || room.audioPaused || !data ||
            String(data.specialAudioId || '') !== String(room.specialAudioId || '') ||
            Number(data.trackStartTime) !== Number(room.specialAudioStartTime) ||
            !Number.isFinite(duration) || duration <= 0 || duration > 6 * 60 * 60) return;
        room.specialAudioDuration = duration;
        scheduleSpecialAudioEnd(socket.roomId);
    });

    socket.on('special-audio-ended', (data) => {
        if (socket.identityBlocked) return;
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        const room = getRoomData(socket.roomId);
        if (!data || String(data.specialAudioId || '') !== String(room.specialAudioId || '') ||
            Number(data.trackStartTime) !== Number(room.specialAudioStartTime)) return;
        completeSpecialAudio(socket.roomId, room.specialAudioId, room.specialAudioStartTime);
    });

    socket.on('send-heart', (data) => {
        if (socket.identityBlocked) return;
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        const currentRoom = getRoomData(socket.roomId);
        if (currentRoom.sessionClosed) return;
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
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        const currentRoom = getRoomData(socket.roomId);
        if (currentRoom.sessionClosed) return;
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
        const targetRoomId = (data && data.roomId) || socket.roomId;
        if (getRoomData(targetRoomId).sessionClosed) return;
        if (data && data.roomId) syncSocketRoom(data.roomId);
        if (blockIfUserIsSeatedElsewhere(socket, socket.roomId)) return;
        const currentRoom = getRoomData(socket.roomId);
        if (currentRoom.sessionClosed) return;
        const wishText = (data && data.text) ? String(data.text).trim() : '';
        if (!wishText) return;
        io.to(socket.roomId).emit('dzp_receive_wish', {
            text: wishText,
            senderSocketId: socket.id,
            timestamp: Date.now()
        });
    });

    socket.on('disconnect', (reason) => {
        console.log(`[Zen Pond] Socket disconnected | room=${socket.roomId || currentRoomId} | reason=${String(reason || 'unknown')} | socket=${socket.id.slice(0, 8)}`);
        const roomId = socket.roomId || currentRoomId;
        socket.roomEntryAccepted = false;
        socket.entryAcceptedAt = 0;
        markRoomEntryDisconnected(socket);
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
        if (room.audioMode === 'special' || room.audioMode === 'paused' || (room.activePlaylist && room.activePlaylist.length > 0)) {
            io.to(roomId).emit('audio-sync-heartbeat', getRoomAudioState(room));
        }
    });
}, 5000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Zen Pond Realtime Server v10.5.0 running on port ${PORT}`);
    if (!IDENTITY_SECRET) {
        console.error('[Zen Pond] Missing DZP_SOCKET_IDENTITY_SECRET. Account identity validation is disabled; seat claims will be rejected.');
    }
});
