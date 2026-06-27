import {Container} from '@cloudflare/containers';
import {connect} from 'cloudflare:sockets';
export class XrayContainer extends Container {
    defaultPort = 8080;
    sleepAfter = '11h';
}
const uuid = '83bcd3fd-6d20-4627-993b-17d322b43145';
const bufferSize = 512 * 1024;
const startThreshold = 50 * 1024 * 1024;
const maxChunkLen = 64 * 1024;
const flushTime = 6;
const textDecoder = new TextDecoder();
const uuidBytes = new Uint8Array(16), offsets = [0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4];
for (let i = 0, c; i < 16; i++) uuidBytes[i] = (((c = uuid.charCodeAt(i * 2 + offsets[i])) > 64 ? c + 9 : c) & 0xF) << 4 | (((c = uuid.charCodeAt(i * 2 + offsets[i] + 1)) > 64 ? c + 9 : c) & 0xF);
const createConnect = (hostname, port, socket = connect({hostname, port})) => socket.opened.then(() => socket);
const concurrentConnect = (hostname, port) => {
    let settled = false, winner = null;
    const sockets = new Array(4);
    const closeSocket = socket => {try {socket?.close()} catch {}};
    const attempts = Array.from({length: 4}, (_, i) => {
        const socket = connect({hostname, port});
        sockets[i] = socket;
        return createConnect(hostname, port, socket).then(openedSocket => {
            if (settled && openedSocket !== winner) closeSocket(openedSocket);
            return openedSocket;
        });
    });
    return Promise.any(attempts).then(socket => {
        settled = true, winner = socket;
        for (const other of sockets) if (other !== socket) closeSocket(other);
        return socket;
    }, err => {
        settled = true;
        for (const socket of sockets) closeSocket(socket);
        throw err;
    });
};
const chunkIdxLookup = new Uint8Array([
    0, 0, 0, 1, 2, 3, 4, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 10, 10,
    10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12
]);
const lowerBounds = new Uint16Array([1024, 1536, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 12288, 20480, 28672]);
const manualPipe = async (readable, writable, close) => {
    const safeBufferSize = bufferSize - maxChunkLen, halfChunkLen = maxChunkLen >> 1, directBufSize = halfChunkLen * 3, fastFlushOffset = bufferSize >> 1;
    let buffer, bufferView, spareBuffer = new ArrayBuffer(maxChunkLen);
    let offset = 0, totalBytes = 0, timerId = null, resume = null, isReading = false, needsFlush = false, protectFlush = false;
    let directBuf = new Uint8Array(directBufSize), directOff = 0, directTimer = null, isClose = false, fastFlush = true;
    let time = 1;
    let globalCount = new Uint32Array(14), globalBytes = new Uint32Array(14);
    let statCount = 0, totalCount = 0, totalGlobalBytes = 0;
    const flushDirect = () => {
        directTimer && (clearTimeout(directTimer), directTimer = null);
        directOff > 0 && !isClose && (directOff === directBufSize ? (writable.send(directBuf), directBuf = new Uint8Array(directBufSize)) : writable.send(directBuf.slice(0, directOff)));
        directOff = 0;
    };
    const flushBuffer = () => {
        if (isReading) return needsFlush = true;
        fastFlush = offset < fastFlushOffset;
        if (offset > 0 && !isClose) {
            if (offset > safeBufferSize) {
                writable.send(bufferView.subarray(0, offset));
                fastFlush || (buffer = new ArrayBuffer(bufferSize), bufferView = new Uint8Array(buffer));
            } else {
                writable.send(bufferView.slice(0, offset));
            }
            offset = 0;
        }
        fastFlush && (totalBytes = 0, directBuf ||= new Uint8Array(directBufSize), buffer = null, bufferView = null);
        needsFlush = false, protectFlush = false, timerId && (clearTimeout(timerId), timerId = null), resume?.(), resume = null;
    };
    const reader = readable.getReader({mode: 'byob'});
    try {
        while (true) {
            if (fastFlush) {
                const {done, value} = await reader.read(new Uint8Array(spareBuffer));
                if (done) break;
                const chunkLen = value.byteLength;
                if (!chunkLen) continue;
                if (chunkLen >= halfChunkLen) {
                    flushDirect(), writable.send(value), spareBuffer = new ArrayBuffer(maxChunkLen);
                } else if (directOff + chunkLen > directBufSize) {
                    flushDirect(), directBuf.set(value, 0), directOff = chunkLen, directTimer = setTimeout(flushDirect, 1), spareBuffer = value.buffer;
                } else {
                    directBuf.set(value, directOff), directOff += chunkLen, directTimer ||= setTimeout(flushDirect, 1), spareBuffer = value.buffer;
                }
                if (chunkLen < 28672) {
                    totalBytes = 0;
                } else if ((totalBytes += chunkLen) > startThreshold) {
                    flushDirect(), fastFlush = false, buffer = new ArrayBuffer(bufferSize), bufferView = new Uint8Array(buffer), directBuf = null;
                }
            } else {
                const useSpare = offset > 0 && protectFlush;
                let readBuffer = buffer, readOffset = offset;
                isReading = offset > 0;
                useSpare && (readBuffer = spareBuffer, readOffset = 0, isReading = false);
                const {done, value} = await reader.read(new Uint8Array(readBuffer, readOffset, maxChunkLen));
                isReading = false;
                useSpare ? (bufferView.set(value, offset), spareBuffer = value.buffer) : (buffer = value.buffer, bufferView = new Uint8Array(buffer));
                if (done) break;
                const chunkLen = value.byteLength;
                if (!chunkLen) {
                    needsFlush && flushBuffer();
                    continue;
                }
                offset += chunkLen;
                if (needsFlush) {
                    flushBuffer();
                } else {
                    if (fastFlush) {
                        time = 1;
                    } else {
                        const idx = chunkLen >= 30720 ? 13 : chunkIdxLookup[chunkLen >> 9];
                        globalCount[idx]++, globalBytes[idx] += chunkLen, statCount++, totalCount++, totalGlobalBytes += chunkLen;
                        if (statCount > 16384) {
                            statCount = 0, totalCount >>>= 1, totalGlobalBytes >>>= 1;
                            for (let i = 0; i < 14; i++) globalCount[i] >>>= 1, globalBytes[i] >>>= 1;
                        }
                        let maxScore = -1, maxIdx = 0;
                        const byteFactor = 0.25 * totalCount / totalGlobalBytes;
                        for (let i = 0; i < 14; i++) {
                            const score = globalCount[i] + globalBytes[i] * byteFactor;
                            score > maxScore && (maxScore = score, maxIdx = i);
                        }
                        if (chunkLen < lowerBounds[maxIdx]) {
                            totalBytes = 0, time = 1;
                        } else if ((totalBytes += chunkLen) > startThreshold) {
                            time = flushTime;
                        }
                    }
                    timerId ||= setTimeout(flushBuffer, time), protectFlush = chunkLen < maxChunkLen;
                    offset > safeBufferSize && (time === flushTime ? await new Promise(r => resume = r) : flushBuffer());
                }
            }
        }
    } catch {close?.(), isClose = true} finally {isReading = false, flushDirect(), flushBuffer()}
};
const getEarlyData = request => {
    const refererHeader = request.headers.get('Referer');
    const protocolHeader = refererHeader || request.headers.get('sec-websocket-protocol');
    let earlyDataHeader = null;
    if (refererHeader) {
        earlyDataHeader = protocolHeader.slice(request.headers.get('host').length);
    } else if (protocolHeader) {
        earlyDataHeader = protocolHeader;
    }
    // @ts-ignore
    return earlyDataHeader ? Uint8Array.fromBase64(earlyDataHeader, {alphabet: 'base64url'}) : null;
};
const parseRequest = chunk => {
    if (chunk.byteLength < 24) return null;
    for (let i = 0; i < 16; i++) if (chunk[i + 1] !== uuidBytes[i]) return null;
    let offset = 19 + chunk[17];
    if (chunk.byteLength < offset + 3) return null;
    const port = (chunk[offset] << 8) | chunk[offset + 1];
    offset += 2;
    const addrType = chunk[offset++];
    let newOffset, hostname;
    if (addrType === 2) {
        if (chunk.byteLength < offset + 1) return null;
        const len = chunk[offset++];
        newOffset = offset + len;
        if (chunk.byteLength < newOffset) return null;
        hostname = textDecoder.decode(chunk.subarray(offset, newOffset));
    } else if (addrType === 1) {
        newOffset = offset + 4;
        if (chunk.byteLength < newOffset) return null;
        const bytes = chunk.subarray(offset, newOffset);
        hostname = `${bytes[0]}.${bytes[1]}.${bytes[2]}.${bytes[3]}`;
    } else {
        newOffset = offset + 16;
        if (chunk.byteLength < newOffset) return null;
        let ipv6Str = ((chunk[offset] << 8) | chunk[offset + 1]).toString(16);
        for (let i = 1; i < 8; i++) ipv6Str += ':' + ((chunk[offset + i * 2] << 8) | chunk[offset + i * 2 + 1]).toString(16);
        hostname = `[${ipv6Str}]`;
    }
    return {version: chunk[0], hostname, port, payload: chunk.subarray(newOffset)};
};
const createBufferedTcpWriter = (tcpWriter, close) => {
    const queue = new Array(4096);
    let head = 0, tail = 0, size = 0, coalesceBuffer = null, drainActive = false, closed = false;
    const closeWriter = () => {
        if (closed) return;
        closed = true;
        for (let i = 0; i < 4096; i++) queue[i] = null;
        close?.();
    };
    const drainQueue = async () => {
        if (closed) return;
        drainActive = true;
        try {
            while (size > 0 && !closed) {
                let chunk = queue[head];
                if (chunk.byteLength >= maxChunkLen) {
                    queue[head] = null, head = (head + 1) & 4095, size--;
                    await tcpWriter.write(chunk);
                    continue;
                }
                let mergedLength = 0;
                coalesceBuffer ||= new Uint8Array(maxChunkLen);
                while (size > 0) {
                    chunk = queue[head];
                    if (mergedLength + chunk.byteLength > maxChunkLen) break;
                    coalesceBuffer.set(chunk, mergedLength), mergedLength += chunk.byteLength;
                    queue[head] = null, head = (head + 1) & 4095, size--;
                }
                if (mergedLength > 0) await tcpWriter.write(coalesceBuffer.subarray(0, mergedLength));
            }
        } catch {closeWriter()} finally {
            drainActive = false;
            if (size > 0 && !closed) drainActive = true, queueMicrotask(drainQueue);
        }
    };
    return chunk => {
        if (closed) return false;
        const data = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
        if (!data.byteLength) return true;
        if (size === 4096) return closeWriter(), false;
        queue[tail] = data, tail = (tail + 1) & 4095, size++;
        if (!drainActive) drainActive = true, queueMicrotask(drainQueue);
        return true;
    };
};
const handleWebSocketConn = (webSocket, tcpSocket, parsedRequest) => {
    const tcpWriter = tcpSocket.writable.getWriter();
    let isClosed = false;
    const close = () => {
        if (isClosed) return;
        isClosed = true;
        try { tcpSocket.close(); } catch {}
        try { webSocket.close(); } catch {}
    };
    const tcpWrite = createBufferedTcpWriter(tcpWriter, close);
    // @ts-ignore
    webSocket.accept({allowHalfOpen: true}), webSocket.binaryType = "arraybuffer";
    webSocket.addEventListener("message", event => {tcpWrite(event.data)});
    webSocket.addEventListener("close", close);
    webSocket.addEventListener("error", close);
    manualPipe(tcpSocket.readable, webSocket, close).finally(close);
    webSocket.send(new Uint8Array([parsedRequest.version, 0]));
    parsedRequest.payload.byteLength && tcpWrite(parsedRequest.payload);
};
export default {
    async fetch(request, env) {
        const container = env.XRAY.getByName('default');
        if (request.headers.get('Upgrade') !== 'websocket') return new Response(null, {status: 404});
        if (request.url.includes('proxyall')) return container.fetch(request);
        const earlyData = getEarlyData(request);
        if (!earlyData) return container.fetch(request);
        const parsedRequest = parseRequest(earlyData);
        if (!parsedRequest) return container.fetch(request);
        const tcpSocket = await concurrentConnect(parsedRequest.hostname, parsedRequest.port).catch(() => null);
        if (!tcpSocket) return container.fetch(request);
        const {0: clientSocket, 1: webSocket} = new WebSocketPair();
        handleWebSocketConn(webSocket, tcpSocket, parsedRequest);
        return new Response(null, {status: 101, webSocket: clientSocket});
    },
    async scheduled(controller, env) {
        console.log(`[保活] Worker Cron 已触发，时间: ${new Date().toISOString()}`);
        const container = env.XRAY.getByName('default');
        try {
            const response = await container.fetch('http://localhost:8080/keepalive');
            console.log(`[保活] Container 保活成功，HTTP ${response.status}`);
        } catch (e) {
            console.error(`[保活] Container 保活失败: ${e.message}`);
        }
    }
};
