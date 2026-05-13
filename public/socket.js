// @ts-check
const pathnameParts = window.location.pathname.split('/');
export const roomCode = pathnameParts[pathnameParts.length - 1];

const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const host = window.location.host;

export const socket = new WebSocket(`${protocol}//${host}/ws/${roomCode}`);

export function sendToServer(messageType, content) {
    if (socket.readyState === WebSocket.OPEN) {
        const payload = {
            message_type: messageType,
            content: content
        };
        socket.send(JSON.stringify(payload));
    } else {
        console.warn("Tried to send a message, but the socket isn't open yet!");
    }
}