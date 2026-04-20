// @ts-check
const pathnameParts = window.location.pathname.split('/');
export const roomCode = pathnameParts[pathnameParts.length - 1];
export const socket = new WebSocket("ws://" + window.location.host + "/ws/" + roomCode);

export function sendToServer(messageType, contents) {
    if (socket.readyState === WebSocket.OPEN) {
        const payload = {
            message_type: messageType,
            contents: contents
        };
        socket.send(JSON.stringify(payload));
    } else {
        console.warn("Tried to send a message, but the socket isn't open yet!");
    }
}