// @ts-check
import { roomCode, main_loop } from "./app.js";

const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const host = window.location.host;

let socket = new WebSocket(`${protocol}//${host}/ws/${roomCode}`);
socket.onopen = serverHandshake;
socket.onclose = reconnect;
socket.onmessage = main_loop;

document.addEventListener("SendToServer", function(event) {
    const messageType = event.detail.message_type;
    const content = event.detail.content;
    sendToServer(messageType, content);
});

function serverHandshake() {
    console.log("Connected to the server!");
    const savedToken = localStorage.getItem(roomCode);
    if (savedToken) {
        sendToServer("PlayerToken", `${savedToken}`);
    }
    else {
        sendToServer("NewPlayer", "");
    }
};
function reconnect() {
    console.warn('Websocket closed - reconnecting..');
    socket = new WebSocket(`${protocol}//${host}/ws/${roomCode}`);
    socket.onopen = serverHandshake;
    socket.onclose = reconnect;
    socket.onmessage = main_loop;
}
function sendToServer(messageType, content) {
    if (socket.readyState === WebSocket.OPEN) {
        const payload = {
            message_type: messageType,
            content: content
        };
        socket.send(JSON.stringify(payload));
        console.log(`Sent to server: ${JSON.stringify(payload)}`);
    } else {
        console.warn("Tried to send a message, but the socket isn't open yet!");
    }
};