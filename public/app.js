// @ts-check
import { sendToServer, roomCode, socket } from "./socket.js";
import { checkMessageLobby } from "./lobby.js";

const savedToken = localStorage.getItem(roomCode);
console.log(savedToken)
socket.onopen = function() {
    console.log("Connected to the server!");
    if (savedToken) {
        sendToServer("PlayerToken", `${savedToken}`)
    }
    else {
        sendToServer("NewPlayer", "")
    }
};

socket.onmessage = function(event) {
    console.log("The server says: ", event.data);
    
    if (event.data == "Room Not Found") {
        window.location.href = "/room_not_found";
        return;
    }
    if (event.data == "") {
        return;
    }
    const serverMessage = JSON.parse(event.data);
    checkMessageLobby(serverMessage);
};