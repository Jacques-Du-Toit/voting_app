// @ts-check
import { sendToServer, roomCode, socket } from "./socket.js";
import { checkMessageLobby } from "./lobby.js";
import "./ranked_voting.js";

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

const roomTitle = document.getElementById("room_title");
roomTitle.textContent = roomCode;

const lobbyScreen = document.getElementById("lobby_screen");
const votingScreen = document.getElementById("voting_screen");
const rankedVotingScreen = document.getElementById("ranked_voting_screen");
const screens = {
    "lobby": lobbyScreen,
    "voting": votingScreen,
    "ranked_voting": rankedVotingScreen
};

let state = "lobby"; // Need to ask server for the current state if someone joins late or rejoins
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

    if (serverMessage.message_type == "ChangeState") {
        const newState = serverMessage.content;
        Object.values(screens).forEach(screenElement => {
            screenElement?.classList.add("hidden");
        });
        screens[newState].classList.remove("hidden");
        state = newState;
    }

    if (state == "lobby"){
        checkMessageLobby(serverMessage);
    }
    else {
        console.log(`Unknown state: ${state}`);
    }
};