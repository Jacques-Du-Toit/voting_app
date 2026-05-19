// @ts-check
import { checkMessageLobby } from "./lobby.js";
import { checkMessageResults } from "./results.js";

const pathnameParts = window.location.pathname.split('/');
export const roomCode = pathnameParts[pathnameParts.length - 1];

const roomTitle = document.getElementById("room_title");
roomTitle.textContent = roomCode;

const lobbyScreen = document.getElementById("lobby_screen");
const votingScreen = document.getElementById("voting_screen");
const rankedVotingScreen = document.getElementById("ranked_voting_screen");
const resultsScreen = document.getElementById("results_screen");
const screens = {
    "lobby": lobbyScreen,
    "voting": votingScreen,
    "ranked_voting": rankedVotingScreen,
    "results": resultsScreen,
};

let phase = "lobby"; // Need to ask server for the current phase if someone joins late or rejoins

export function main_loop(event) {
    console.log(`Received from server: ${event.data}`);
    
    if (event.data == "Room Not Found") {
        window.location.href = "/room_not_found";
        return;
    }
    if (event.data == "") {
        return;
    }
    const serverMessage = JSON.parse(event.data);

    if (serverMessage.message_type == "ChangePhase") {
        const newPhase = serverMessage.content;
        Object.values(screens).forEach(screenElement => {
            screenElement?.classList.add("hidden");
        });
        screens[newPhase].classList.remove("hidden");
        phase = newPhase;
    }

    if (phase == "lobby") {
        checkMessageLobby(serverMessage, roomCode);
    }
    else if (phase == "results") {
        checkMessageResults(serverMessage);
    }
    else {
        console.log(`Unknown phase: ${phase}`);
    }
};