// @ts-check
import Sortable from 'https://cdn.jsdelivr.net/npm/sortablejs@1.15.2/+esm';
import { sendToServer, roomCode } from "./socket.js";

const playerCount = document.getElementById("player_count_display");
const readyBtn = document.getElementById("ready_btn");
const nextBtn = document.getElementById("next_btn");
const form = document.getElementById("option_form");
const inputBox = document.getElementById("add_option_box");
const optionList = document.getElementById("options_list");

const sendOptionOrdering = function() {
    const items = document.querySelectorAll("#options_list .sortable-item");
    const optionsOrder = Array.from(items).map(item => item.getAttribute("data-option"));
    sendToServer("OptionsOrder", optionsOrder.join(","));
}

if (optionList) {
    new Sortable(optionList, {
        animation: 150, // Adds a smooth sliding animation (in milliseconds)
        ghostClass: 'dragging-state', // The class applied to the space left behind
        delay: 10, // Important for mobile: wait 10ms before dragging so users can still scroll the page
        delayOnTouchOnly: true,
        onEnd: sendOptionOrdering
    });
}

readyBtn.onclick = function() {
    sendToServer("ToggleReady", "");
}

nextBtn.onclick = function() {
    sendOptionOrdering();
    sendToServer("ChangePhase", "results"); // eventually change to select_voting
}

form.addEventListener("submit", function(event) {
    event.preventDefault(); 
    const optionText = inputBox.value.trim();
    if (optionText == "") {
        return;
    }
    sendToServer("NewOption", optionText);    
    inputBox.value = ""; 
});

const addNewOption = function(option_text, optionList) {
    const newOptionContainer = document.createElement("li");
    newOptionContainer.setAttribute("data-option", option_text);
    newOptionContainer.setAttribute("class", "sortable-item");

    const optionText = document.createElement("span");
    optionText.textContent = option_text;
    
    const deleteBtn = document.createElement("button");
    deleteBtn.textContent = "X";
    deleteBtn.className = "delete-btn"
    deleteBtn.onclick = function() {sendToServer("DeleteOption", optionText.textContent)};
    
    newOptionContainer.appendChild(optionText);
    newOptionContainer.appendChild(deleteBtn);
    
    optionList.appendChild(newOptionContainer);
}

const removeOption = function(option_text) {
    const item = document.querySelector(`[data-option="${option_text}"]`);
    if (!item) return; 

    const animation = item.animate([
        { opacity: 1, transform: 'scale(1)' },     // Start state
        { opacity: 0, transform: 'scale(0.9)' }    // End state (faded and slightly shrunk)
    ], {
        duration: 300,        // How long it takes in milliseconds
        easing: 'ease-out'    // Makes the animation start fast and slow down at the end
    });
    animation.onfinish = () => {
        item.remove();
    };
}

export const checkMessageLobby = function(serverMessage) {
    if (serverMessage.message_type == "PlayerToken") {
        localStorage.setItem(roomCode, serverMessage.content);
    }
    else if (serverMessage.message_type == "ToggleReady") {
        const [readyTxt, allReady] = serverMessage.content.split(' ');
        playerCount.textContent = readyTxt;
        if (allReady == "true") {
            nextBtn.disabled = false;
        }
        else {
            nextBtn.disabled = true;
        }
    }
    else if (serverMessage.message_type == "NewOption") {
        addNewOption(serverMessage.content, optionList);
        sendOptionOrdering();
    }
    else if (serverMessage.message_type == "DeleteOption") {
        removeOption(serverMessage.content);
        sendOptionOrdering();
    }
    else {
        console.log("Unknown message_type:", serverMessage.message_type);
    }
}