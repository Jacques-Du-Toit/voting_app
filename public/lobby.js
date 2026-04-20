// @ts-check
import { sendToServer, roomCode } from "./socket.js";

const playerCount = document.getElementById("player_count_display");
const readyBtn = document.getElementById("ready_btn");
const startBtn = document.getElementById("start_btn");
const form = document.getElementById("option_form");
const inputBox = document.getElementById("add_option_box");
const optionList = document.getElementById("options_list");

readyBtn.onclick = function() {
    sendToServer("ToggleReady", "")
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
    newOptionContainer.style.marginLeft = "35px";

    const optionText = document.createElement("span");
    optionText.textContent = option_text;
    
    const deleteBtn = document.createElement("button");
    deleteBtn.textContent = "X";
    deleteBtn.className = "outline"; 
    deleteBtn.style.marginLeft = "20px";
    deleteBtn.onclick = function() {sendToServer("DeleteOption", optionText.textContent)};
    
    newOptionContainer.appendChild(optionText);
    newOptionContainer.appendChild(deleteBtn);
    
    optionList.appendChild(newOptionContainer);
}

const removeOption = function(option_text) {
    document.querySelector(`[data-option="${option_text}"]`).remove();
}

export const checkMessageLobby = function(serverMessage) {
    if (serverMessage.message_type == "PlayerToken") {
        localStorage.setItem(roomCode, serverMessage.content);
    }
    else if (serverMessage.message_type == "ToggleReady") {
        const [readyTxt, allReady] = serverMessage.content.split(' ');
        playerCount.textContent = `Ready: ${readyTxt}`;
        if (allReady == "true") {
            startBtn.disabled = false;
        }
        else {
            startBtn.disabled = true;
        }

    }
    else if (serverMessage.message_type == "NewOption") {
        addNewOption(serverMessage.content, optionList);
    }
    else if (serverMessage.message_type == "DeleteOption") {
        removeOption(serverMessage.content);
    }
    else {
        console.log("Unknown message_type:", serverMessage.message_type);
    }
}