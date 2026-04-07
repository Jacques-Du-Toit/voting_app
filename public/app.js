const pathParts = window.location.pathname.split('/');
const roomCode = pathParts[pathParts.length - 1];
const socket = new WebSocket("ws://" + window.location.host + "/ws/" + roomCode);

const sendMessageToServer = function(messageType, messageContents) {
    const payload = {
        message_type: messageType,
        contents: messageContents
    };
    socket.send(JSON.stringify(payload))
}

const savedToken = localStorage.getItem("player_token");
socket.onopen = function() {
    console.log("Connected to the server!");
    if (savedToken) {
        sendMessageToServer("PlayerToken", `${savedToken}`)
    }
    else {
        sendMessageToServer("NewPlayer", "")
    }
};

const playerCount = document.getElementById("player_count_display");
const readyBtn = document.getElementById("ready_btn");
const form = document.getElementById("option_form");
const inputBox = document.getElementById("add_option_box");
const optionList = document.getElementById("options_list");

readyBtn.onclick = function() {
    sendMessageToServer("ToggleReady", "")
}

form.addEventListener("submit", function(event) {
    event.preventDefault(); 
    const optionText = inputBox.value.trim();
    if (optionText == "") {
        return;
    }
    sendMessageToServer("NewOption", optionText);    
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
    deleteBtn.onclick = function() {sendMessageToServer("DeleteOption", optionText.textContent)};
    
    newOptionContainer.appendChild(optionText);
    newOptionContainer.appendChild(deleteBtn);
    
    optionList.appendChild(newOptionContainer);
}

const removeOption = function(option_text) {
    document.querySelector(`[data-option="${option_text}"]`).remove();
}

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
    if (serverMessage.message_type == "PlayerToken") {
        localStorage.setItem("player_token", serverMessage.content);
    }
    else if (serverMessage.message_type == "ToggleReady") {
        playerCount.textContent = `Ready: ${serverMessage.content}`;
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
};