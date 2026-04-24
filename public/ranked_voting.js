// @ts-check
import Sortable from 'https://cdn.jsdelivr.net/npm/sortablejs@1.15.2/+esm';
import { sendToServer } from './socket.js';

const votingList = document.getElementById("ranked_options_list");

if (votingList) {
    new Sortable(votingList, {
        animation: 150, // Adds a smooth sliding animation (in milliseconds)
        ghostClass: 'dragging-state', // The class applied to the space left behind
        delay: 100, // Important for mobile: wait 100ms before dragging so users can still scroll the page
        delayOnTouchOnly: true
    });
}