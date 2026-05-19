// @ts-check
export function trySendToServer(messageType, content) {
    const newEvent = new CustomEvent("SendToServer", { 
        detail: { message_type: messageType, content: content }
    });
    document.dispatchEvent(newEvent);
}