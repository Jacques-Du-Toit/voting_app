use crate::lobby::{
    add_option_to_room, disconnect_player_and_send_from_tower, get_player_id,
    remove_option_from_room, switch_player_ready,
};
use crate::state::{ClientMessage, GameError, GameState, MessageType, ServerMessage};

use axum::extract::ws::{
    Message::{self, Text},
    WebSocket, WebSocketUpgrade,
};
use axum::extract::{Path, State};
use futures::{
    Stream,
    sink::{Sink, SinkExt},
    stream::{SplitSink, SplitStream, StreamExt},
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::{Receiver, Sender};

pub async fn ws_handler(
    State(state): State<Arc<Mutex<HashMap<String, GameState>>>>,
    ws: WebSocketUpgrade,
    Path(room_code): Path<String>,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_socket(state, socket, room_code))
}

/// Main loop that is ran per player,
/// so each player will have a separate instance of this loop running for their websocket.
async fn handle_socket(
    state: Arc<Mutex<HashMap<String, GameState>>>,
    mut socket: WebSocket,
    room_code: String,
) {
    println!("Someone connected to room {room_code}");

    let (sender, mut receiver) = match get_sender_and_receiver(&state, &room_code) {
        Some((s, r)) => (s, r),
        None => {
            send_to_socket(&mut socket, "Room Not Found").await;
            return;
        }
    };
    let player_id = match get_player_id(&mut socket, &state, &room_code, &sender).await {
        Ok(id) => id,
        Err(e) => {
            println!("Couldn't read player id {:?}", e);
            return;
        }
    };
    println!("{player_id} has connected");

    send_all_current_options_to_websocket(&state, &mut socket, &room_code).await;

    let (mut socket_write, mut socket_read) = socket.split();

    loop {
        tokio::select! {
            _ = check_receiver(&mut receiver, &mut socket_write) => {}
            connected = check_message(&mut socket_read, &state, &room_code, &sender, &player_id) => {
                if !connected { break };
            }
        }
    }
    disconnect_player_and_send_from_tower(player_id, &state, &room_code, &sender);
}

/// Creates a copy of a room's sender and receiver objects,
/// which are used to broadcast to all other websockets of other players
fn get_sender_and_receiver(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
) -> Option<(Sender<String>, Receiver<String>)> {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms.get_mut(room_code)?;

    let new_sender = game_state.tower.clone();
    let new_receiver = game_state.tower.subscribe();

    Some((new_sender, new_receiver))
}

/// Used to check if there are any new messages from the receiver,
/// messages from here will have been sent to all websockets.
async fn check_receiver(
    receiver: &mut Receiver<String>,
    socket: &mut SplitSink<WebSocket, Message>,
) {
    match receiver.recv().await {
        Ok(json_str) => send_to_socket(socket, &json_str).await,
        Err(e) => println!("Error receiving json_str {:?}", e),
    };
}

/// Used to send a json string formatted with the message type and content to the current players websocket
async fn send_to_socket<S, E>(socket: &mut S, text: &str)
where
    S: Sink<Message, Error = E> + Unpin,
    E: Debug,
{
    if let Err(e) = socket.send(Text(text.into())).await {
        println!("Error sending {text} to WebSocket due to {:?}", e);
    }
}

pub async fn send_message_to_socket<S, E>(
    message_type: MessageType,
    content: String,
    socket: &mut S,
) where
    S: Sink<Message, Error = E> + Unpin,
    E: Debug,
{
    let json_string = to_server_message_json(message_type, content);
    send_to_socket(socket, &json_string).await
}

/// Used to check if there is a message from the current players websocket,
/// will hang until a message is receiver on the .await line
pub async fn receive_from_socket<S>(socket: &mut S) -> Result<ClientMessage, GameError>
where
    S: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let msg = socket.next().await.ok_or(GameError::UserDisconnected)??;
    if let Text(text) = msg {
        let parsed_msg = serde_json::from_str::<ClientMessage>(&text.to_string())?;
        println!("Received from client: {:?}", parsed_msg);
        Ok(parsed_msg)
    } else {
        Err(GameError::WrongFrameType(format!(
            "Expected Text enum got {:?}",
            msg
        )))
    }
}

async fn check_message(
    socket: &mut SplitStream<WebSocket>,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    sender: &Sender<String>,
    player_id: &str,
) -> bool {
    match receive_from_socket(socket).await {
        Ok(msg) => {
            evaluate_parsed_msg(msg, state, room_code, sender, player_id);
            true
        }
        Err(e) => match e {
            GameError::WrongFrameType(fram_err) => {
                println!("{:?}", fram_err);
                true // Keep connection alive for bad frames
            }
            _ => {
                println!("User {player_id} disconnected");
                false // Break loop on disconnect or network error
            }
        },
    }
}

pub fn send_from_tower(message_type: MessageType, content: String, room_tower: &Sender<String>) {
    let json_string = to_server_message_json(message_type, content);
    let _ = room_tower.send(json_string);
}

fn to_server_message_json(message_type: MessageType, content: String) -> String {
    let outgoing_msg = ServerMessage {
        message_type,
        content,
    };
    serde_json::to_string(&outgoing_msg).unwrap()
}

async fn send_all_current_options_to_websocket(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    socket: &mut WebSocket,
    room_code: &str,
) {
    let game_state_options = {
        let locked_rooms = state.lock().unwrap();
        locked_rooms
            .get(room_code)
            .expect("Room doesn't exist although we just checked in prev function?")
            .options
            .clone()
    };
    for option in game_state_options {
        // we dont want to broadcast so we don't use the sender here
        // as they may have joined at a different time
        send_message_to_socket(MessageType::NewOption, option.clone(), socket).await;
    }
}

/// Calls certain code when a message is received from the websocket
fn evaluate_parsed_msg(
    parsed_msg: ClientMessage,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    sender: &Sender<String>,
    player_id: &str,
) {
    match parsed_msg.message_type {
        MessageType::NewPlayer => println!(
            "Got NewPlayer Client Message but should have already been handled in handshake"
        ),
        MessageType::PlayerToken => println!(
            "Got PlayerToken Client Message but should have already been handled in handshake"
        ),
        MessageType::OptionsOrder => println!("Doing nothing for now"),
        MessageType::NewOption => {
            add_option_to_room(state, parsed_msg.contents, room_code, sender);
        }
        MessageType::DeleteOption => {
            remove_option_from_room(state, parsed_msg.contents, room_code, sender);
        }
        MessageType::ToggleReady => switch_player_ready(player_id, state, room_code, sender),
        MessageType::ChangeState => change_state(parsed_msg.contents, sender),
        MessageType::Debug => println!("{}", parsed_msg.contents),
    }
}

fn change_state(state: String, sender: &Sender<String>) {
    send_from_tower(MessageType::ChangeState, state, sender);
}
