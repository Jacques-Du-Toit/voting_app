use crate::lobby::{
    add_option_to_room, disconnect_player_and_send_from_tower, get_player_id, rank_system,
    remove_option_from_room, score_system, switch_player_ready, update_player_option_scores,
};
use crate::results::results;
use crate::state::{
    BroadcastIntent, BroadcastMessage, ClientMessage, GameError, GameState, MessageType,
    ServerMessage,
};

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

    send_all_current_options_to_websocket(&state, &mut socket, &room_code, &player_id).await;

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
) -> Option<(Sender<BroadcastMessage>, Receiver<BroadcastMessage>)> {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms.get_mut(room_code)?;

    let new_sender = game_state.tower.clone();
    let new_receiver = game_state.tower.subscribe();

    Some((new_sender, new_receiver))
}

// Sends a Message to all websockets by broadcasting it from the room tower (one-to-many)
pub fn send_to_all_websockets(
    message_type: MessageType,
    content: String,
    room_tower: &Sender<BroadcastMessage>,
) {
    let json_string = to_server_message_json(message_type, content);
    let broadcast_message = BroadcastMessage {
        broadcast_intent: BroadcastIntent::WebSockets,
        broadcast_content: json_string,
    };
    let _ = room_tower.send(broadcast_message);
}

fn to_server_message_json(message_type: MessageType, content: String) -> String {
    let outgoing_msg = ServerMessage {
        message_type,
        content,
    };
    serde_json::to_string(&outgoing_msg).unwrap()
}

// Sends a String to all the async clients by boradcasting it from the tower (one-to-many)
fn send_to_all_clients(
    broadcast_intent: BroadcastIntent,
    broadcast_content: String,
    room_tower: &Sender<BroadcastMessage>,
) {
    let broadcast_message = BroadcastMessage {
        broadcast_intent,
        broadcast_content,
    };
    let _ = room_tower.send(broadcast_message);
}

/// Used to check if there are any new messages from the receiver,
/// will either send it to its websocket or run other code depending on destination
async fn check_receiver(
    receiver: &mut Receiver<BroadcastMessage>,
    socket: &mut SplitSink<WebSocket, Message>,
) {
    match receiver.recv().await {
        Ok(BroadcastMessage {
            broadcast_intent,
            broadcast_content,
        }) => match broadcast_intent {
            BroadcastIntent::WebSockets => send_to_socket(socket, &broadcast_content).await,
            BroadcastIntent::ChangeSystem => {}
        },
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
    } else {
        println!("Sent to client: {text}");
    }
}

// this sends a Message to the websocket connected to its host (one-to-one)
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
    sender: &Sender<BroadcastMessage>,
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

/// Calls certain code when a message is received from the websocket
fn evaluate_parsed_msg(
    parsed_msg: ClientMessage,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<BroadcastMessage>,
    player_id: &str,
) {
    match parsed_msg.message_type {
        MessageType::NewPlayer => println!(
            "Got NewPlayer Client Message but should have already been handled in handshake"
        ),
        MessageType::PlayerToken => println!(
            "Got PlayerToken Client Message but should have already been handled in handshake"
        ),
        MessageType::OptionsOrder => {
            update_player_option_scores(parsed_msg.content, state, room_code, player_id)
        }
        MessageType::NewOption => {
            add_option_to_room(state, parsed_msg.content, room_code, room_tower);
        }
        MessageType::DeleteOption => {
            remove_option_from_room(state, parsed_msg.content, room_code, room_tower);
        }
        MessageType::ToggleReady => switch_player_ready(player_id, state, room_code, room_tower),
        MessageType::ChangeSystem => {
            send_to_all_clients(BroadcastIntent::ChangeSystem, "".to_string(), room_tower);
        }
        MessageType::ChangePhase => change_phase(parsed_msg.content, state, room_code, room_tower),
        MessageType::Debug => println!("{}", parsed_msg.content),
    }
}

fn hashmap_to_vector(map: HashMap<String, f32>) -> Vec<String> {
    let mut sorted_pairs: Vec<(String, f32)> = map.into_iter().collect();
    sorted_pairs.sort_by(|(_, val_a), (_, val_b)| val_a.partial_cmp(val_b).unwrap());
    sorted_pairs.into_iter().map(|(key, _)| key).collect()
}

async fn send_all_current_options_to_websocket(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    socket: &mut WebSocket,
    room_code: &str,
    player_id: &str,
) {
    let (game_state_options, player_options) = {
        let locked_rooms = state.lock().unwrap();
        let room = locked_rooms
            .get(room_code)
            .expect("Room doesn't exist although we just checked in prev function?");
        if let Some(player) = room.players.iter().find(|p| p.name == player_id) {
            (
                room.options.clone(),
                hashmap_to_vector(player.option_scores.clone()),
            )
        } else {
            (room.options.clone(), vec![])
        }
    };
    // first send any new options they haven't seen, then ones they've already ranked
    // TODO: Swap this when we have new options go to the top of the page instead of bottom
    let missing_options: Vec<String> = game_state_options
        .into_iter()
        .filter(|option| !player_options.contains(option))
        .collect();
    for option in player_options.into_iter().chain(missing_options) {
        send_message_to_socket(MessageType::NewOption, option, socket).await;
    }
}

fn change_system(
    system: String,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<BroadcastMessage>,
) {
    // let all websockets know to change to this new system on front end
    send_to_all_websockets(MessageType::ChangeSystem, system.clone(), room_tower);

    // do the other backend work on this client
    match system.to_ascii_lowercase() {
        x if x == "rank".to_string() => rank_system(state, room_code, room_tower),
        x if x == "score".to_string() => score_system(state, room_code, room_tower),
        _ => println!("No code for system {system}"),
    }
}

fn change_phase(
    phase: String,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<BroadcastMessage>,
) {
    send_to_all_websockets(MessageType::ChangePhase, phase.clone(), room_tower);
    // maybe something to change the state of the GameState on the backend

    match phase.to_ascii_lowercase() {
        x if x == "results".to_string() => results(state, room_code, room_tower),
        _ => println!("No code for phase {phase}"),
    }
}
