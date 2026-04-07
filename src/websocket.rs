use crate::state::{
    ClientMessage, GameError, GameState, MessageType, Player, ServerMessage, build_player,
};
use axum::extract::ws::{
    Message::{self, Text},
    WebSocket, WebSocketUpgrade,
};
use axum::extract::{Path, State};
use futures::stream::{SplitSink, SplitStream, StreamExt};
use futures::{
    Stream,
    sink::{Sink, SinkExt},
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

async fn handle_socket(
    state: Arc<Mutex<HashMap<String, GameState>>>,
    mut socket: WebSocket,
    room_code: String,
) {
    println!("Someone connected to room {room_code}!");

    let (sender, mut receiver) = match get_sender_and_receiver(&state, &room_code) {
        Some((s, r)) => (s, r),
        None => {
            send_to_socket(&mut socket, "Room Not Found").await;
            return;
        }
    };
    if let Ok(msg) = receive_from_socket(&mut socket).await {
        println!("{}", msg.contents);
    }

    let player_id = add_new_player_and_send_from_tower(&state, &room_code, &sender);

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

async fn send_to_socket<S, E>(socket: &mut S, text: &str)
where
    S: Sink<Message, Error = E> + Unpin,
    E: Debug,
{
    if let Err(e) = socket.send(Text(text.into())).await {
        println!("Error sending {text} to WebSocket due to {:?}", e);
    }
}

async fn receive_from_socket<S>(socket: &mut S) -> Result<ClientMessage, GameError>
where
    S: Stream<Item = Result<Message, axum::Error>> + Unpin,
{
    let msg = socket.next().await.ok_or(GameError::UserDisconnected)??;
    if let Text(text) = msg {
        let parsed_msg = serde_json::from_str::<ClientMessage>(&text.to_string())?;
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
    if let Some(msg) = socket.next().await {
        let msg = match msg {
            Ok(some_msg) => some_msg,
            Err(e) => {
                println!("Error reading message due to {:?}", e);
                return false;
            }
        };
        println!("Received a message: {:?}", msg);

        if let Text(text) = msg {
            let msg_str = text.to_string();

            if let Ok(parsed_msg) = serde_json::from_str::<ClientMessage>(&msg_str) {
                evaluate_parsed_msg(parsed_msg, state, room_code, sender, player_id);
            } else {
                println!("Failed to parse JSON: {}", msg_str);
            }
        }
        true
    } else {
        println!("User {player_id} disconnected");
        false
    }
}

fn evaluate_parsed_msg(
    parsed_msg: ClientMessage,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    sender: &Sender<String>,
    player_id: &str,
) {
    match parsed_msg.message_type {
        MessageType::NewPlayer => {}   // TODO
        MessageType::PlayerToken => {} // TODO
        MessageType::NewOption => {
            add_option_to_room(state, parsed_msg.contents, room_code, sender);
        }
        MessageType::DeleteOption => {
            remove_option_from_room(state, parsed_msg.contents, room_code, sender);
        }
        MessageType::ToggleReady => switch_player_ready(player_id, state, room_code, sender),
        MessageType::Debug => println!("{}", parsed_msg.contents),
    }
}

async fn check_receiver(
    receiver: &mut Receiver<String>,
    socket: &mut SplitSink<WebSocket, Message>,
) {
    match receiver.recv().await {
        Ok(option) => send_to_socket(socket, &option).await,
        Err(e) => println!("Error receiving option {:?}", e),
    };
}

fn switch_player_ready(
    player_id: &str,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    sender: &Sender<String>,
) {
    let mut locked_rooms = state.lock().unwrap();
    let players = &mut locked_rooms
        .get_mut(room_code)
        .expect("Room doesn't exist although we just checked in prev function?")
        .players;

    if let Some(player) = players.iter_mut().find(|p| p.name == player_id) {
        player.ready = !player.ready;
    }
    send_ready_player_count(players, sender)
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
        let json_string = to_server_message_json(MessageType::NewOption, option.clone());
        send_to_socket(socket, &json_string).await
    }
}

fn disconnect_player_and_send_from_tower(
    player_id: String,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<String>,
) {
    let mut locked_rooms = state.lock().unwrap();
    let players = &mut locked_rooms
        .get_mut(room_code)
        .expect("Room doesn't exist although we just checked in prev function?")
        .players;
    if let Some(player) = players.iter_mut().find(|p| p.name == player_id) {
        player.is_connected = false;
        player.ready = false;
    }
    send_ready_player_count(players, room_tower);
}

fn add_new_player_and_send_from_tower(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<String>,
) -> String {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms
        .get_mut(room_code)
        .expect("Room doesn't exist although we just checked in prev function?");
    let players = &mut game_state.players;
    game_state.latest_id += 1;
    let player_id = game_state.latest_id.to_string();
    players.push(build_player(player_id.clone()));
    send_ready_player_count(players, room_tower);
    player_id
}

fn send_ready_player_count(players: &mut Vec<Player>, room_tower: &Sender<String>) {
    let ready_players = players.iter().filter(|player| player.ready).count();
    let num_players = players.iter().filter(|player| player.is_connected).count();
    send_from_tower(
        MessageType::ToggleReady,
        format!("{ready_players}/{num_players}"),
        room_tower,
    );
}

fn remove_option_from_room(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    option: String,
    room_code: &str,
    room_tower: &Sender<String>,
) -> Option<()> {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms.get_mut(room_code)?;

    game_state
        .options
        .retain(|existing_option| existing_option != &option);
    send_from_tower(MessageType::DeleteOption, option, room_tower);
    Some(())
}

fn add_option_to_room(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    option: String,
    room_code: &str,
    room_tower: &Sender<String>,
) -> Option<()> {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms.get_mut(room_code)?;
    if !game_state.options.contains(&option) && (option != "") {
        game_state.options.push(option.clone());
        send_from_tower(MessageType::NewOption, option, room_tower);
    }
    Some(())
}

fn send_from_tower(message_type: MessageType, content: String, room_tower: &Sender<String>) {
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
