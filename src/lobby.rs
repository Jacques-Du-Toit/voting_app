use crate::state::{GameError, GameState, MessageType, Player, build_player};
use crate::websocket::{receive_from_socket, send_from_tower, send_message_to_socket};
use axum::extract::ws::WebSocket;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::Sender;

/// Adds a new player to the GameState of the room,
/// sends the id to the socket so it knows what the player is in future,
/// lets all websockets know the new ready/player count
pub async fn add_new_player_and_send_to_socket_and_tower(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    socket: &mut WebSocket,
    room_tower: &Sender<String>,
) -> String {
    let player_id = {
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
    };
    send_message_to_socket(MessageType::PlayerToken, player_id.clone(), socket).await;
    player_id
}

/// If a player_id already exists in the GameState that just joined, sets their is_connected to True
/// and lets all other websockets know the new ready/player count
pub fn active_old_player_and_send_from_tower(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<String>,
    player_id: &str,
) {
    let mut locked_rooms = state.lock().unwrap();
    let players = &mut locked_rooms
        .get_mut(room_code)
        .expect("Room doesn't exist although we just checked in prev function?")
        .players;
    if let Some(old_player) = players.iter_mut().find(|p| p.name == player_id) {
        old_player.is_connected = true;
    }
    send_ready_player_count(players, room_tower);
}

pub fn disconnect_player_and_send_from_tower(
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

pub fn switch_player_ready(
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

pub fn add_option_to_room(
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

pub fn remove_option_from_room(
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

fn send_ready_player_count(players: &mut Vec<Player>, room_tower: &Sender<String>) {
    let ready_players = players.iter().filter(|player| player.ready).count();
    let num_players = players.iter().filter(|player| player.is_connected).count();
    let all_ready = ready_players == num_players;
    send_from_tower(
        MessageType::ToggleReady,
        format!("{ready_players}/{num_players} {all_ready}"),
        room_tower,
    );
}

pub async fn get_player_id(
    socket: &mut WebSocket,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<String>,
) -> Result<String, GameError> {
    let client_msg = receive_from_socket(socket).await?;
    match client_msg.message_type {
        MessageType::NewPlayer => Ok(add_new_player_and_send_to_socket_and_tower(
            state, room_code, socket, room_tower,
        )
        .await),
        MessageType::PlayerToken => {
            active_old_player_and_send_from_tower(
                state,
                room_code,
                room_tower,
                &client_msg.content,
            );
            Ok(client_msg.content)
        }
        _ => {
            println!("A different MessageType was sent before player Id was established.");
            Err(GameError::WrongFrameType(format!(
                "Received {:?}",
                client_msg
            )))
        }
    }
}

pub fn update_player_option_scores(
    option_order: String,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    player_id: &str,
) {
    let option_order: HashMap<String, f32> = option_order
        .split(',')
        .enumerate()
        .map(|(index, key)| (key.to_string(), index as f32))
        .collect();
    // should generalise getting all the players from the room but requires lifetimes etc.
    let mut locked_rooms = state.lock().unwrap();
    let players = &mut locked_rooms
        .get_mut(room_code)
        .expect("Room doesn't exist although we just checked in prev function?")
        .players;
    // also generalise changing a variable of a specific players
    if let Some(player) = players.iter_mut().find(|p| p.name == player_id) {
        player.option_scores = option_order;
    }
}
