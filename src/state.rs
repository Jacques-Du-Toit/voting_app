use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast::Sender;

#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub enum MessageType {
    NewOption,
    DeleteOption,
    ToggleReady,
    Debug,
}

#[derive(Deserialize)]
pub struct ClientMessage {
    pub message_type: MessageType,
    pub contents: String,
}

#[derive(Serialize)]
pub struct ServerMessage {
    pub message_type: MessageType,
    pub content: String,
}

#[derive(Deserialize)]
pub struct JoinRequest {
    pub room_code: String,
}

pub struct Player {
    pub name: String,
    pub ready: bool,
    pub option_scores: HashMap<String, f32>,
}

pub fn build_player(name: String) -> Player {
    Player {
        name,
        ready: false,
        option_scores: HashMap::new(),
    }
}

pub struct GameState {
    pub tower: Sender<String>,
    pub players: Vec<Player>,
    pub options: Vec<String>,
    pub latest_id: u32,
}

pub fn build_gamestate() -> GameState {
    GameState {
        tower: Sender::new(20),
        players: vec![],
        options: vec![],
        latest_id: 0,
    }
}
