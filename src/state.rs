use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast::Sender;

#[derive(Deserialize, Serialize, PartialEq, Debug)]
pub enum MessageType {
    NewPlayer,
    PlayerToken,
    OptionsOrder,
    NewOption,
    DeleteOption,
    ToggleReady,
    ChangePhase,
    Debug,
}

#[derive(Deserialize, Debug)]
pub struct ClientMessage {
    pub message_type: MessageType,
    pub content: String,
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
    pub is_connected: bool,
    pub option_scores: HashMap<String, f32>,
}

pub fn build_player(name: String) -> Player {
    Player {
        name,
        ready: false,
        is_connected: true,
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

#[allow(dead_code)]
#[derive(Debug)]
pub enum GameError {
    UserDisconnected,
    NetworkFailure(axum::Error),
    ParseFailure(serde_json::Error),
    WrongFrameType(String),
    WrongMessageType(String),
}

impl From<axum::Error> for GameError {
    fn from(error: axum::Error) -> Self {
        GameError::NetworkFailure(error)
    }
}

impl From<serde_json::Error> for GameError {
    fn from(error: serde_json::Error) -> Self {
        GameError::ParseFailure(error)
    }
}
