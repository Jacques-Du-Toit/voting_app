use axum::extract::ws::{
    Message::{self, Text},
    WebSocket, WebSocketUpgrade,
};
use axum::extract::{Path, State};
use axum::response::{Html, Redirect};
use axum::{Form, Router, routing::get, routing::post};
use futures::sink::{Sink, SinkExt};
use futures::stream::{SplitSink, SplitStream, StreamExt};
use rand::RngExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::broadcast::{Receiver, Sender};

#[derive(Deserialize, PartialEq, Debug)]
enum MessageType {
    NewOption,
    DeleteOption,
    Debug,
}

#[derive(Deserialize)]
struct ClientMessage {
    message_type: MessageType,
    contents: String,
}

#[derive(Deserialize)]
struct JoinRequest {
    room_code: String,
}

struct GameState {
    tower: Sender<String>,
    options: Vec<String>, // should store as hashset if no duplicates allowed? but maybe order matters
}

fn build_gamestate() -> GameState {
    GameState {
        tower: Sender::new(20),
        options: vec![],
    }
}

#[tokio::main]
async fn main() {
    let rooms: HashMap<String, GameState> = HashMap::new();
    let shared_state = Arc::new(Mutex::new(rooms));

    let app = Router::new()
        .route("/", get(home_screen))
        .route("/create_room", post(create_room))
        .route("/join_room", post(join_room))
        .route("/room/{room_code}", get(show_room))
        .route("/room_not_found", get(room_not_found))
        .route("/ws/{room_code}", get(ws_handler))
        .with_state(shared_state);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server is running! Go to http://127.0.0.1:3000 in your browser.");

    axum::serve(listener, app).await.unwrap();
}

async fn home_screen() -> Html<&'static str> {
    Html(include_str!("../templates/index.html"))
}

fn generate_room(rooms: &mut HashMap<String, GameState>) -> String {
    let mut rng = rand::rng();
    let alphabet = [
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];
    let mut room_code = "".to_string();
    loop {
        room_code.clear();
        for _ in 0..4 {
            let random_index = rng.random_range(0..26);
            room_code.push(alphabet[random_index]);
        }
        if !rooms.contains_key(&room_code) {
            break;
        }
    }
    rooms.insert(room_code.clone(), build_gamestate());
    room_code
}

async fn create_room(State(state): State<Arc<Mutex<HashMap<String, GameState>>>>) -> Redirect {
    let mut locked_rooms = state.lock().unwrap();
    let room_code = generate_room(&mut locked_rooms);
    Redirect::to(&format!("/room/{room_code}"))
}

async fn join_room(
    State(state): State<Arc<Mutex<HashMap<String, GameState>>>>,
    Form(request): Form<JoinRequest>,
) -> Redirect {
    let locked_rooms = state.lock().unwrap();
    let code_they_entered = request.room_code;
    if locked_rooms.contains_key(&code_they_entered) {
        Redirect::to(&format!("/room/{code_they_entered}"))
    } else {
        Redirect::to(&format!("/"))
    }
}

async fn show_room(Path(room_code): Path<String>) -> Html<String> {
    Html(include_str!("../templates/show_room.html").replace("[ROOM_CODE]", &room_code))
}

async fn ws_handler(
    State(state): State<Arc<Mutex<HashMap<String, GameState>>>>,
    ws: WebSocketUpgrade,
    Path(room_code): Path<String>,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_socket(state, socket, room_code))
}

async fn send_to_socket<S, E>(socket: &mut S, text: &str)
where
    // S must be something that can "Sink" (send) WebSockets Messages
    // Unpin is a Tokio requirement to safely pass the socket by mutable reference
    S: Sink<Message, Error = E> + Unpin,
    E: Debug,
{
    if let Err(e) = socket.send(Text(text.into())).await {
        println!("Error sending {} to WebSocket due to {:?}", text, e);
    }
}

async fn room_not_found() -> Html<&'static str> {
    Html(include_str!("../templates/room_not_found.html"))
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

fn add_option_to_room(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    option: String,
    room_code: &str,
    room_tower: &Sender<String>,
) -> Option<()> {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms.get_mut(room_code)?;

    game_state.options.push(option.clone());
    let _ = room_tower.send(option); // should handle this case eventually (where it errors)
    Some(())
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
    // need to tell all websockets to remove this option from the front end
    //let _ = room_tower.send(option); // should handle this case eventually (where it errors)
    Some(())
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
        send_to_socket(socket, &option).await;
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

fn evaluate_parsed_msg(
    parsed_msg: ClientMessage,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    sender: &Sender<String>,
) {
    // may need to make function async at some point
    match parsed_msg.message_type {
        MessageType::NewOption => {
            add_option_to_room(&state, parsed_msg.contents, room_code, sender);
        }
        MessageType::DeleteOption => {
            remove_option_from_room(&state, parsed_msg.contents, room_code, sender);
        }
        MessageType::Debug => println!("{}", parsed_msg.contents),
    }
}

async fn check_message(
    socket: &mut SplitStream<WebSocket>,
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    sender: &Sender<String>,
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
                evaluate_parsed_msg(parsed_msg, &state, room_code, sender);
            } else {
                println!("Failed to parse JSON: {}", msg_str);
            }
        }
        true
    } else {
        println!("User disconnected");
        false
    }
}

async fn handle_socket(
    state: Arc<Mutex<HashMap<String, GameState>>>,
    mut socket: WebSocket,
    room_code: String,
) {
    println!("Someone connected to room {room_code}!");

    // Get the sender and receiver of the current room, if it doesn't exist send them to a page that shows this
    let (sender, mut receiver) = match get_sender_and_receiver(&state, &room_code) {
        Some((s, r)) => (s, r),
        None => {
            send_to_socket(&mut socket, "Room Not Found").await;
            return;
        }
    };
    // If someone joins late want to send all the current options to their screen, break out if room not found
    send_all_current_options_to_websocket(&state, &mut socket, &room_code).await;

    // Want to be able to read and write from socket at the same time without multiple references error
    let (mut socket_write, mut socket_read) = socket.split();

    // Check whether a new option has been added or whether they have sent a new option to the backend simultaneously
    loop {
        tokio::select! {
            _ = check_receiver(&mut receiver, &mut socket_write) => {}
            connected = check_message(&mut socket_read, &state, &room_code, &sender) => {
                if !connected {break};
            }
        }
    }
}
