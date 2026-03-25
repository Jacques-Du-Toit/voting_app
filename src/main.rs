use axum::extract::ws::{Message::Text, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::{Html, Redirect};
use axum::{Form, Router, routing::get, routing::post};
use rand::RngExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::broadcast::{Receiver, Sender};

struct GameState {
    tower: Sender<String>,
    options: Vec<String>,
}

fn build_gamestate(channel_num: usize) -> GameState {
    GameState {
        tower: Sender::new(channel_num),
        options: vec![],
    }
}

#[derive(Deserialize)]
struct JoinRequest {
    room_code: String,
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

    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
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
    rooms.insert(room_code.clone(), build_gamestate(10)); // somehow turn the room code into a usize (like some basic hashing function)
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

async fn room_not_found() -> Html<&'static str> {
    Html(include_str!("../templates/room_not_found.html"))
}

fn add_option_to_room_and_get_options(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    option: String,
    room_code: &str,
) -> Option<(Vec<String>, Sender<String>, Receiver<String>)> {
    let mut locked_rooms = state.lock().unwrap();
    let game_state = locked_rooms.get_mut(room_code)?;
    // probaably should move these out of this function as we dont need to create these every time an option is added
    let new_sender = game_state.tower.clone();
    let new_receiver = game_state.tower.subscribe();

    game_state.options.push(option);
    Some((game_state.options.clone(), new_sender, new_receiver))
}

async fn handle_socket(
    state: Arc<Mutex<HashMap<String, GameState>>>,
    mut socket: WebSocket,
    room_code: String,
) {
    println!("Someone connected to room {}!", room_code);

    while let Some(msg) = socket.recv().await {
        let msg = msg.unwrap();
        println!("Received a message: {:?}", msg);

        if let Text(text) = msg {
            let received_option = text.to_string();
            let (options, sender, receiver) =
                match add_option_to_room_and_get_options(&state, received_option, &room_code) {
                    Some((op, sen, rec)) => (op, sen, rec),
                    None => {
                        socket.send(Text("Room Not Found".into())).await.unwrap();
                        break;
                    }
                };

            for option in options {
                socket.send(Text(option.clone().into())).await.unwrap();
                sender.send(option);
            }

            //receiver.recv().await.unwrap() // when will it receive? do we need to keep checking all the messages?
        }
    }

    // If the loop breaks, it means they closed the browser tab!
    println!("User disconnected!");
}
