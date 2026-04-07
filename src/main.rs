mod state;
mod websocket;

use crate::state::{GameState, JoinRequest, build_gamestate};
use crate::websocket::ws_handler;

use axum::extract::{Path, State};
use axum::response::{Html, Redirect};
use axum::{Form, Router, routing::get, routing::post};
use rand::RngExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

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
        .fallback_service(ServeDir::new("public"))
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

async fn room_not_found() -> Html<&'static str> {
    Html(include_str!("../templates/room_not_found.html"))
}
