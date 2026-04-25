use crate::state::GameState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// gets the mean mid max and var of a vector of floats
fn get_stats(vector: Vec<f32>) -> (f32, f32, f32, f32) {
    let mut min = f32::MAX;
    let mut max = f32::MIN;
    let mut total = 0.0;
    let count = vector.len() as f32;

    for num in vector.clone() {
        if num > max {
            max = num;
        }
        if num < min {
            min = num;
        }
        total += num;
    }

    let mean = total / count;

    let mut var = 0.0;
    for num in vector {
        var += (num - mean).powf(2.0);
    }
    var = var / count;

    (mean, min, max, var)
}

pub fn results(state: &Arc<Mutex<HashMap<String, GameState>>>, room_code: &str) {
    let mut locked_rooms = state.lock().unwrap();
    let room = locked_rooms
        .get_mut(room_code)
        .expect("Room doesn't exist although we just checked in prev function?");

    let mut option_final_scores: HashMap<String, Vec<f32>> = HashMap::new();

    for player in &room.players {
        for (option, score) in &player.option_scores {
            option_final_scores
                .entry(option.clone())
                .or_insert(vec![])
                .push(score.clone());
        }
    }

    println!("{:?}", option_final_scores);
}
