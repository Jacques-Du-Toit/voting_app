use crate::state::{GameState, MessageType};
use crate::websocket::send_from_tower;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast::Sender;

fn get_stats(vector: Vec<f32>) -> [f32; 4] {
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

    [max + 1.0, mean + 1.0, var, min + 1.0]
}

pub fn results(
    state: &Arc<Mutex<HashMap<String, GameState>>>,
    room_code: &str,
    room_tower: &Sender<String>,
) {
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
    println!("Option Scores: {:?}", option_final_scores);

    let mut option_stats: HashMap<String, [f32; 4]> = HashMap::new();
    for (option, scores) in option_final_scores {
        option_stats.insert(option, get_stats(scores));
    }
    println!("Option Stats: {:?}", option_stats);

    let mut ordered_options: Vec<String> = vec![];
    for (new_option, new_stats) in &option_stats {
        let mut placed = false;
        for (i, ordered_option) in ordered_options.clone().iter().enumerate() {
            let ordered_stats = option_stats
                .get(ordered_option)
                .expect("How can we not have the option?");
            for (new_stat, ordered_stat) in new_stats.iter().zip(ordered_stats.iter()) {
                if new_stat > ordered_stat {
                    break; // already worse
                } else if new_stat < ordered_stat {
                    // move ordered option and all the options after along one, place this one here
                    ordered_options.insert(i, new_option.to_string());
                    placed = true; // is now placed and doesnt have to be added at the end
                    break;
                }
                // otherwise they are equal and it needs to check the next stat to compare
            }
            if placed {
                break;
            };
        }
        if !placed {
            ordered_options.push(new_option.to_string());
        }
    }
    println!("Ordered Options: {:?}", ordered_options);

    for option in ordered_options {
        let mut option_results = vec![option.clone()];
        for stat in option_stats.get(&option).unwrap() {
            option_results.push(stat.to_string());
        }
        send_from_tower(MessageType::NewOption, option_results.join(","), room_tower);
    }
}
