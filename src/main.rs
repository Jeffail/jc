#[macro_use]
extern crate chan;
extern crate chan_signal;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use std::thread;
use std::io::BufRead;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use chan_signal::Signal;

fn main() {
    // Signal gets a value when the OS sent a INT or TERM signal.
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);

    // When our work is complete, send a sentinel value on `sdone`.
    let (sdone, rdone) = chan::sync(0);

    // Run work.
    thread::spawn(move || run(sdone));

    // Wait for a signal or for work to be done.
    chan_select! {
        signal.recv() -> signal => {
            println!("Received signal: {:?}, shutting down...", signal.unwrap())
        },
        rdone.recv() => {}
    }
}

#[derive(PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum JValue {
    Null,
    Bool(bool),
    Float(f64),
    String(String),
    Array(Vec<JValue>),
    Object(HashMap<String, JValue>),
}

impl Eq for JValue {}

impl Hash for JValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            &JValue::Null => state.write_u8(0),
            &JValue::Bool(ref b) => state.write_u8(if *b { 1u8 } else { 0u8 }),
            &JValue::Float(ref n) => state.write(format!("{}", n).as_bytes()),
            &JValue::String(ref s) => state.write(s.as_bytes()),
            &JValue::Array(ref a) => a.hash(state),
            &JValue::Object(ref o) => for el in o {
                el.hash(state);
            },
        }
        state.finish();
    }
}

// Populate our hashmap of key to value set by traversing the JSON object.
fn map_value(path: String, map: &mut HashMap<String, HashSet<JValue>>, val: JValue) {
    match val {
        JValue::Null | JValue::Bool(_) | JValue::Float(_) | JValue::String(_) => {
            if !map.contains_key(&path) {
                map.insert(path.clone(), HashSet::<JValue>::new());
            }
        }
        _ => {}
    }
    match val {
        JValue::Null => {
            map.get_mut(&path).unwrap().insert(JValue::Null);
        }
        JValue::Bool(b) => {
            map.get_mut(&path).unwrap().insert(JValue::Bool(b));
        }
        JValue::Float(f) => {
            map.get_mut(&path).unwrap().insert(JValue::Float(f));
        }
        JValue::String(s) => {
            map.get_mut(&path).unwrap().insert(JValue::String(s));
        }
        JValue::Array(a) => for ele in a {
            map_value(path.clone(), map, ele);
        },
        JValue::Object(o) => for (next, ele) in o {
            let mut new_path = path.clone();
            if new_path.len() == 0 {
                new_path = next;
            } else {
                new_path = [new_path, next].join(".");
            }
            map_value(new_path, map, ele);
        },
    }
}

fn run(_sdone: chan::Sender<()>) {
    let mut v_map = HashMap::<String, HashSet<JValue>>::new();
    let stdin = std::io::stdin();

    // Read line delimited JSON blobs.
    for line in stdin.lock().lines() {
        let val: JValue = match serde_json::from_str::<JValue>(&line.unwrap()) {
            Ok(v) => v,
            Err(e) => {
                println!("Error parsing JSON: {:?}", e);
                continue;
            }
        };

        map_value(String::from(""), &mut v_map, val);
    }

    // Create a key to cardinality hashmap.
    let mut c_map = HashMap::<String, usize>::new();
    for (key, set) in v_map {
        c_map.insert(key, set.len());
    }

    // Print the hashmap in JSON format.
    println!("{}", serde_json::to_string(&c_map).unwrap());
}
