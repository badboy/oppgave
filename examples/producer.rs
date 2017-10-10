#[macro_use]
extern crate serde_derive;
extern crate redis;
extern crate oppgave;

use oppgave::Queue;
use std::time::Duration;
use std::thread::sleep;

#[derive(Deserialize, Serialize, Debug)]
struct Job {
    id: u64,
}

fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    let q = Queue::new("default".into(), con);

    println!("Enqueuing jobs");

    let d = Duration::from_millis(1000);
    for i in 0.. {
        println!("Pushing job {}", i);
        q.push(Job { id: i }).unwrap();
        sleep(d);
    }
}
