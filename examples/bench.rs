extern crate rustc_serialize;
extern crate redis;
extern crate oppgave;
extern crate libc;

use oppgave::Queue;
use std::str::FromStr;
use std::process::Command;
use std::time::{Duration, Instant};
use std::{thread, process};

#[derive(RustcDecodable, RustcEncodable, Debug)]
struct Job {
    id: usize
}

impl Job {
    pub fn process(&self) {
        // This is left empty
    }
}

fn load(n: usize) {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    let q = Queue::new("default".into(), con);

    let now = Instant::now();
    for i in 0..n {
        let j = Job{ id: i };
        q.push(j).unwrap();
    }

    println!("Created {} tasks in {} seconds", n, now.elapsed().as_secs());
}

fn process_rss(pid: i32) -> u64 {
    let output = Command::new("ps")
        .arg("-o")
        .arg("rss=")
        .arg("-p")
        .arg(format!("{}", pid))
        .output()
        .unwrap_or_else(|e| { panic!("failed to execute process: {}", e)  });

    let s = String::from_utf8_lossy(&output.stdout);
    let end = s.find('\n').unwrap();
    FromStr::from_str(&s[1..end]).unwrap()
}

fn main() {
    let total = 10 * 10_000;
    load(total);

    thread::spawn(move || {
        let pid = unsafe { libc::getpid() };
        let queue_name = "oppgave:default";
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();

        let now = Instant::now();

        loop {
            let count : u64 = redis::cmd("LLEN").arg(queue_name).query(&con).unwrap();
            if count == 0 {
                let elapsed = now.elapsed().as_secs();
                let per_second = total as f64 / elapsed as f64;
                println!("Done in {}: {} jobs/sec", elapsed, per_second);
                process::exit(0);
            }

            println!("pid: {}, count: {}, rss: {}", pid, count, process_rss(pid));
            thread::sleep(Duration::from_millis(200));
        }
    });


    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    let worker = Queue::new("default".into(), con);

    while let Some(task) = worker.next::<Job>() {
        if task.is_err() { continue; }
        let task = task.unwrap();
        task.process();
    }
}
