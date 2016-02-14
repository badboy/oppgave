extern crate rustc_serialize;
extern crate redis;

use std::str;
use std::cell::Cell;
use std::thread;
use std::ops::{Deref, Drop};
use std::convert::From;
use rustc_serialize::{json, Decodable, Encodable};
use redis::{Value, RedisResult, ErrorKind, Commands};

pub trait TaskDecodable where Self: Sized {
    fn decode_task(value: &Value) -> RedisResult<Self>;
}

pub trait TaskEncodable {
    fn encode_task(&self) -> Vec<u8>;
}

impl<T: Decodable> TaskDecodable for T {
    fn decode_task(value: &Value) -> RedisResult<T> {
        match value {
            &Value::Data(ref v) => {
                let s = try!(str::from_utf8(&v));
                Ok(try!(json::decode(&s).map_err(|_| (ErrorKind::TypeError, "JSON decode failed"))))

            },
            v @ _ => {
                println!("what do we have here: {:?}", v);
                try!(Err((ErrorKind::TypeError, "Can only decode from a string")))
            }
        }
    }
}

impl<T: Encodable> TaskEncodable for T {
    fn encode_task(&self) -> Vec<u8> {
        json::encode(self).unwrap().into_bytes()
    }
}

pub struct TaskGuard<'a, T: 'a> {
    task: T,
    worker: &'a Queue
}

impl<'a, T> TaskGuard<'a, T> {
    pub fn stop(&self) {
        self.worker.stop();
    }

    pub fn inner(&self) -> &T {
        &self.task
    }
}

impl<'a, T> Deref for TaskGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.task
    }
}

impl<'a, T> Drop for TaskGuard<'a, T> {
    fn drop(&mut self) {
        // Pop job from backup queue
        let backup = &self.worker.backup_queue[..];
        let _ : () = self.worker.client.lpop(backup).expect("LPOP from backup queue failed");
    }
}

pub struct Queue {
    queue_name: String,
    backup_queue: String,
    stopped: Cell<bool>,
    pub client: redis::Connection,
}

impl Queue {
    pub fn new(name: String, client: redis::Connection) -> Queue {
        let backup_queue = [thread::current().name().unwrap_or("default".into()), "1"].join(":");
        Queue {
            queue_name: name,
            backup_queue: backup_queue,
            client: client,
            stopped: Cell::new(false),
        }
    }

    pub fn stop(&self) {
        self.stopped.set(true);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.get()
    }

    pub fn queue(&self) -> &str {
        &self.queue_name
    }

    pub fn backup_queue(&self) -> &str {
        &self.backup_queue
    }

    pub fn size(&self) -> u64 {
        self.client.llen(self.queue()).unwrap_or(0)
    }

    pub fn push<T: TaskEncodable>(&self, task: T) {
        let _ : () = self.client.lpush(self.queue(), task.encode_task()).expect("LPUSH failed to enqueue task");
    }

    pub fn next<T: TaskDecodable>(&self) -> Option<RedisResult<TaskGuard<T>>> {
        if self.stopped.get() {
            return None;
        }

        let v;
        {
            let qname = &self.queue_name[..];
            let backup = &self.backup_queue[..];

            v = match self.client.brpoplpush(qname, backup, 0) {
                Ok(v) => v,
                Err(_) => {
                    return Some(Err(From::from((ErrorKind::TypeError, "next failed"))));
                }
            };
        }

        let v = match v {
            v @ Value::Data(_) => v,
            _ => {
                return Some(Err(From::from((ErrorKind::TypeError, "Not a proper reply"))));
            }
        };

        let task = T::decode_task(&v).unwrap();

        Some(Ok(TaskGuard{task: task, worker: self}))
    }
}


#[cfg(test)]
mod test {
    extern crate redis;

    use redis::Commands;
    use super::*;

    #[derive(RustcDecodable, RustcEncodable)]
    struct Job {
        id: u64
    }

    #[test]
    fn decodes_job() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Queue::new("default".into(), con2);

        let _ : () = con.rpush(worker.queue(), "{\"id\":42}").unwrap();

        let j = worker.next::<Job>().unwrap().unwrap();
        assert_eq!(42, j.id);
    }

    #[test]
    fn releases_job() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Queue::new("default".into(), con2);
        let bqueue = worker.backup_queue();

        let _ : () = con.del(bqueue).unwrap();
        let _ : () = con.lpush("default", "{\"id\":42}").unwrap();

        {
            let j = worker.next::<Job>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_backup : Vec<String> = con.lrange(bqueue, 0, -1).unwrap();
            assert_eq!(1, in_backup.len());
            assert_eq!("{\"id\":42}", in_backup[0]);
        }

        let in_backup : u32 = con.llen(bqueue).unwrap();
        assert_eq!(0, in_backup);
    }

    #[test]
    fn can_be_stopped() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Queue::new("stopper".into(), con2);

        let _ : () = con.del(worker.queue()).unwrap();
        let _ : () = con.lpush(worker.queue(), "{\"id\":1}").unwrap();
        let _ : () = con.lpush(worker.queue(), "{\"id\":2}").unwrap();
        let _ : () = con.lpush(worker.queue(), "{\"id\":3}").unwrap();

        assert_eq!(3, worker.size());

        while let Some(task) = worker.next::<Job>() {
            let task = task.unwrap();
            task.stop();
        }

        assert_eq!(2, worker.size());
    }

    #[test]
    fn can_enqueu() {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let con = client.get_connection().unwrap();
        let _ : () = con.del("enqueue").unwrap();

        let worker = Queue::new("enqueue".into(), con);

        assert_eq!(0, worker.size());

        worker.push(Job{id: 53});

        assert_eq!(1, worker.size());

        let j = worker.next::<Job>().unwrap().unwrap();
        assert_eq!(53, j.id);
    }
}
