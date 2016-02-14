extern crate rustc_serialize;
extern crate redis;

use std::str;
use std::thread;
use std::ops::{Deref, Drop};
use std::convert::From;
use std::iter::Iterator;
use std::marker::PhantomData;
use rustc_serialize::{json, Decodable};
use redis::{Value, RedisResult, ErrorKind, Commands};

#[derive(RustcDecodable, RustcEncodable)]
struct Job {
    id: u64
}

pub trait TaskDecodable where Self: Sized {
    fn decode_task(value: &Value) -> RedisResult<Self>;
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

struct TaskGuard<'a, T: 'a> {
    task: T,
    worker: &'a Worker<T>
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

struct Worker<T> {
    queue_name: String,
    backup_queue: String,
    pub client: redis::Connection,
    _job_type: PhantomData<T>,
}

impl<T: TaskDecodable> Worker<T> {
    pub fn new(name: String, client: redis::Connection) -> Worker<T> {
        let backup_queue = [thread::current().name().unwrap_or("default".into()), "1"].join(":");
        Worker {
            queue_name: name,
            backup_queue: backup_queue,
            client: client,
            _job_type: PhantomData,
        }
    }

    fn next(&self) -> Option<RedisResult<TaskGuard<T>>> {
        let qname = &self.queue_name[..];
        let backup = &self.backup_queue[..];

        let v = match self.client.brpoplpush(qname, backup, 0) {
            Ok(v) => v,
            Err(e) => {
                return Some(Err(From::from((ErrorKind::TypeError, "next failed"))));
            }
        };

        let v = match v {
            v @ Value::Data(_) => v,
            _ => {
                return Some(Err(From::from((ErrorKind::TypeError, "Not a 2-item Bulk reply"))));
            }
        };

        let task = T::decode_task(&v).unwrap();

        Some(Ok(TaskGuard{task: task, worker: self}))
    }
}


#[test]
fn decodes_job() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();

    let _ : () = con.rpush("default", "{\"id\":42}").unwrap();

    let mut worker = Worker::<Job>::new("default".into(), con);
    let j = worker.next().unwrap().unwrap();
    assert_eq!(42, j.id);
}

#[test]
fn releases_job() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    let mut worker = Worker::<Job>::new("default".into(), con);

    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();

    let _ : () = con.lpush("default", "{\"id\":42}").unwrap();

    {
        let j = worker.next().unwrap().unwrap();
        assert_eq!(42, j.id);
        let in_backup : Vec<String> = con.lrange("releases_job:1", 0, -1).unwrap();
        assert_eq!(1, in_backup.len());
        assert_eq!("{\"id\":42}", in_backup[0]);
    }

    let in_backup : Vec<String> = con.lrange("releases_job:1", 0, -1).unwrap();
    assert_eq!(0, in_backup.len());
}
