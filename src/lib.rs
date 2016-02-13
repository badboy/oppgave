extern crate rustc_serialize;
extern crate redis;

use std::str;
use std::convert::From;
use std::iter::Iterator;
use std::marker::PhantomData;
use rustc_serialize::json;
use redis::{FromRedisValue, Value, RedisResult, ErrorKind, Commands};

#[derive(RustcDecodable, RustcEncodable)]
struct Job {
    id: u64
}

macro_rules! enqueueable {
    ($typ:ty) => {
        impl FromRedisValue for $typ {
            fn from_redis_value(value: &Value) -> RedisResult<Self> {
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
    }
}

enqueueable!(Job);

struct Worker<T> {
    queue_name: String,
    client: redis::Connection,
    _job_type: PhantomData<T>,
}

impl<T: FromRedisValue> Worker<T> {
    pub fn new(name: String, client: redis::Connection) -> Worker<T> {
        Worker {
            queue_name: name,
            client: client,
            _job_type: PhantomData,
        }
    }

    fn _next(&self) -> RedisResult<T> {
        let v = Value::Data(vec![123, 34, 105, 100, 34, 58, 52, 50, 125]);
        FromRedisValue::from_redis_value(&v)
    }
}

impl<T: FromRedisValue> Iterator for Worker<T> {
    type Item = RedisResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let qname = &self.queue_name[..];

        let v = match self.client.blpop(qname, 0) {
            Ok(v) => v,
            Err(e) => {
                return Some(Err(From::from((ErrorKind::TypeError, "next failed"))));
            }
        };

        let v = match v {
            Value::Bulk(ref v) if v.len() == 2 => &v[1],
            _ => {
                return Some(Err(From::from((ErrorKind::TypeError, "Not a 2-item Bulk reply"))));
            }
        };

        Some(FromRedisValue::from_redis_value(&v))
    }
}



#[test]
fn decodes_job() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();

    let job = "{\"id\":42}".chars().map(|c| c as u8).collect::<Vec<_>>();

    let mut worker = Worker::<Job>::new("default".into(), con);
    let j : Job = worker.next().unwrap().unwrap();
    assert_eq!(42, j.id);
}
