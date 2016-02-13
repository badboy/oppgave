extern crate rustc_serialize;
extern crate redis;

use std::str;
use std::iter::Iterator;
use std::marker::PhantomData;
use rustc_serialize::json;
use redis::{FromRedisValue, Value, RedisResult, ErrorKind};

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
                    _ => try!(Err((ErrorKind::TypeError, "Can only decode from a string")))
                }
            }
        }
    }
}

enqueueable!(Job);

struct Worker<T> {
    queue_name: String,
    _job_type: PhantomData<T>,
}

impl<T: FromRedisValue> Worker<T> {
    pub fn new(name: String) -> Worker<T> {
        Worker {
            queue_name: name,
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
        Some(self._next())
    }
}



#[test]
fn decodes_job() {
    let job = "{\"id\":42}".chars().map(|c| c as u8).collect::<Vec<_>>();

    let mut worker = Worker::<Job>::new("default".into());
    let j : Job = worker.next().unwrap().unwrap();
    assert_eq!(42, j.id);
}
