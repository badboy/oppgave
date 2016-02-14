# oppgave - A simple Redis-based task queue

[![Build Status](https://travis-ci.org/badboy/oppgave.svg?branch=master)](https://travis-ci.org/badboy/oppgave)
[![crates.io](http://meritbadge.herokuapp.com/oppgave)](https://crates.io/crates/oppgave)

## The name: oppgave - task

`oppgave` is Norwegian for task.
So `oppgave` is a **oppgave kø**, a **task queue**.

Sadly, characters like `ø` don't play to well with stable Rust. [Non-ASCII identifiers are feature-gated](https://github.com/rust-lang/rust/issues/28979).

If you are okay with using nightly, you can get `oppgave-kø` instead and add `#![feature(non_ascii_idents)]` to your main crate file.

## [Documentation][]

[Documentation is available online.][documentation]

[documentation]: http://badboy.github.io/oppgave

## Installation

Add it to your dependencies in `Cargo.toml`

```toml
[dependencies]
oppgave = "0.1.0"
```

## Example: Producer

See [`examples/worker.rs`](examples/worker.rs) for a working example.
Run it with `cargo run --example worker`.

```rust
#[derive(RustcDecodable, RustcEncodable)]
struct Job { id: u64 }

let client = redis::Client::open("redis://127.0.0.1/").unwrap();
let con = client.get_connection().unwrap();
let producer = Queue::new("default".into(), con);

producer.push(Job{ id: 42 });
```

## Example: Worker

See [`examples/worker.rs`](examples/worker.rs) for a working example.
Run it with `cargo run --example worker`.

```rust
#[derive(RustcDecodable, RustcEncodable)]
struct Job { id: u64 }

let client = redis::Client::open("redis://127.0.0.1/").unwrap();
let con = client.get_connection().unwrap();
let worker = Queue::new("default".into(), con);

while let Some(task) = worker.next() {
    println!("Working with Job {}", job.id);
}
```

## License

MIT. See [LICENSE](LICENSE).
