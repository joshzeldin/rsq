# rsq &emsp;

**rsq is a library for ipc communication with kdb+. rsq is written in native rust so unlike other libraries, there is no need to use a nightly rust build.**

---

## add rsq into your project

```toml
[dependencies]
rsq = { version = "0.1"}
```

```rust
use rsq::{KObj, KType, open, send_sync, send_async};

fn main() {
    let mut conn = open("127.0.0.1:1234", "username", "password").unwrap();
    println!("{:?}", send_sync(&mut conn, &KObj::Atom(KType::Symbol(String::from("variable")))));
}
```