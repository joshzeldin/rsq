use rsq::{KObj, KType, open, send_sync, send_async};

fn main() {
    let mut conn = open("127.0.0.1:1234", "username", "password").unwrap();
    println!("{:?}", send_sync(&mut conn, &KObj::Atom(KType::Symbol(String::from("variable")))));
}