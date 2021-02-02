use rsq::*;
use uuid::Uuid;

fn main() {
    let mut conn = open("127.0.0.1:1234", "josh", "password").unwrap();
    println!("{:?}", send_sync(&mut conn, &to_kfloat(3.14152653)));
}