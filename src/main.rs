use rsq::*;
// use uuid::Uuid;

fn main() {
    let mut conn = open("127.0.0.1:1234", "josh", "password").unwrap();
    println!("{:?}", send_sync(&mut conn, &KObj::Atom(KType::Symbol(String::from("josh")))));
    // loop {
    //     let data = read(&mut conn);
    //     println!("{:?}", data);
    //     conn.take_error().expect("No error was expected...");
    // }

}