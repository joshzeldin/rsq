use rsq::{Kdb,KObj,KType};

fn main() {
    let mut kdb = Kdb::new("localhost", 1234, "josh", "password");
    let response = kdb.send_sync(&KObj::List(vec![
         KObj::Atom(KType::Symbol("upd".to_string())),
         KObj::Atom(KType::Symbol("trade".to_string())),
         KObj::Atom(KType::Symbol("".to_string()))
    ])).unwrap();
    println!("{}",response);
    loop {
        println!("{}",kdb.read());
    }
}