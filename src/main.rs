use rsq::{Kdb, KObj, KType};

fn main() {
    let mut kdb = Kdb::new("localhost", 1234, "josh", "password");
    println!("{:?}", kdb.send_sync(&KObj::List(vec![
        KType::Char('t'),
        KType::Char('i'),
        KType::Char('l'),
        KType::Char(' '),
        KType::Char('1'),
        KType::Char('0'),
        KType::Char('0'),
        ]
    )));

}