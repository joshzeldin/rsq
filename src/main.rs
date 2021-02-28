use rsq::{Kdb, KObj, KType};

fn main() {
    let mut kdb = Kdb::new("localhost", 1234, "josh", "password");
    loop{
        kdb.read();
    }
}