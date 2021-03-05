use rsq::{Kdb,KObj,KType};
use chrono::Utc;


fn main() {
    let mut kdb = Kdb::new("localhost", 1234, "josh", "password");
    loop {
        kdb.send_async(&KObj::GenericList(vec![
            KObj::Atom(KType::Symbol(".u.upd".to_string())),
            KObj::Atom(KType::Symbol("trade".to_string())),
            KObj::GenericList(vec![
                KObj::GenericList(vec![
                    KObj::Atom(KType::Timestamp(Utc::now())),
                    KObj::Atom(KType::Symbol("TSLA".to_string())),
                    KObj::Atom(KType::Float(630.2)),
                    KObj::Atom(KType::Int(100))
                ]),
            ])
       ])).unwrap();    
    }
}