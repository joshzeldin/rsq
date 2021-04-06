use super::ktype::KType;
use std::fmt;
use uuid::Uuid;
use chrono::Utc;
use byteorder::{LittleEndian, WriteBytesExt};


#[derive(Debug)]
#[derive(PartialEq)]
pub enum KObj {
    Atom(KType),
    List(Vec<KObj>),
    GenericList(Vec<KObj>),
    Dict(Vec<KObj>, Vec<KObj>),
    Table(Vec<KObj>, Vec<KObj>),
    Error(String),
    Lambda(String)
}

impl fmt::Display for KObj {
    fn fmt(&self, f:&mut fmt::Formatter) -> fmt::Result {
        match self {
            KObj::Atom(k) => k.fmt(f),
            KObj::List(k) => {
                let list: Vec<String> = k.iter().map(|x|format!("{}", x)).collect();
                let needs_enlist = if 1 == list.len(){
                    String::from("enlist ")
                } else {
                    String::from("")
                };
                let string_list = String::from("(") + &needs_enlist + &list.join(";") + ")";
                write!(f, "{}", string_list)
            },
            KObj::GenericList(k) => {
                let list: Vec<String> = k.iter().map(|x|format!("{}", x)).collect();
                let needs_enlist = if 1 == list.len(){
                    String::from("enlist ")
                } else {
                    String::from("")
                };
                let string_list = String::from("(") + &needs_enlist + &list.join(";") + ")";
                write!(f, "{}", string_list)
            },
            KObj::Dict(k,v) => {
                let keys: Vec<String> = k.iter().map(|x|format!("{}", x)).collect();
                let keys = String::from("(") + &keys.join(";") + ")";
                write!(f, "{}!", keys).ok();
                let vals: Vec<String> = v.iter().map(|x|format!("{}", x)).collect();
                let vals = String::from("(") + &vals.join(";") + ")";
                write!(f, "{}", vals)
            },
            KObj::Table(k,v) => {
                let keys: Vec<String> = k.iter().map(|x|format!("{}", x)).collect();
                let keys = String::from("(") + &keys.join(";") + ")";
                write!(f, "flip {}!", keys).ok();
                let vals: Vec<String> = v.iter().map(|x|format!("{}", x)).collect();
                let vals = String::from("(") + &vals.join(";") + ")";
                write!(f, "{}", vals)
            },
            KObj::Lambda(l) => {
                write!(f, "{}", l)
            }
            KObj::Error(e) => {
                write!(f, "'{}", e)
            }
        }
    }
}


impl KObj {

    pub fn new(code: i8) -> KObj {
        match code {
            code if (code > 0 && code <= 19) && (code != 10) => KObj::List(vec![]),
             00 => KObj::GenericList(vec![]),
            -01 => KObj::Atom(KType::Boolean(false)),
            -02 => KObj::Atom(KType::Guid(Uuid::nil())),
            -04 => KObj::Atom(KType::Byte(0)),
            -05 => KObj::Atom(KType::Short(0)),
            -06 => KObj::Atom(KType::Int(0)),
            -07 => KObj::Atom(KType::Long(0)),
            -08 => KObj::Atom(KType::Real(0.)),
            -09 => KObj::Atom(KType::Float(0.)),
            -10 => KObj::Atom(KType::Char(' ')),
             10 => KObj::Atom(KType::String(String::from(""))),
            -11 => KObj::Atom(KType::Symbol(String::from(""))),
            -12 => KObj::Atom(KType::Timestamp(Utc::now())),
            -13 => KObj::Atom(KType::Month(Utc::today())),
            -14 => KObj::Atom(KType::Date(Utc::today())),
            -15 => KObj::Atom(KType::Datetime(Utc::now())),
            -16 => KObj::Atom(KType::Timespan(Utc::now())),
            -17 => KObj::Atom(KType::Minute(Utc::now())),
            -18 => KObj::Atom(KType::Second(Utc::now())),
            -19 => KObj::Atom(KType::Time(Utc::now())),
             99 => KObj::Dict(vec![], vec![]),
             98 => KObj::Table(vec![], vec![]),
            100 => KObj::Lambda(String::from("")),
            101 => KObj::Atom(KType::Unary(0)),
            102 => KObj::Atom(KType::Operator(0)),
           -128 => KObj::Error(String::from("")),
              _ => KObj::Error(String::from(""))
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            KObj::Atom(t) => t.serialize(),
            KObj::List(t) => {
                let mut result = vec![];
                // 1 byte for attribute
                result.push(0);

                // 4 bytes for length
                let mut length_buf = vec![]; 
                length_buf.write_i32::<LittleEndian>(t.len() as i32).unwrap();
                for b in length_buf.iter(){
                    result.push(*b);
                };
                // ? bytes for data
                for k in t.iter() {
                    for byte in k.serialize(){
                        result.push(byte);
                    };
                };
                result
            },
            KObj::GenericList(t) => {
                let mut result = vec![];
                // 1 byte for attribute
                result.push(0);

                // 4 bytes for length
                let mut length_buf = vec![]; 
                length_buf.write_i32::<LittleEndian>(t.len() as i32).unwrap();
                for b in length_buf.iter(){
                    result.push(*b);
                };
                // ? bytes for data
                for k in t.iter() {
                    result.push(k.type_as_bytes());
                    for byte in k.serialize(){
                        result.push(byte);
                    };
                };
                result
            },
            KObj::Dict(_,_) => vec![],
            KObj::Table(_,_) => vec![],
            KObj::Lambda(_) => vec![],
            KObj::Error(_) => vec![]
        }
    }

    pub fn type_as_bytes(&self) -> u8 {
        let code = match self {
            KObj::Atom(t) => t.type_as_code() as u8,
            // todo: support generic lists
            // assumes all lists are of the type of the first element
            KObj::List(t) => {
                match &t[0] {
                    KObj::Atom(t) => (-1 * t.type_as_code()) as u8,
                    _ => 0u8,
                }
            },
            KObj::GenericList(_) => 0u8,
            KObj::Dict(_,_) => 99u8,
            KObj::Table(_,_) => 98u8,
            KObj::Lambda(_) => 100u8,
            KObj::Error(_) =>  0u8
        };
        code as u8
    }

    pub fn deserialize(&self, data: &Vec<u8>) -> KObj{
        match self {
            KObj::Atom(t) => KObj::Atom(t.deserialize(data)),
            _             => KObj::List(vec![]),  // this will never get used
        }
    }
}