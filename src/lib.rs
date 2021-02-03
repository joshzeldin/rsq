use std::net::{ TcpStream, ToSocketAddrs};
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use uuid::Uuid;
use chrono::{Date, DateTime, Utc, NaiveDateTime, NaiveDate, Datelike};

pub fn open<A: ToSocketAddrs>(addr: A, user: &str, pass: &str) -> Result<TcpStream, Error> {
    let mut stream = TcpStream::connect(addr)?;
    let response = format!("{}:{}{}",user, pass, "\x06\x00");
    stream.write(response.as_bytes())?;
    match stream.read(&mut [0; 1]) {
        Ok(s) => {
            if s == 1 {
                Ok(stream)
            } else{
                Err(Error::new(ErrorKind::Other, "kdb failure"))
            }
        },
        Err(e) => Err(e),
    }
}

pub fn send_async(stream: &mut TcpStream, data: &KObj) {
    let header_bytes = [1, 0, 0, 0].iter().cloned();
    let mut data_bytes = data.serialize().clone();
    let type_bytes = [data.type_as_bytes()];
    let mut size_bytes = vec![];
    size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
    data_bytes.splice(0..0, type_bytes.iter().cloned());
    data_bytes.splice(0..0, size_bytes.iter().cloned());
    data_bytes.splice(0..0, header_bytes);
    println!("Bytes: {:?}", data_bytes);
    stream.write(&data_bytes).unwrap();
    stream.flush().unwrap();
}

pub fn send_sync(mut stream: &mut TcpStream, data: &KObj) -> KObj {
    let header_bytes = [1, 1, 0, 0].iter().cloned();
    let mut data_bytes = data.serialize().clone();
    let type_bytes = [data.type_as_bytes()];
    let mut size_bytes = vec![];
    size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
    data_bytes.splice(0..0, type_bytes.iter().cloned());
    data_bytes.splice(0..0, size_bytes.iter().cloned());
    data_bytes.splice(0..0, header_bytes);
    println!("Bytes: {:?}", data_bytes);
    stream.write(&data_bytes).unwrap();
    stream.flush().unwrap();    let response = read(&mut stream);
    response
}

#[derive(Debug)]
pub enum KType {
    Boolean(bool),
    Guid(Uuid),
    Byte(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Real(f32),
    Float(f64),
    Char(char),
    Symbol(String),
    Timestamp(DateTime<Utc>),
    Month(Date<Utc>),
    Date(Date<Utc>),
    Datetime(DateTime<Utc>),
    Timespan(DateTime<Utc>),
    Minute(DateTime<Utc>),
    Second(DateTime<Utc>),
    Time(DateTime<Utc>),
    Other,
}

pub trait Serialize{
    fn type_as_bytes(&self) -> u8;
    fn deserialize(&self, data: &Vec<u8>) -> KObj;
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Debug)]
pub enum KObj {
    Atom(KType),
    List(Vec<KType>)
}

impl Serialize for KObj {
    fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];
        match self {
            KObj::Atom(t) => match t {
                KType::Boolean(n) => vec![*n as u8],
                KType::Guid(n) => n.as_bytes().iter().cloned().collect(),
                KType::Byte(n) => vec![*n as u8],
                KType::Short(n) => {buf.write_i16::<LittleEndian>(*n).unwrap(); buf},
                KType::Int(n) => {buf.write_i32::<LittleEndian>(*n).unwrap(); buf},
                KType::Long(n) => {buf.write_i64::<LittleEndian>(*n).unwrap(); buf},
                KType::Real(n) => {buf.write_f32::<LittleEndian>(*n).unwrap(); buf},
                KType::Float(n) => {buf.write_f64::<LittleEndian>(*n).unwrap(); buf},
                KType::Char(n) => vec![*n as u8],
                KType::Symbol(n) => {let mut sym = Vec::from(n.as_bytes());sym.push(0);sym},
                KType::Timestamp(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 94668480000000000).unwrap(); buf},
                KType::Month(n) => {buf.write_i32::<LittleEndian>(n.num_days_from_ce() - 730119).unwrap(); buf},
                KType::Date(n) => {buf.write_i32::<LittleEndian>(n.num_days_from_ce() - 730119).unwrap(); buf},
                KType::Datetime(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 94668480000000000).unwrap(); buf},
                KType::Timespan(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 94668480000000000).unwrap(); buf},
                KType::Minute(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 94668480000000000).unwrap(); buf},
                KType::Second(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 94668480000000000).unwrap(); buf},
                KType::Time(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 94668480000000000).unwrap(); buf},
                _ => vec![0]
            },
            KObj::List(t) => match t {
                _ => vec![2]
            }
        }
    }

    fn type_as_bytes(&self) -> u8 {
        let code = match self {
            KObj::Atom(t) => match t {
                KType::Boolean(_n)   => -01,
                KType::Guid(_n)      => -02,
                KType::Byte(_n)      => -04,
                KType::Short(_n)     => -05,
                KType::Int(_n)       => -06,
                KType::Long(_n)      => -07,
                KType::Real(_n)      => -08,
                KType::Float(_n)     => -09,
                KType::Char(_n)      => -10,
                KType::Symbol(_n)   => -11,
                KType::Timestamp(_n) => -12,
                KType::Month(_n)     => -13,
                KType::Date(_n)      => -14,
                KType::Datetime(_n)  => -15,
                KType::Timespan(_n)  => -16,
                KType::Minute(_n)    => -17,
                KType::Second(_n)    => -18,
                KType::Time(_n)      => -19,
                _ => 0
            },
            KObj::List(t) => match t {
                _ => 0
            }
        };
        code as u8
    }

    fn deserialize(&self, data: &Vec<u8>) -> KObj{
        match self {
            KObj::Atom(t) => { 
                let kdata = match t {
                    KType::Boolean(_n) => KType::Boolean(data[0] == 1),
                    KType::Guid(_n) => KType::Guid(Uuid::from_slice(data).unwrap()),
                    KType::Byte(_n) => KType::Byte(data[0]),
                    KType::Short(_n) => KType::Short(LittleEndian::read_i16(data)),
                    KType::Int(_n) => KType::Int(LittleEndian::read_i32(data)),
                    KType::Long(_n) => KType::Long(LittleEndian::read_i64(data)),
                    KType::Real(_n) => KType::Real(LittleEndian::read_f32(data)),
                    KType::Float(_n) => KType::Float(LittleEndian::read_f64(data)),
                    KType::Char(_n) => KType::Char(data[0] as char),
                    KType::Symbol(_n) => {let mut sym = data.clone();sym.pop();KType::Symbol(String::from_utf8(sym.to_vec()).unwrap())},
                    KType::Timestamp(_n) => {
                        let dt = LittleEndian::read_i64(data) + 946684800000000000;
                        KType::Timestamp(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(dt / 1_000_000_000, (dt % 1_000_000_000) as u32), Utc))
                    },
                    // KType::Month(n) => vec![0;4],
                    KType::Date(_n) => {
                        let dt = LittleEndian::read_i32(data) + 730119;
                        KType::Date(Date::<Utc>::from_utc(
                            NaiveDate::from_num_days_from_ce(dt), Utc))
                    },
                    KType::Datetime(_n) => {
                        let dt = (LittleEndian::read_f64(data) + 10_957.0) * 86_400.0 * 1_000_000_000.0;
                        KType::Datetime(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp((dt / 1_000_000_000.0) as i64, dt as u32 % 1_000_000_000), Utc))
                    },
                    KType::Timespan(_n) => {
                        let dt = LittleEndian::read_i64(data);
                        KType::Timespan(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(dt / 1_000_000_000, (dt % 1_000_000_000) as u32), Utc))
                    },        
                    KType::Minute(_n) => {
                        let m = LittleEndian::read_i32(data);
                        KType::Minute(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp((m * 60) as i64, 0), Utc))
                    },   
                    KType::Second(_n) => {
                        let s = LittleEndian::read_i32(data);
                        KType::Second(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp(s as i64, 0), Utc))
                    },   
                    KType::Time(_n) => {
                        let s = LittleEndian::read_i32(data);
                        KType::Time(DateTime::<Utc>::from_utc(
                            NaiveDateTime::from_timestamp((s / 1000) as i64, 1_000_000*(s % 1_000) as u32), Utc))
                    },  
                    _ => KType::Other,
                };
                KObj::Atom(kdata)
            },
            _ => KObj::List(vec![])
        }
    }
}

pub fn get_ktype(code: i8) -> KObj {
    match code {
        code if code >= 0 && code <= 19 => KObj::List(vec![]),
        -01 => KObj::Atom(KType::Boolean(false)),
        -02 => KObj::Atom(KType::Guid(Uuid::nil())),
        -04 => KObj::Atom(KType::Byte(0)),
        -05 => KObj::Atom(KType::Short(0)),
        -06 => KObj::Atom(KType::Int(0)),
        -07 => KObj::Atom(KType::Long(0)),
        -08 => KObj::Atom(KType::Real(0.)),
        -09 => KObj::Atom(KType::Float(0.)),
        -10 => KObj::Atom(KType::Char(' ')),
        -11 => KObj::Atom(KType::Symbol(String::from(""))),
        -12 => KObj::Atom(KType::Timestamp(Utc::now())),
        -13 => KObj::Atom(KType::Month(Utc::today())),
        -14 => KObj::Atom(KType::Date(Utc::today())),
        -15 => KObj::Atom(KType::Datetime(Utc::now())),
        -16 => KObj::Atom(KType::Timespan(Utc::now())),
        -17 => KObj::Atom(KType::Minute(Utc::now())),
        -18 => KObj::Atom(KType::Second(Utc::now())),
        -19 => KObj::Atom(KType::Time(Utc::now())),
        _ => KObj::Atom(KType::Other),
    }
}

fn read_data(stream: &mut TcpStream, header: &Header) -> KObj {
    let kobj = get_ktype(header.msg_type);
    let mut buffer = match &kobj {
        KObj::Atom(KType::Boolean(_n)) => vec![0;1],
        KObj::Atom(KType::Guid(_n)) => vec![0;16],
        KObj::Atom(KType::Byte(_n)) => vec![0;1],
        KObj::Atom(KType::Short(_n)) => vec![0;2],
        KObj::Atom(KType::Int(_n)) => vec![0;4],
        KObj::Atom(KType::Long(_n)) => vec![0;8],
        KObj::Atom(KType::Real(_n)) => vec![0;4],
        KObj::Atom(KType::Float(_n)) => vec![0;8],
        KObj::Atom(KType::Char(_n)) => vec![0;1],
        KObj::Atom(KType::Symbol(_n)) => vec![0;(header.length - 9) as usize],
        KObj::Atom(KType::Timestamp(_n)) => vec![0;8],
        KObj::Atom(KType::Month(_n)) => vec![0;4],
        KObj::Atom(KType::Date(_n)) => vec![0;4],
        KObj::Atom(KType::Datetime(_n)) => vec![0;8],
        KObj::Atom(KType::Timespan(_n)) => vec![0;8],
        KObj::Atom(KType::Minute(_n)) => vec![0;4],
        KObj::Atom(KType::Second(_n)) => vec![0;4],
        KObj::Atom(KType::Time(_n)) => vec![0;4],
        KObj::List(_n) => vec![0;1024],
        _ => vec![0;1024],
    };
    stream.read(&mut buffer).unwrap();
    kobj.deserialize(&buffer)

}


#[derive(Debug)]
struct Header {
    endianness: u8,
    protocol: u8,
    length: u32,
    msg_type: i8
}

fn read_header(stream: &mut TcpStream) -> Header {
    let mut endianness = [0;1];
    println!("reading endian");
    stream.read(&mut endianness).unwrap();
    let mut protocol = [0;1]; 
    stream.read(&mut protocol).unwrap(); println!("reading protocol");
    stream.read(&mut [0;2]).unwrap(); // throw away two padding bytes
    let mut msg_length = [0;4];
    stream.read(&mut msg_length).unwrap(); println!("reading length");
    let mut msg_type = [0;1];
    stream.read(&mut msg_type).unwrap(); println!("reading type");
    Header {
        endianness: u8::from_le_bytes(endianness),
        protocol: u8::from_le_bytes(protocol),
        length: u32::from_le_bytes(msg_length),
        msg_type: i8::from_le_bytes(msg_type)
    }
}

pub fn read(mut stream: &mut TcpStream) -> KObj {
    let msg_header = read_header(&mut stream);
    println!("{:#?}", msg_header);
    read_data(&mut stream, &msg_header)
}