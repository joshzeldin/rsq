use std::net::{ TcpStream, ToSocketAddrs};
use std::io::prelude::*;
use std::io::{Error, ErrorKind};
use byteorder::{ByteOrder, LittleEndian};
use uuid::Uuid;
use chrono::{Date, DateTime, Utc, NaiveDateTime, NaiveDate};
use std::{thread, time};

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

pub fn to_kint(int: i32) -> [u8; 13]{
    let mut msg: [u8; 13] = [1, 1, 0, 0, 13, 0, 0, 0, 250, 0, 0, 0, 0];
    let mut buf = [0; 4];
    LittleEndian::write_i32(&mut buf, int);
    for (i, x) in buf.iter().rev().enumerate(){
        msg[12 - i] = *x;
    };
    msg
}

pub fn to_klong(long: i64) -> [u8; 17]{
    let mut msg: [u8; 17] = [1, 1, 0, 0, 17, 0, 0, 0, 249, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut buf = [0; 8];
    LittleEndian::write_i64(&mut buf, long);
    for (i, x) in buf.iter().rev().enumerate(){
        msg[16 - i] = *x;
    };
    msg
}

pub fn to_kreal(real: f32) -> [u8; 13]{
    let mut msg: [u8; 13] = [1, 1, 0, 0, 13, 0, 0, 0, 248, 0, 0, 0, 0];
    let mut buf = [0; 4];
    LittleEndian::write_f32(&mut buf, real);
    for (i, x) in buf.iter().rev().enumerate(){
        msg[12 - i] = *x;
    };
    msg
}

pub fn to_kfloat(float: f64) -> [u8; 17]{
    let mut msg: [u8; 17] = [1, 1, 0, 0, 17, 0, 0, 0, 247, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut buf = [0; 8];
    LittleEndian::write_f64(&mut buf, float);
    for (i, x) in buf.iter().rev().enumerate(){
        msg[16 - i] = *x;
    };
    msg
}

pub fn to_kdate(int: i32) -> [u8; 13]{
    let mut msg: [u8; 13] = [1, 1, 0, 0, 13, 0, 0, 0, 242, 0, 0, 0, 0];
    let mut buf = [0; 4];
    LittleEndian::write_i32(&mut buf, int);
    for (i, x) in buf.iter().rev().enumerate(){
        msg[12 - i] = *x;
    };
    msg
}

pub fn send_async(stream: &mut TcpStream, data: &[u8]) {
    stream.write(data).unwrap();
    stream.flush().unwrap();
}

pub fn send_sync(mut stream: &mut TcpStream, data: &[u8]) -> KType {
    stream.write(data).unwrap();
    stream.flush().unwrap();
    let response = read(&mut stream);
    response
}

#[derive(Debug)]
pub enum KType {
    Boolean(bool),
    BooleanList(Vec<bool>),
    Guid(Uuid),
    GuidList(Vec<Uuid>),
    Byte(u8),
    ByteList(Vec<u8>),
    Short(i16),
    ShortList(Vec<i16>),
    Int(i32),
    IntList(Vec<i32>),
    Long(i64),
    LongList(Vec<i64>),
    Real(f32),
    RealList(Vec<f32>),
    Float(f64),
    FloatList(Vec<f64>),
    Char(char),
    CharList(Vec<char>),
    Symbol(String),
    SymbolList(Vec<String>),
    Timestamp(DateTime<Utc>),
    TimestampList(Vec<DateTime<Utc>>),
    Month(Date<Utc>),
    MonthList(Vec<Date<Utc>>),
    Date(Date<Utc>),
    DateList(Vec<Date<Utc>>),
    Datetime(DateTime<Utc>),
    DatetimeList(Vec<DateTime<Utc>>),
    Timespan(DateTime<Utc>),
    TimespanList(Vec<DateTime<Utc>>),
    Minute(DateTime<Utc>),
    MinuteList(Vec<DateTime<Utc>>),
    Second(DateTime<Utc>),
    SecondList(Vec<DateTime<Utc>>),
    Time(DateTime<Utc>),
    TimeList(Vec<DateTime<Utc>>),
    Other(Vec<i32>),
}

pub enum KList {
    Boolean(Vec<KType>),
    Guid(Vec<KType>),
    Byte(Vec<KType>),
    Short(Vec<KType>),
    Int(Vec<KType>),
    Long(Vec<KType>),
    Real(Vec<KType>),
    Float(Vec<KType>),
    Char(Vec<KType>),
    Symbol(Vec<KType>),
    Timestamp(Vec<KType>),
    Month(Vec<KType>),
    Date(Vec<KType>),
    Datetime(Vec<KType>),
    Timespan(Vec<KType>),
    Minute(Vec<KType>),
    Second(Vec<KType>),
    Time(Vec<KType>),
    Other(Vec<KType>),
}

pub enum KObj {
    Atom(KType),
    List(Vec<KType>)
}

pub fn get_ktype(code: i8) -> KType {
    match code {
        -01 => KType::Boolean(false),
         01 => KType::BooleanList(vec![]),
        -02 => KType::Guid(Uuid::nil()),
         02 => KType::GuidList(vec![]),
        -04 => KType::Byte(0),
         04 => KType::ByteList(vec![]),
        -05 => KType::Short(0),
         05 => KType::ShortList(vec![]),
        -06 => KType::Int(0),
         06 => KType::IntList(vec![]),
        -07 => KType::Long(0),
         07 => KType::LongList(vec![]),
        -08 => KType::Real(0.),
         08 => KType::RealList(vec![]),
        -09 => KType::Float(0.),
         09 => KType::FloatList(vec![]),
        -10 => KType::Char(' '),
         10 => KType::CharList(vec![]),
        -11 => KType::Symbol(String::from("")),
         11 => KType::SymbolList(vec![]),
        -12 => KType::Timestamp(Utc::now()),
         12 => KType::TimestampList(vec![]),
        -13 => KType::Month(Utc::today()),
         13 => KType::MonthList(vec![]),
        -14 => KType::Date(Utc::today()),
         14 => KType::DateList(vec![]),
        -15 => KType::Datetime(Utc::now()),
         15 => KType::DatetimeList(vec![]),
        -16 => KType::Timespan(Utc::now()),
         16 => KType::TimespanList(vec![]),
        -17 => KType::Minute(Utc::now()),
         17 => KType::MinuteList(vec![]),
        -18 => KType::Second(Utc::now()),
         18 => KType::SecondList(vec![]),
        -19 => KType::Time(Utc::now()),
         19 => KType::TimeList(vec![]),
        _ => KType::Other(vec![]),
    }
}

fn convert_to_ktype (ktype: KType,data: &Vec<u8>) -> KType {
    match ktype {
        KType::Boolean(_n) => KType::Boolean(data[0] == 1),
        KType::Guid(_n) => KType::Guid(Uuid::from_slice(data).unwrap()),
        KType::Byte(_n) => KType::Byte(data[0]),
        KType::Short(_n) => KType::Short(LittleEndian::read_i16(data)),
        KType::Int(_n) => KType::Int(LittleEndian::read_i32(data)),
        KType::Long(_n) => KType::Long(LittleEndian::read_i64(data)),
        KType::Real(_n) => KType::Real(LittleEndian::read_f32(data)),
        KType::Float(_n) => KType::Float(LittleEndian::read_f64(data)),
        KType::Char(_n) => KType::Char(data[0] as char),
        // KType::Symbol(n) => vec![0;1024],
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
        _ => KType::Other(vec![]),
    }

}

fn read_data(stream: &mut TcpStream, header: &Header) -> KType {
    let ktype = get_ktype(header.msg_type);
    let mut buffer = match &ktype {
        KType::Boolean(_n) => vec![0;1],
        KType::Guid(_n) => vec![0;16],
        KType::Byte(_n) => vec![0;1],
        KType::Short(_n) => vec![0;2],
        KType::Int(_n) => vec![0;4],
        KType::Long(_n) => vec![0;8],
        KType::Real(_n) => vec![0;4],
        KType::Float(_n) => vec![0;8],
        KType::Char(_n) => vec![0;1],
        KType::Symbol(_n) => vec![0;1024],
        KType::Timestamp(_n) => vec![0;8],
        KType::Month(_n) => vec![0;4],
        KType::Date(_n) => vec![0;4],
        KType::Datetime(_n) => vec![0;8],
        KType::Timespan(_n) => vec![0;8],
        KType::Minute(_n) => vec![0;4],
        KType::Second(_n) => vec![0;4],
        KType::Time(_n) => vec![0;4],
        _ => vec![0;1024],
    };
    stream.read(&mut buffer).unwrap();
    convert_to_ktype(ktype, &buffer)

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
    stream.read(&mut [0;2]).unwrap(); //throw away two padding bytes
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

pub fn read(mut stream: &mut TcpStream) -> KType {
    let msg_header = read_header(&mut stream);
    println!("{:#?}", msg_header);
    read_data(&mut stream, &msg_header)
}