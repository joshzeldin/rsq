use std::net::{ TcpStream};
use std::io::prelude::*;
use std::io::{Error};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use uuid::Uuid;
use chrono::{Date, DateTime, Utc, NaiveDateTime, NaiveDate, Datelike};

pub struct Kdb {
    host: String,
    port: u16,
    user: String,
    pass: String,
    stream: Option<TcpStream>,
}

struct Header {
    endian: Endian,
    protocol: u8,
    length: u32,
}

enum Endian {
    Big,
    Little
}

impl Header {
    fn read(kdb: &mut Kdb) -> Header {

        let mut stream = kdb.stream.as_ref().unwrap();
        let mut endian = [0;1];
        let mut protocol = [0;1]; 
        let mut msg_length = [0;4];

        stream.read(&mut endian).unwrap();
        stream.read(&mut protocol).unwrap();
        // throw away two padding bytes
        stream.read(&mut [0;2]).unwrap();
        stream.read(&mut msg_length).unwrap();

        if endian[0] == 1 {
            Header {
                endian: Endian::Little,
                protocol: u8::from_le_bytes(protocol),
                length: u32::from_le_bytes(msg_length),
            }
        } else {
            Header {
                endian: Endian::Big,
                protocol: u8::from_be_bytes(protocol),
                length: u32::from_be_bytes(msg_length),
            }
        }

    }
}

impl Kdb {
    pub fn new(host: &str, port: u16, user: &str, pass: &str) -> Kdb {
        Kdb {
            host: host.to_string(),
            port,
            user: user.to_string(),
            pass: pass.to_string(),
            stream: None,
        }
    }

    pub fn open(&mut self) -> Result<(),Error> {
        let mut stream = TcpStream::connect(format!("{}:{}",self.host,self.port))?;
        let response = format!("{}:{}{}",self.user, self.pass, "\x06\x00");
        stream.write(response.as_bytes())?;
        stream.read(&mut [0; 1])?;
        self.stream = Some(stream);
        Ok(())
    }

    pub fn stream(&mut self) -> &TcpStream {
        self.stream.as_ref().unwrap()
    }

    pub fn close(&mut self) -> Result<(), Error> {
        self.stream = None;
        Ok(())
    }

    pub fn send_async(&mut self, data: &KObj) -> Result<(), Error> {
        if self.stream.is_none() {
            self.open()?;
        };
        let header_bytes = [1, 0, 0, 0].iter().cloned();
        let mut data_bytes = data.serialize().clone();
        let type_bytes = [data.type_as_bytes()];
        let mut size_bytes = vec![];
        size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
        data_bytes.splice(0..0, type_bytes.iter().cloned());
        data_bytes.splice(0..0, size_bytes.iter().cloned());
        data_bytes.splice(0..0, header_bytes);
        self.stream().write(&data_bytes).unwrap();
        self.stream().flush().unwrap();
        Ok(())
    }

    pub fn read(&mut self) -> KObj {
        if self.stream.is_none() {
            self.open().unwrap();
        };
        let msg_header = Header::read(self);
        let mut stream = self.stream();


        let mut msg_type = [0;1];
        stream.read(&mut msg_type).unwrap();
        let data = self.read_data(i8::from_le_bytes(msg_type));

        if msg_header.protocol == 1 {
            self.send_response(&KObj::Atom(KType::Boolean(true))).unwrap();
        };

        data

    }

    fn extract_atom(&mut self, len: usize) -> Vec<u8> {
        let mut vec = vec![0;len];
        self.stream().read(&mut vec).unwrap();
        vec
    }

    fn extract_sym(&mut self) -> Vec<u8> {
        let mut stream = self.stream();
        let mut sym = vec![];
        let mut bit = [1;1];
        loop {
            stream.read(&mut bit).unwrap();
            if bit[0] == 0 { break };
            sym.push(bit[0]);
        }
        sym
    }

    fn read_atom(&mut self, ktype: KType) -> KObj {
        let vec_data = match ktype {
            KType::Boolean(_)   => self.extract_atom(1),
            KType::Guid(_)      => self.extract_atom(16),
            KType::Byte(_)      => self.extract_atom(1),
            KType::Short(_)     => self.extract_atom(2),
            KType::Int(_)       => self.extract_atom(4),
            KType::Long(_)      => self.extract_atom(8),
            KType::Real(_)      => self.extract_atom(4),
            KType::Float(_)     => self.extract_atom(8),
            KType::Char(_)      => self.extract_atom(1),
            KType::Symbol(_)    => self.extract_sym(),
            KType::Timestamp(_) => self.extract_atom(8),
            KType::Month(_)     => self.extract_atom(4),
            KType::Date(_)      => self.extract_atom(4),
            KType::Datetime(_)  => self.extract_atom(8),
            KType::Timespan(_)  => self.extract_atom(8),
            KType::Minute(_)    => self.extract_atom(4),
            KType::Second(_)    => self.extract_atom(4),
            KType::Time(_)      => self.extract_atom(4),
        };
        KObj::Atom(ktype).deserialize(&vec_data)
    }

    fn read_uniform_list(&mut self, msg_type: i8, len: u32) -> KObj {
        let mut list = vec![];
        for _ in 0..len {
            let data = self.read_data(-1 * msg_type);
            list.push(data);
        };  
        KObj::List(list)
    }

    fn read_generic_list(&mut self, len:u32) -> KObj {
        let mut list = vec![];
        for _ in 0..len{
            let mut msg_type = [0;1];
            self.stream().read(&mut msg_type).unwrap();
            let msg_code = i8::from_le_bytes(msg_type);
            list.push(self.read_data(msg_code));
        };  
        KObj::List(list)
    }  

    fn read_list(&mut self, msg_type: i8) -> KObj {
        let mut attr = [0;1];
        self.stream().read(&mut attr).unwrap(); // throw away attribute for now
        let mut len = [0;4];                     // extract vector length
        self.stream().read(&mut len).unwrap();
        let len = u32::from_le_bytes(len);
        if msg_type == 0 {
            self.read_generic_list(len)
        } else {
            self.read_uniform_list(msg_type, len)
        }
    }

    fn read_data(&mut self, msg_type: i8) -> KObj {
        let mut kobj = KObj::new(msg_type);
        kobj = match kobj {
            KObj::Atom(k) => self.read_atom(k),
            KObj::List(_) => self.read_list(msg_type)
        };
        kobj
    }

    pub fn send_sync(&mut self, data: &KObj) -> Result<KObj, Error> {
        if self.stream.is_none() {
            self.open()?;
        };
        let header_bytes = [1, 1, 0, 0].iter().cloned();
        let mut data_bytes = data.serialize().clone();
        let type_bytes = [data.type_as_bytes()];
        let mut size_bytes = vec![];
        size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
        data_bytes.splice(0..0, type_bytes.iter().cloned());
        data_bytes.splice(0..0, size_bytes.iter().cloned());
        data_bytes.splice(0..0, header_bytes);
        self.stream().write(&data_bytes).unwrap();
        self.stream().flush().unwrap();    
        let response = self.read();
        Ok(response)
    }

    pub fn send_response(&mut self, data: &KObj) -> Result<(), Error> {
        if self.stream.is_none() {
            self.open()?;
        };
        let header_bytes = [1, 2, 0, 0].iter().cloned();
        let mut data_bytes = data.serialize().clone();
        let type_bytes = [data.type_as_bytes()];
        let mut size_bytes = vec![];
        size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
        data_bytes.splice(0..0, type_bytes.iter().cloned());
        data_bytes.splice(0..0, size_bytes.iter().cloned());
        data_bytes.splice(0..0, header_bytes);
        self.stream().write(&data_bytes).unwrap();
        self.stream().flush().unwrap();    
        Ok(())
    }
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
}

#[derive(Debug)]
pub enum KObj {
    Atom(KType),
    List(Vec<KObj>)
}

impl KType {
    fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];
        match self  {
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
        }
    }

    fn deserialize(&self, data: &Vec<u8>) -> KType {
        match self {
            KType::Boolean(_)   => KType::Boolean(data[0] == 1),
            KType::Guid(_)      => KType::Guid(Uuid::from_slice(data).unwrap()),
            KType::Byte(_)      => KType::Byte(data[0]),
            KType::Short(_)     => KType::Short(LittleEndian::read_i16(data)),
            KType::Int(_)       => KType::Int(LittleEndian::read_i32(data)),
            KType::Long(_)      => KType::Long(LittleEndian::read_i64(data)),
            KType::Real(_)      => KType::Real(LittleEndian::read_f32(data)),
            KType::Float(_)     => KType::Float(LittleEndian::read_f64(data)),
            KType::Char(_)      => KType::Char(data[0] as char),
            KType::Symbol(_)    => KType::Symbol(String::from_utf8(data.to_vec()).unwrap()),
            KType::Timestamp(_) => {
                let dt = LittleEndian::read_i64(data) + 946684800000000000;
                KType::Timestamp(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(dt / 1_000_000_000, (dt % 1_000_000_000) as u32), Utc))
            },
            KType::Month(_) => {
                let dt = LittleEndian::read_i32(data) * 30 + 730119;
                KType::Date(Date::<Utc>::from_utc(
                    NaiveDate::from_num_days_from_ce(dt), Utc))
            },
            KType::Date(_) => {
                let dt = LittleEndian::read_i32(data) + 730119;
                KType::Date(Date::<Utc>::from_utc(
                    NaiveDate::from_num_days_from_ce(dt), Utc))
            },
            KType::Datetime(_) => {
                let dt = (LittleEndian::read_f64(data) + 10_957.0) * 86_400.0 * 1_000_000_000.0;
                KType::Datetime(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp((dt / 1_000_000_000.0) as i64, dt as u32 % 1_000_000_000), Utc))
            },
            KType::Timespan(_) => {
                let dt = LittleEndian::read_i64(data);
                KType::Timespan(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(dt / 1_000_000_000, (dt % 1_000_000_000) as u32), Utc))
            },        
            KType::Minute(_) => {
                let m = LittleEndian::read_i32(data);
                KType::Minute(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp((m * 60) as i64, 0), Utc))
            },   
            KType::Second(_) => {
                let s = LittleEndian::read_i32(data);
                KType::Second(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp(s as i64, 0), Utc))
            },   
            KType::Time(_) => {
                let s = LittleEndian::read_i32(data);
                KType::Time(DateTime::<Utc>::from_utc(
                    NaiveDateTime::from_timestamp((s / 1000) as i64, 1_000_000*(s % 1_000) as u32), Utc))
            }
        }
    }

    fn type_as_code(&self) -> i8 {
        match self {
            KType::Boolean(_)   => -01,
            KType::Guid(_)      => -02,
            KType::Byte(_)      => -04,
            KType::Short(_)     => -05,
            KType::Int(_)       => -06,
            KType::Long(_)      => -07,
            KType::Real(_)      => -08,
            KType::Float(_)     => -09,
            KType::Char(_)      => -10,
            KType::Symbol(_)    => -11,
            KType::Timestamp(_) => -12,
            KType::Month(_)     => -13,
            KType::Date(_)      => -14,
            KType::Datetime(_)  => -15,
            KType::Timespan(_)  => -16,
            KType::Minute(_)    => -17,
            KType::Second(_)    => -18,
            KType::Time(_)      => -19,
            _ => 0
        }
    }
}


impl KObj {

    pub fn new(code: i8) -> KObj {
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
            _ => panic!["unrecognized type code"]
        }
    }

    fn serialize(&self) -> Vec<u8> {
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
            }
        }
    }

    fn type_as_bytes(&self) -> u8 {
        let code = match self {
            KObj::Atom(t) => t.type_as_code() as u8,
            // todo: support generic lists
            // assumes all lists are of the type of the first element
            KObj::List(t) => {
                match &t[0] {
                    KObj::Atom(t) => (-1 * t.type_as_code()) as u8,
                    KObj::List(_) => 0 as u8,
                }
            }
        };
        code as u8
    }

    fn deserialize(&self, data: &Vec<u8>) -> KObj{
        match self {
            KObj::Atom(t) => KObj::Atom(t.deserialize(data)),
            KObj::List(_) => KObj::List(vec![]),  // this will never get used
        }
    }
}