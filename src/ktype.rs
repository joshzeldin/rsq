use std::fmt;
use uuid::Uuid;
use chrono::{Date, DateTime, Utc, NaiveDateTime, NaiveDate, Datelike, Timelike};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

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
    String(String),
    Symbol(String),
    Timestamp(DateTime<Utc>),
    Month(Date<Utc>),
    Date(Date<Utc>),
    Datetime(DateTime<Utc>),
    Timespan(DateTime<Utc>),
    Minute(DateTime<Utc>),
    Second(DateTime<Utc>),
    Time(DateTime<Utc>),
    Unary(u8),
    Operator(u8),
}

impl fmt::Display for KType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KType::Boolean(k)   => {write!(f, "{}b", if *k {1} else {0})},
            KType::Guid(k)      => write!(f, "\"G\"$\"{}\"",k),
            KType::Byte(k)      => write!(f, "{}",k),
            KType::Short(k)     => write!(f, "{}h",k),
            KType::Int(k)       => write!(f, "{}i",k),
            KType::Long(k)      => write!(f, "{}j",k),
            KType::Real(k)      => write!(f, "{}e",k),
            KType::Float(k)     => write!(f, "{}f",k),
            KType::Char(k)      => write!(f, "\"{}\"",k),
            KType::String(k)    => write!(f, "\"{}\"",k),
            KType::Symbol(k)    => write!(f, "`{}",k),
            KType::Timestamp(k) => write!(f, "{}", k.format("%Y.%m.%dD%H:%M:%S.%f")),
            KType::Month(k)     => write!(f, "{}", k.format("%Y.%mm")),
            KType::Date(k)      => write!(f, "{}", k.format("%Y.%m.%d")),
            KType::Datetime(k)  => write!(f, "{}", k.format("%Y.%m.%dT%H:%M:%S%.3f")),
            //todo: fix this to handle date offsets
            KType::Timespan(k)  => write!(f, "{}", k.format("0D%H:%M:%S.%f")),
            KType::Minute(k)    => write!(f, "{}", k.format("%H:%M")),
            KType::Second(k)    => write!(f, "{}", k.format("%H:%M:%S")),
            KType::Time(k)      => write!(f, "{}", k.format("%H:%M:%S%.3f")),
            KType::Unary(k)     => write!(f, "{}", KType::unary_mapping(k)),
            KType::Operator(k)  => write!(f, "{}", KType::operator_mapping(k)),
        }
    }
}

impl KType {
    fn unary_mapping(code: &u8) -> String {
        let unary = match code {
            0 => "::",
            1 => "+:",
            2 => "-:",
            3 => "*:",
            4 => "%:",
            5 => "&:",
            6 => "|:",
            7 => "^:",
            8 => "=:",
            9 => "<:",
           10 => ">:",
           11 => "$:",
           12 => ",:",
           13 => "#:",
           14 => "_:",
           15 => "~:",
           16 => "!:",
           17 => "?:",
           18 => "@:",
           19 => ".:",
           20 => "0::",
           21 => "1::",
           22 => "2::",
           23 => "avg",
           24 => "last",
           25 => "sum",
           26 => "prd",
           27 => "min",
           28 => "max",
           29 => "exit",
           30 => "getenv",
           31 => "abs",
           32 => "sqrt",
           33 => "log",
           34 => "exp",
           35 => "sin",
           36 => "asin",
           37 => "cos",
           38 => "acos",
           39 => "tan",
           40 => "atan",
           41 => "enlist",
           _  =>  "",
       };
       String::from(unary)
    }

    fn operator_mapping(code: &u8) -> String {
        let op = match code {
            0 =>  ":",
            1 =>  "+",
            2 =>  "-",
            3 =>  "*",
            4 =>  "%",
            5 =>  "&",
            6 =>  "|",
            7 =>  "^",
            8 =>  "=",
            9 =>  "<",
            10 => ">",
            11 => "$",
            12 => ",",
            13 => "#",
            14 => "_",
            15 => "~",
            16 => "!",
            17 => "?",
            18 => "@",
            19 => ".",
            20 => "0:",
            21 => "1:",
            22 => "2:",
            23 => "in",
            24 => "within",
            25 => "like",
            26 => "bin",
            27 => "ss",
            28 => "insert",
            29 => "wsum",
            30 => "wavg",
            31 => "div",
            32 => "xexp",
            33 => "setenv",
            34 => "binr",
            35 => "cov",
            36 => "cor",
           _  =>  "",
       };
       String::from(op)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];
        match self  {
            KType::Boolean(n)   => vec![*n as u8],
            KType::Guid(n)      => n.as_bytes().iter().cloned().collect(),
            KType::Byte(n)      => vec![*n as u8],
            KType::Short(n)     => {buf.write_i16::<LittleEndian>(*n).unwrap(); buf},
            KType::Int(n)       => {buf.write_i32::<LittleEndian>(*n).unwrap(); buf},
            KType::Long(n)      => {buf.write_i64::<LittleEndian>(*n).unwrap(); buf},
            KType::Real(n)      => {buf.write_f32::<LittleEndian>(*n).unwrap(); buf},
            KType::Float(n)     => {buf.write_f64::<LittleEndian>(*n).unwrap(); buf},
            KType::Char(n)      => vec![*n as u8],
            KType::String(n)    => {
                let mut string = Vec::<u8>::with_capacity(5 + n.len());
                string.push(0);
                buf.write_i32::<LittleEndian>(n.len() as i32).unwrap();
                string.append(&mut buf);
                string.append(&mut Vec::from(n.as_bytes()));
                string
            },
            KType::Symbol(n)    => {let mut sym = Vec::from(n.as_bytes());sym.push(0);sym},
            KType::Timestamp(n) => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 946684800000000000).unwrap(); buf},
            KType::Month(n)     => {buf.write_i32::<LittleEndian>(n.num_days_from_ce() - 730119).unwrap(); buf},
            KType::Date(n)      => {buf.write_i32::<LittleEndian>(n.num_days_from_ce() - 730119).unwrap(); buf},
            KType::Datetime(n)  => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 946684800000000000).unwrap(); buf},
            KType::Timespan(n)  => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 946684800000000000).unwrap(); buf},
            KType::Minute(n)    => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 946684800000000000).unwrap(); buf},
            KType::Second(n)    => {buf.write_i64::<LittleEndian>(n.timestamp_nanos() - 946684800000000000).unwrap(); buf},
            KType::Time(n)      => {buf.write_i32::<LittleEndian>((n.time().num_seconds_from_midnight() * 1000 + n.time().nanosecond() / 1_000_000) as i32).unwrap();buf},
            KType::Unary(n)     => vec![*n as u8],
            KType::Operator(n)  => vec![*n as u8],
        }
    }

    pub fn deserialize(&self, data: &Vec<u8>) -> KType {
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
            KType::String(_)    => KType::String(String::from_utf8(data.to_vec()).unwrap()),
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
            },
            KType::Unary(_)      => KType::Unary(data[0]),
            KType::Operator(_)      => KType::Operator(data[0]),
        }
    }

    pub fn type_as_code(&self) -> i8 {
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
            KType::String(_)    =>  10,
            KType::Symbol(_)    => -11,
            KType::Timestamp(_) => -12,
            KType::Month(_)     => -13,
            KType::Date(_)      => -14,
            KType::Datetime(_)  => -15,
            KType::Timespan(_)  => -16,
            KType::Minute(_)    => -17,
            KType::Second(_)    => -18,
            KType::Time(_)      => -19,
            KType::Unary(_)     => 101,
            KType::Operator(_)  => 102,
        }
    }
}