use std::net::{ TcpStream};
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::io::{Error, Write};
use byteorder::{LittleEndian, WriteBytesExt};
use crate::{KObj};
use super::header::Header;
use super::ktype::KType;

pub struct Kdb {
    host: String,
    port: u16,
    user: String,
    pass: String,
    reader: Option<BufReader<TcpStream>>,
    writer: Option<BufWriter<TcpStream>>
}

impl Kdb {

    pub fn new(host: &str, port: u16, user: &str, pass: &str) -> Kdb {
        Kdb {
            host: host.to_string(),
            port,
            user: user.to_string(),
            pass: pass.to_string(),
            reader: None,
            writer: None
        }
    }

    pub fn open(&mut self) -> Result<(),Error> {
        let mut stream = TcpStream::connect(format!("{}:{}",self.host,self.port))?;
        let response = format!("{}:{}{}",self.user, self.pass, "\x06\x00");
        stream.write(response.as_bytes())?;
        stream.read(&mut [0; 1])?;
        self.reader = Some(BufReader::new(stream.try_clone()?));
        self.writer = Some(BufWriter::new(stream));
        Ok(())
    }

    pub fn reader(&mut self) -> &mut BufReader<TcpStream> {
        self.reader.as_mut().unwrap()
    }

    pub fn writer(&mut self) -> &mut BufWriter<TcpStream> {
        self.writer.as_mut().unwrap()
    }

    pub fn close(&mut self) -> Result<(), Error> {
        self.reader = None;
        self.writer = None;
        Ok(())
    }

    pub fn send_async(&mut self, data: &KObj) -> Result<(), Error> {
        if self.writer.is_none() {
            self.open()?;
        };
        let header_bytes = vec![1, 0, 0, 0];
        let mut data_bytes = data.serialize();
        let type_bytes = vec![data.type_as_bytes()];
        let mut size_bytes = vec![];
        size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32)?;
        data_bytes.splice(0..0, type_bytes);
        data_bytes.splice(0..0, size_bytes);
        data_bytes.splice(0..0, header_bytes);
        let writer = self.writer();
        writer.write(&data_bytes)?;
        Ok(())
    }

    pub fn read(&mut self) -> KObj {
        if self.reader.is_none() {
            match self.open() {
                Ok(_) => {},
                Err(e) => {return KObj::Error(format!("{}",e))}
            };
        };
        let msg_header = Header::read(self);
        let reader = self.reader();


        let mut msg_type = [0;1];
        reader.read(&mut msg_type).unwrap();
        let data = self.read_data(i8::from_le_bytes(msg_type));

        if msg_header.protocol == 1 {
            self.send_response(&KObj::Atom(KType::Boolean(true))).unwrap();
        };

        data

    }

    fn extract_atom(&mut self, len: usize) -> Vec<u8> {
        let mut vec = vec![0;len];
        self.reader().read(&mut vec).unwrap();
        vec
    }

    fn extract_string(&mut self) -> Vec<u8> {
        let stream = self.reader();
        stream.read(&mut [0;1]).unwrap(); // discard attribute

        let mut len = [0;4];
        stream.read(&mut len).unwrap();
        let len = u32::from_le_bytes(len) as usize;

        let mut string = Vec::with_capacity(len);
        stream.read(&mut string).unwrap();
        string
    }

    fn extract_sym(&mut self) -> Vec<u8> {
        let stream = self.reader();
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
            KType::String(_)    => self.extract_string(),
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
            self.reader().read(&mut msg_type).unwrap();
            let msg_code = i8::from_le_bytes(msg_type);
            list.push(self.read_data(msg_code));
        };  
        KObj::List(list)
    }  

    fn read_list(&mut self, msg_type: i8) -> KObj {
        let mut attr = [0;1];
        self.reader().read(&mut attr).unwrap(); // throw away attribute for now
        let mut len = [0;4];                     // extract vector length
        self.reader().read(&mut len).unwrap();
        let len = u32::from_le_bytes(len);
        if msg_type == 0 {
            self.read_generic_list(len)
        } else {
            self.read_uniform_list(msg_type, len)
        }
    }

    fn read_dict(&mut self) -> KObj {

        let mut key_type = [0;1];
        self.reader().read(&mut key_type).unwrap();
        let key_type = i8::from_le_bytes(key_type);

        let keys = self.read_data(key_type);

        let mut val_type = [0;1];
        self.reader().read(&mut val_type).unwrap();
        let val_type = i8::from_le_bytes(val_type);
        let vals = self.read_data(val_type);

        let keys: Vec<KObj> = match keys {
             KObj::List(k) => k,
            _ => return KObj::Error("keys of dictionary must be a list".to_string()) // this shouldn't happen
        };

        let vals = match vals {
            KObj::List(k) => k,
           _ => return KObj::Error("keys of dictionary must be a list".to_string()) // this shouldn't happen
        };
        
        KObj::Dict(keys, vals)
        
    }

    fn read_table(&mut self) -> KObj {

        let mut key_type = [0;1];
        self.reader().read(&mut key_type).unwrap();
        let key_type = i8::from_le_bytes(key_type);

        let keys = self.read_data(key_type);

        let mut val_type = [0;1];
        self.reader().read(&mut val_type).unwrap();
        let val_type = i8::from_le_bytes(val_type);
        let vals = self.read_data(val_type);

        let keys: Vec<KObj> = match keys {
             KObj::List(k) => k,
            _ => return KObj::Error("keys of dictionary must be a list".to_string()) // this shouldn't happen
        };

        let vals = match vals {
            KObj::List(k) => k,
           _ => return KObj::Error("keys of dictionary must be a list".to_string()) // this shouldn't happen
        };
        
        KObj::Table(keys, vals)
     
    }

    fn read_error(&mut self) -> KObj {
        let error_msg = self.extract_sym();
        KObj::Error(String::from_utf8(error_msg.to_vec()).unwrap())
    }

    fn read_data(&mut self, msg_type: i8) -> KObj {
        let mut kobj = KObj::new(msg_type);
        kobj = match kobj {
            KObj::Atom(k) => self.read_atom(k),
            KObj::List(_) => self.read_list(msg_type),
            KObj::GenericList(_) => self.read_list(msg_type),
            KObj::Dict(_,_) => self.read_dict(),
            KObj::Table(_,_) => {
                self.reader().read(&mut[0;2]).unwrap();
                self.read_table()
            },
            KObj::Error(_) => {
                self.read_error()
            }
        };
        kobj
    }

    pub fn send_sync(&mut self, data: &KObj) -> Result<KObj, Error> {
        if self.writer.is_none() {
            self.open()?;
        };
        let header_bytes = vec![1, 1, 0, 0];
        let mut data_bytes = data.serialize();
        let type_bytes = vec![data.type_as_bytes()];
        let mut size_bytes = vec![];
        size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
        data_bytes.splice(0..0, type_bytes);
        data_bytes.splice(0..0, size_bytes);
        data_bytes.splice(0..0, header_bytes);
        self.writer().write(&data_bytes).unwrap();
        self.writer().flush().unwrap();    
        let response = self.read();
        Ok(response)
    }

    pub fn send_response(&mut self, data: &KObj) -> Result<(), Error> {
        if self.writer.is_none() {
            self.open()?;
        };
        let header_bytes = vec![1, 2, 0, 0];
        let mut data_bytes = data.serialize();
        let type_bytes = vec![data.type_as_bytes()];
        let mut size_bytes = vec![];
        size_bytes.write_i32::<LittleEndian>((4 + header_bytes.len() + data_bytes.len() + type_bytes.len()) as i32).unwrap();
        data_bytes.splice(0..0, type_bytes);
        data_bytes.splice(0..0, size_bytes);
        data_bytes.splice(0..0, header_bytes);
        self.writer().write(&data_bytes).unwrap();
        self.writer().flush().unwrap();    
        Ok(())
    }
}
