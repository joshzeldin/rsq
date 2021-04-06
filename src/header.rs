use super::kdb::Kdb;
use std::io::{Read, Write};

pub struct Header {
    pub endian: Endian,
    pub protocol: u8,
    pub length: u32,
}

pub enum Endian {
    Big,
    Little
}

impl Header {
    pub fn read<R:Read, W:Write>(kdb: &mut Kdb<R,W>) -> Header {

        let stream = kdb.reader();
        let mut endian = [0;1];
        let mut protocol = [0;1]; 
        let mut msg_length = [0;4];

        stream.read_exact(&mut endian).unwrap();
        stream.read_exact(&mut protocol).unwrap();
        // throw away two padding bytes
        stream.read_exact(&mut [0;2]).unwrap();
        stream.read_exact(&mut msg_length).unwrap();

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