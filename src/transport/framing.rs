use std::io;
use std::io::Read;
use std::io::Write;
use std::net::{TcpStream};

pub struct LengthPrefixFraming {
    stream: TcpStream
}

pub trait Framing {
    fn write_msg(&mut self, msg: &str) -> io::Result<()>;

    fn read_msg(&mut self) -> io::Result<Option<String>>;
}

impl Framing for LengthPrefixFraming {
    fn write_msg(&mut self, msg: &str) -> io::Result<()> {
        let bytes = msg.as_bytes();

        if bytes.len() > u8::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "message too long"));
        }

        // TODO - maybe writing in one go is faster.
        self.stream.write_all(&[bytes.len() as u8])?;
        self.stream.write_all(&bytes)?;
        Ok(())
    }

    fn read_msg(&mut self) -> io::Result<Option<String>> {
        // Reading the length of message.
        let mut length_buffer = [0; 1];

        match self.stream.read_exact(&mut length_buffer) {
            Ok(_) => (),
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e),
        };

        let message_length = length_buffer[0] as usize;

        // Reading the message itself.
        let mut msg_buffer = vec![0u8; message_length as usize];

        match self.stream.read_exact(&mut msg_buffer) {
            Ok(_) => (),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e),
        };

        let s = match String::from_utf8(msg_buffer) {
            Ok(s) => s,
            Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Could not decode UTF-8 string."))
        };

        Ok(Some(s))
    }
}

impl LengthPrefixFraming {
    pub fn new(stream: TcpStream) -> io::Result<LengthPrefixFraming> {
        stream.set_nonblocking(true)?;
        Ok(LengthPrefixFraming { stream })
    }
}
