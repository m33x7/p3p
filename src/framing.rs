use std::io;
use std::io::Read;
use std::io::Write;
use std::net::{TcpStream};

pub struct Framing {
    stream: TcpStream
}

pub struct Message {
    pub msg: String
}

impl Framing {
    pub fn new(stream: TcpStream) -> Framing {
        let r = stream.set_nonblocking(false);
        match r {
            Ok(_) => { }
            Err(e) => eprintln!("Error setting framing to blocking : {e}")
        };
        Framing {stream}
    }

    pub fn write_msg(&mut self, msg: &str) -> io::Result<()> {
        let bytes = msg.as_bytes();

        if bytes.len() > u8::MAX as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "message too long"));
        }

        self.stream.write_all(&[bytes.len() as u8])?;
        self.stream.write_all(&bytes)?;
        Ok(())
    }
}

impl Iterator for Framing {
    // TODO - change item to be io::Result<Message>.
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        // Reading the length of message.
        let mut length_buffer = [0; 1];

        if self.stream.read_exact(&mut length_buffer).is_err() {
            return None;
        }

        let message_length = length_buffer[0] as usize;

        // Reading the message itself.
        let mut msg_buffer = vec![0u8; message_length];

        if self.stream.read_exact(&mut msg_buffer).is_err() {
            return None;
        }

        let s = match String::from_utf8(msg_buffer) {
            Ok(s) => s,
            Err(_) => return None
        };

        Some(Message { msg: s })
    }
}

