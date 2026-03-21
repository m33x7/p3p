use std::thread;
use std::io;
use std::time::Duration;

mod discovery;
mod framing;

use crate::framing::{Framing};

fn main() -> std::io::Result<()> {
    let write_stream = discovery::establish_connection()?;
    let read_stream = write_stream.try_clone()?;

    let write_framing = framing::LengthPrefixFraming::new(write_stream)?;
    let read_framing = framing::LengthPrefixFraming::new(read_stream)?;
    
    let read_thread = thread::spawn(move || output_message(read_framing));
    let write_thread = thread::spawn(move || write_message(write_framing));

    read_thread.join().expect("reader thread panicked")?;
    write_thread.join().expect("writer thread panicked")?;
    
    Ok(())
}

fn write_message(mut f: framing::LengthPrefixFraming) -> io::Result<()>{
    let stdin = io::stdin();

    for line in stdin.lines() {
        let line = line?;
        f.write_msg(line.trim())?;
    }

    Ok(())
}

fn output_message(mut f: framing::LengthPrefixFraming) -> io::Result<()>{
    loop {
        match f.read_msg() {
            Ok(Some(msg)) => println!(">>> {}", msg.msg),
            Ok(None) => {}
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                println!("Connection closed by peer");
                break;
            }
            Err(e) => return Err(e),
        }
        
        thread::sleep(Duration::from_millis(250));
    }

    Ok(())
}
