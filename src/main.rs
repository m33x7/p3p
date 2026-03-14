use std::thread;
use std::io;

mod discovery;
mod framing;

fn main() -> std::io::Result<()> {
    let write_stream = discovery::establish_connection()?;
    let read_stream = write_stream.try_clone()?;

    let write_framing = framing::Framing::new(write_stream);
    let read_framing = framing::Framing::new(read_stream);
    
    let read_thread = thread::spawn(move || output_message(read_framing));
    let write_thread = thread::spawn(move || write_message(write_framing));

    read_thread.join().expect("reader thread panicked")?;
    write_thread.join().expect("writer thread panicked")?;
    
    Ok(())
}

fn write_message(mut f: framing::Framing) -> io::Result<()>{
    let stdin = io::stdin();

    for line in stdin.lines() {
        let line = line?;
        f.write_msg(line.trim())?;
    }

    Ok(())
}

fn output_message(f: framing::Framing) -> io::Result<()>{
    for msg in f {
        println!(">>> {}", msg.msg);
    }
    Ok(())
}
