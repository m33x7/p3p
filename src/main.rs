mod discovery;
mod framing;

fn main() -> std::io::Result<()> {
    let stream = discovery::establish_connection()?;
    let framing = framing::Framing::new(stream);

    println!("Connected");
    Ok(())
}
