use std::{fs::File, io::Write, time::Instant};

fn bool_to_byte(b: bool) -> [u8; 1] {
    if b {
        [1]
    } else {
        [0]
    }
}

fn main() {
    let mut native_writer = File::create("native2.bin").unwrap();

    let id: i32 = 1;
    let name: &str = "Saumil";
    let active: bool = true;
    let points: f64 = 2.5;

    // println!("{:?}", &id.to_be_bytes());
    // println!("{:?}", name.as_bytes());
    // println!("{:?}", &bool_to_byte(active));
    // println!("{:?}", &points.to_be_bytes());

    let start = Instant::now();

    for _ in 0..1000000 {
        native_writer.write_all(&id.to_be_bytes()).unwrap();
        native_writer.write_all(name.as_bytes()).unwrap();
        native_writer.write_all(&bool_to_byte(active)).unwrap();
        native_writer.write_all(&points.to_be_bytes()).unwrap();
    }

    println!("{:?} per iter", start.elapsed() / 1000000);

    let id: i32 = 2;
    let name: &str = "Ramesh";
    let active: bool = false;
    let points: f64 = 1.6;

    native_writer.write_all(&id.to_be_bytes()).unwrap();
    native_writer.write_all(name.as_bytes()).unwrap();
    native_writer.write_all(&bool_to_byte(active)).unwrap();
    native_writer.write_all(&points.to_be_bytes()).unwrap();
}
