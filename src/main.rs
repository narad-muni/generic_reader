use std::time::Instant;

use reader::Reader;

fn main() {
    // Assign reader adapters here
    let reader = Reader::new(
        "/home/appadmin/Work/generic_reader/config.json".to_string(),
        "native2.bin".to_string(),
        reader::Type::Native,
    )
    .unwrap();

    let start = Instant::now();

    let (columns, data) = reader.read(None, None).unwrap();

    println!("{:?} for {:?} values\n {:?} per iter", start.elapsed(), data.len(), start.elapsed()/data.len() as u32);

    println!("Columns: \n\t{:?}", columns);

    println!("Data: ");
    for i in data {
        println!("\t{i:?}");
    }
}
