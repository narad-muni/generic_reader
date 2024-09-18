use std::{time::Instant, u64};

use reader::Reader;

fn main() {
    // Assign reader adapters here
    let reader = Reader::new(
        "/home/appadmin/Work/generic_reader/config_fao.json".to_string(),
        "out3_fao.txt".to_string(),
        reader::Type::MultiNative,
    )
    .unwrap();

    let start = Instant::now();

let data = reader.read(None, None).unwrap();

    println!(
        "{:?} for {:?} values\n {:?} per iter",
        start.elapsed(),
        data.len(),
        start.elapsed() / data.len().max(1) as u32
    );

    if data.len() > 0 {
        println!(
            "Columns: \n\t{:?}",
            data[0].keys().collect::<Vec<&String>>()
        );
    }

    println!("Data: ");
    for i in data {
        for (k, v) in i {
            // println!("\t{k}: {v}");
        }
    }
}
