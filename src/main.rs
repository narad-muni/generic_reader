use recorder::Reader;

fn main() {
    // Assign reader adapters here
    let reader = Reader::new(
        "/home/appadmin/Work/generic_reader/config.json".to_string(),
        "native.bin".to_string(),
        recorder::Type::Native,
    );

    let (columns, data) = reader.read(None, None);

    println!("Columns: \n\t{:?}", columns);

    println!("Data: ");
    for i in data {
        println!("\t{i:?}");
    }
}
