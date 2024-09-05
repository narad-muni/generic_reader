use recorder::Reader;

fn main() {
    // Assign reader adapters here
    let reader = Reader::new("/home/appadmin/Work/generic_reader/settings.json".to_string());

    let (columns, data) = reader.read(Some(0), Some(5));

    println!("Columns: \n\t{:?}", columns);

    println!("Data: ");
    for i in data {
        println!("\t{i:?}");
    }
}
