use crate::BufferValue;

pub fn get_len_from_columns(columns: Vec<&BufferValue>) -> usize {
    let mut columns = columns.clone();

    columns.sort_by_key(|e| e.offset.unwrap_or(0));

    let last = columns.last().unwrap();

    last.offset.unwrap_or(0) + last.length
}
