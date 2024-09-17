use std::error::Error;

use serde_json::Value;

use crate::{BufferValue, DType};

pub fn cast_bytes(buf: &[u8], dtype: &DType) -> Result<Value, Box<dyn Error>> {
    Ok(match dtype {
        DType::Byte => Value::Number(serde_json::Number::from(u8::from_be_bytes(
            buf.to_vec()
                .try_into()
                .map_err(|_| "Failed to convert to u32")?,
        ))),
        DType::Char => Value::String(String::from_utf8_lossy(buf).to_string()),
        DType::Bool => Value::Bool(buf[0] != 0),
        DType::UInt => Value::Number(serde_json::Number::from(u32::from_be_bytes(
            buf.to_vec()
                .try_into()
                .map_err(|_| "Failed to convert to u32")?,
        ))),
        DType::Short => Value::Number(serde_json::Number::from(i16::from_be_bytes(
            buf.try_into()?,
        ))),
        DType::SInt => Value::Number(serde_json::Number::from(i32::from_be_bytes(
            buf.try_into()?,
        ))),
        DType::Float => Value::Number(
            serde_json::Number::from_f64(f64::from_be_bytes(buf.try_into()?)).ok_or("NaN")?,
        ),
        DType::None => Value::Null,
    })
}

pub fn col_from_buf(
    column: &BufferValue,
    buf: &[u8],
    offset: &mut usize,
    bit_offset: &mut usize,
) -> Result<Value, Box<dyn Error>> {

    println!("{} {}", column.name, offset);

    // Calculation for padding
    if column.length % 2 == 0 && *offset % 2 == 1 {
        // println!("Adding offset at {} {}",column.name, offset);
        *offset += 1;
    }

    if column.offset.is_some() {
        *offset = column.offset.unwrap();
    }

    let slice = &buf[*offset..(*offset + column.length)];

    *offset += column.length;

    cast_bytes(slice, &column.dtype)
}

pub fn col_from_buf_no_padding(
    column: &BufferValue,
    buf: &[u8],
    offset: &mut usize
) -> Result<Value, Box<dyn Error>> {
    if column.offset.is_some() {
        *offset = column.offset.unwrap();
    }

    let slice = &buf[*offset..(*offset + column.length)];

    *offset += column.length;

    cast_bytes(slice, &column.dtype)
}

pub fn get_buffer_slice<'a>(
    buf: &'a [u8],
    length: usize,
    offset: &mut usize,
) -> &'a [u8] {
    let slice = &buf[*offset..*offset + length];

    *offset += length;

    slice
}