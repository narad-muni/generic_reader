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
        DType::Bit => {
            let byte = bits_to_u8(buf.try_into()?);

            Value::Number(serde_json::Number::from(byte))
        }
    })
}

pub fn get_bit_slice(byte: u8, bit_offset: usize, length: usize, out_buf: &mut [u8]) {
    for i in 0..length {
        out_buf[i] = (byte >> (7 - bit_offset - i)) & 0x01;
    }
}

pub fn bits_to_u8(bits: [u8; 8]) -> u8 {
    let byte = bits
        .iter()
        .enumerate()
        .fold(0, |acc, (i, &bit)| acc | (bit << (7 - i)));

    byte
}

pub fn col_from_buf(
    column: &BufferValue,
    buf: &[u8],
    offset: &mut usize,
    bit_offset: &mut usize,
    packing: usize,
) -> Result<Value, Box<dyn Error>> {
    // println!("{} {}", column.name, offset);

    // If byte sized column and bit_offset is non zero, increase offset and reset bit_offset
    if column.dtype != DType::Bit && *bit_offset != 0 {
        *bit_offset = 0;
        *offset += 1;
    }

    // Calculation for padding
    // Ignore for bit columns
    if column.length % packing == 0 && *offset % packing != 0 && column.dtype != DType::Bit {
        // println!("Adding offset at {} {}",column.name, offset);
        *offset += *offset % packing;
    }

    if column.offset.is_some() {
        *offset = column.offset.unwrap();
    }

    let mut bit_slice = [0; 8];

    let slice = if column.dtype == DType::Bit {
        get_bit_slice(buf[*offset], *bit_offset, column.length, &mut bit_slice);

        &bit_slice
    } else {
        &buf[*offset..(*offset + column.length)]
    };

    // Increase offset depending on dtype
    if column.dtype == DType::Bit {
        *bit_offset += column.length;

        // Increase offset once bit offset is reached to 8
        if *bit_offset == 8 {
            *bit_offset = 0;
            *offset += 1;
        }
    } else {
        *offset += column.length;
    }

    cast_bytes(slice, &column.dtype)
}

pub fn get_buffer_slice<'a>(buf: &'a [u8], length: usize, offset: &mut usize) -> &'a [u8] {
    let slice = &buf[*offset..*offset + length];

    *offset += length;

    slice
}
