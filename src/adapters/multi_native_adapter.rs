use std::{
    error::Error, fs::File, io::{BufReader, Read}, vec
};

use serde_json::{Map, Value};
use crate::{
    adapters::utils::{byte_utils::{col_from_buf, get_buffer_slice}, column_utils::get_len_from_columns}, BufferValue, CompressionType, Readable
};

#[derive(Debug)]
pub struct MultiNative {}

impl Readable for MultiNative {
    fn read(
        &self,
        file_path: &String,
        config: &crate::Config,
        from: Option<usize>,
        len: usize,
    ) -> Result<Vec<Map<String, Value>>, Box<dyn Error>> {
        let from = from.unwrap_or(0);

        // Create file reader and BufReader
        let file = File::open(file_path)?;
        let mut buf_reader = BufReader::new(file);

        let mut values = vec![];
        let mut pos = 0;

        // Get all headers
        let packet_header = &config.native.packet_header;
        let packet_info = &config.native.packet_info;

        let header_size = get_len_from_columns(vec![&packet_header.timestamp, &packet_header.packet_size]);

        // Loop for each buffer in file
        loop {
            // Init a buffer
            let mut buf: [u8; 1024] = [0; 1024];
            let mut offset = 0;

            // Check if header data is available in file or EOF
            if buf_reader.read_exact(&mut buf[0..header_size]).is_err() {
                // println!("EOF at {pos}");
                break;
            }

            // Get timestamp and packet size from header
            let timestamp = col_from_buf(&packet_header.timestamp, &buf, &mut offset)?;
            let packet_size = col_from_buf(&packet_header.packet_size, &buf, &mut offset)?;

            // println!("timestamp {}", timestamp);
            // println!("packet size {}", packet_size);

            // Read buffer
            buf = [0; 1024];
            buf_reader.read_exact(&mut buf[0..packet_size.as_u64().unwrap() as usize]).unwrap();
            let mut offset = 0;

            // After reading buffer
            // Skip packets before from
            if pos < from {
                pos += 1;
                continue;    
            }

            // get no of packets
            let no_of_packets = col_from_buf(&packet_info.no_of_packets, &buf, &mut offset)?;
            // println!("no_of_packets {}", no_of_packets);

            // Read buffer
            let buf = &buf[offset..];
            let mut base = 0;

            // For each packet inside udp packet
            for _ in 0..no_of_packets.as_u64().unwrap_or(1) {
                // load buffer from base
                let mut offset = 0;
                let mut buf = &buf[base..];
                let mut decompress_buf = [0; 2048];

                // Get compressed packet size
                let compressed_packet_size = col_from_buf(&packet_info.compressed_packet_size, &buf, &mut offset).unwrap();

                // println!("compressed_packet_size {}", compressed_packet_size);

                // Check if packet is compressed
                if compressed_packet_size.is_u64() && compressed_packet_size.as_u64().unwrap() > 0 {
                    let mut temp_offset = offset;
                    let compressed_buf = get_buffer_slice(&buf, compressed_packet_size.as_u64().unwrap() as usize, &mut temp_offset);

                    match packet_info.compresseion_type {
                        CompressionType::Lzo => mylzo::decompress(compressed_buf, &mut decompress_buf).unwrap(),
                    };                    

                    buf = &decompress_buf;
                }else{
                    buf = &buf[offset..];
                }

                offset = 0;

                // Packet size and identifier
                let packet_identifier = col_from_buf(&packet_info.packet_identifier, &buf, &mut offset).unwrap();
                let packet_size = col_from_buf(&packet_info.packet_size, &buf, &mut offset).unwrap();

                // println!("packet_size {}", packet_size);
                // println!("packet_identifier {}", packet_identifier);

                // Get column details or default
                let column_details = packet_info.column_details.get(&packet_identifier.as_u64().unwrap()).unwrap_or(
                    packet_info.column_details.get(&0).expect(format!("Unable to find columns for {}", packet_identifier).as_str()),
                );

                // Read values from packet buf
                let mut hashmap = Map::new();

                hashmap.insert("timestamp".to_string(), timestamp.clone());

                read_uncompressed(&column_details.columns, &buf, &mut offset, &mut hashmap);

                values.push(hashmap);

                // Calculate base for next packet
                if packet_size.is_i64() {
                    // If packet size is given in buffer
                    base += packet_size.as_i64().unwrap() as usize;
                }else if column_details.expected_size > 0 {
                    // If packet size is given in config
                    base += column_details.expected_size as usize;
                }else{
                    // Else start next packet from current packet end
                    base += offset;
                }
            }

            pos += 1;

            if pos >= (from + len) {
                // println!("END at {pos}");
                break;
            }
        }

        Ok(values)
    }
}

fn read_uncompressed(columns: &Vec<BufferValue>, buf: &[u8], total_offset: &mut usize, hashmap: &mut Map<String, Value>){
    // Offset to track position in buffer
    let mut offset = 0;

    for column in columns {
        // Auto increment offset
        // Auto type cast based on value
        let val = col_from_buf(column, &buf, &mut offset).unwrap();

        // Null is used for padding
        // We can skip adding these columns
        if val.is_null() {
            continue;
        }

        // Push value
        hashmap.insert(column.name.clone(), val);
    }

    // Increment total offset
    // Used for multiple packets in same buffer
    *total_offset += offset;
}