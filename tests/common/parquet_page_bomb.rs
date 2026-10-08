//! Narrow hostile-page fixture used to exercise the real Parquet decoder boundary.

use std::{fs, fs::File, path::Path};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn read_compact_varint(bytes: &[u8], cursor: &mut usize) -> u64 {
    let mut value = 0_u64;
    let mut shift = 0;
    loop {
        let byte = *bytes.get(*cursor).expect("complete compact varint");
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
        assert!(shift < 64, "bounded compact varint");
    }
}

fn write_compact_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn zigzag_i32(value: i32) -> u64 {
    ((value as u32) << 1 ^ (value >> 31) as u32) as u64
}

fn compressed_page_with_room(
    bytes: &[u8],
    column: &parquet::file::metadata::ColumnChunkMetaData,
) -> Option<(usize, usize, usize, usize)> {
    let page_offset = usize::try_from(
        column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset()),
    )
    .ok()?;
    let mut cursor = page_offset;
    if bytes.get(cursor) != Some(&0x15) {
        return None;
    }
    cursor += 1;
    let _page_type = read_compact_varint(bytes, &mut cursor);
    if bytes.get(cursor) != Some(&0x15) {
        return None;
    }
    cursor += 1;
    let size_start = cursor;
    let _uncompressed_size = read_compact_varint(bytes, &mut cursor);
    let size_end = cursor;
    if bytes.get(cursor) != Some(&0x15) {
        return None;
    }
    cursor += 1;
    let compressed_size =
        usize::try_from(read_compact_varint(bytes, &mut cursor).div_ceil(2)).ok()?;
    if compressed_size <= 16 {
        return None;
    }
    let body_start = bytes[page_offset..]
        .windows(4)
        .position(|window| window == [0x28, 0xb5, 0x2f, 0xfd])?
        .checked_add(page_offset)?;
    let body_end = body_start.checked_add(compressed_size)?;
    (body_start > cursor && body_end <= bytes.len())
        .then_some((size_start, size_end, body_start, body_end))
}

pub(crate) fn mutate_dictionary_page_to_memory_bomb(path: &Path) {
    const BOMB_UNCOMPRESSED_BYTES: i32 = 1_500_000_000;

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let original = fs::read(path).unwrap();
    let (size_start, size_end, body_start, body_end) = builder.metadata().row_groups()[0]
        .columns()
        .iter()
        .find_map(|column| compressed_page_with_room(&original, column))
        .expect("Zstandard page body follows its header");
    let mut bomb_size = Vec::new();
    write_compact_varint(zigzag_i32(BOMB_UNCOMPRESSED_BYTES), &mut bomb_size);
    let added_header_bytes = bomb_size.len() - (size_end - size_start);
    assert!(added_header_bytes > 0);
    assert!(body_end - body_start > added_header_bytes);

    let mut mutated = Vec::with_capacity(original.len());
    mutated.extend_from_slice(&original[..size_start]);
    mutated.extend_from_slice(&bomb_size);
    mutated.extend_from_slice(&original[size_end..body_start]);
    mutated.extend_from_slice(&original[body_start..body_end - added_header_bytes]);
    mutated.extend_from_slice(&original[body_end..]);
    assert_eq!(mutated.len(), original.len());
    fs::write(path, mutated).unwrap();

    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
}
