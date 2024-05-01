use polars::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};

pub fn distribute_data(
    combined_data: &[String],
    header: &str,
    output_files: &[&str],
) -> Result<(), Box<dyn Error>> {
    println!("Total combined data rows: {}", combined_data.len());
    if combined_data.is_empty() {
        println!("No data to write. Exiting.");
        return Ok(());
    }

    let mut writers: Vec<BufWriter<File>> = output_files
        .iter()
        .map(|file| File::create(file).map(BufWriter::new))
        .collect::<Result<_, std::io::Error>>()?;

    for writer in &mut writers {
        writeln!(writer, "{}", header)?;
    }

    let num_output_files = writers.len();
    let num_rows_per_file = combined_data.len() / num_output_files;
    let mut remaining_rows = combined_data.len() % num_output_files;
    let mut row_index = 0;

    for writer in &mut writers {
        let rows_to_write = num_rows_per_file + if remaining_rows > 0 { 1 } else { 0 };
        if remaining_rows > 0 {
            remaining_rows -= 1;
        }

        for _ in 0..rows_to_write {
            if row_index < combined_data.len() {
                writeln!(writer, "{}", combined_data[row_index])?;
                row_index += 1;
            }
        }
        writer.flush()?;
    }

    println!("Data writing complete. {} rows distributed.", row_index);
    Ok(())
}

pub fn combine_dataframes(dataframes: Vec<DataFrame>) -> Result<DataFrame, Box<dyn Error>> {
    let mut combined_df = dataframes[0].clone();
    for df in dataframes.into_iter().skip(1) {
        combined_df = combined_df.vstack(&df)?;
    }

    Ok(combined_df)
}