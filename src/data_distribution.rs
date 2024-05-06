
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

    let specific_columns = [
        "GameID", "Event", "White", "WhiteElo", "WhiteRatingDiff",
        "White_tosViolation", "White_playTime_total", "White_count_all",
        "Black", "BlackElo", "BlackRatingDiff", "Black_tosViolation",
        "Black_playTime_total", "Black_count_all", "Moves", "TotalMoves",
        "ECO", "Opening", "TimeControl", "Result"
    ];

    let headers: Vec<&str> = header.split(',').collect();
    let column_indices: Vec<usize> = headers
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| specific_columns.contains(&col).then(|| idx))
        .collect();

    let selected_headers: String = column_indices
        .iter()
        .map(|&idx| headers[idx])
        .collect::<Vec<&str>>()
        .join(",");

    let mut writers: Vec<BufWriter<File>> = output_files
        .iter()
        .map(|&file| File::create(file).map(BufWriter::new).map_err(Into::into))
        .collect::<Result<_, Box<dyn Error>>>()?;

    for writer in &mut writers {
        writeln!(writer, "{}", selected_headers)?;
    }

    let num_output_files = writers.len();
    let num_rows_per_file = combined_data.len() / num_output_files;
    let remaining_rows = combined_data.len() % num_output_files;

    let mut row_index = 0;
    for (file_index, writer) in writers.iter_mut().enumerate() {
        let rows_to_write = num_rows_per_file + if file_index < remaining_rows { 1 } else { 0 };
        for _ in 0..rows_to_write {
            if let Some(line) = combined_data.get(row_index) {
                let row_data: Vec<&str> = line.split(',').collect();
                let selected_row_data: String = column_indices
                    .iter()
                    .map(|&idx| row_data[idx])
                    .collect::<Vec<&str>>()
                    .join(",");
                writeln!(writer, "{}", selected_row_data)?;
            }
            row_index += 1;
        }
        writer.flush()?;
    }

    println!("Data writing complete. {} rows distributed.", row_index);
    Ok(())
}
