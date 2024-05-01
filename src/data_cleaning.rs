use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

pub fn clean_data(input_file: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    // Open the input file
    let file = File::open(input_file)?;
    let reader = BufReader::new(file);

    // Open the output file
    let output = File::create(output_file)?;
    let mut writer = BufWriter::new(output);

    // Iterate over each line in the input file
    for line in reader.lines() {
        let line = line?;

        // Split the line into fields
        let fields: Vec<&str> = line.split(',').collect();

        // Perform data cleaning tasks on the fields
        let cleaned_fields: Vec<String> = fields
            .into_iter()
            .map(|field| {
                // Remove leading/trailing whitespaces
                field.trim().to_string()
            })
            .collect();

        // Join the cleaned fields back into a comma-separated line
        let cleaned_line = cleaned_fields.join(",");

        // Write the cleaned line to the output file
        writeln!(writer, "{}", cleaned_line)?;
    }

    Ok(())
}