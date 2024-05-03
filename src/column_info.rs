use polars::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read};

pub fn print_column_info(subset_files: &[&str]) -> Result<(), Box<dyn Error>> {
    for subset_file in subset_files {
        let file = File::open(subset_file)?;
        let mut reader = BufReader::new(file);
        let mut content = String::new();
        reader.read_to_string(&mut content)?;

        let lines: Vec<&str> = content.lines().collect();

        if let Some(header) = lines.first() {
            let columns: Vec<String> = header.split(',').map(|col| col.to_string()).collect();
            println!("Column information for {}:", subset_file);

            for (col_index, column) in columns.iter().enumerate() {
                println!("Column: {}", column);
                let mut data_type = "Unknown".to_string();
                let mut has_null = false;

                for line in lines.iter().skip(1) {
                    let fields: Vec<&str> = line.split(',').collect();

                    if let Some(value) = fields.get(col_index) {
                        if value.trim().is_empty() {
                            has_null = true;
                        }

                        if let Ok(_) = value.parse::<i64>() {
                            data_type = "Integer".to_string();
                        } else if let Ok(_) = value.parse::<f64>() {
                            data_type = "Float".to_string();
                        } else {
                            data_type = "String".to_string();
                        }
                    }
                }

                println!("Data Type: {}", data_type);
                println!("Contains Null: {}", has_null);
                println!();
            }
        }
    }

    Ok(())
}

pub fn convert_columns_to_float(
    file_path: &str,
    column_names: &[&str],
) -> Result<DataFrame, Box<dyn Error>> {
    let mut df = CsvReader::from_path(file_path)?
        .infer_schema(None)
        .has_header(true)
        .finish()?;

    for column_name in column_names {
        let column = df.column(column_name)
            .map_err(|e| format!("Failed to find column '{}': {}", column_name, e))?
            .cast(&DataType::Float64)
            .map_err(|e| format!("Failed to cast column '{}' to Float64: {}", column_name, e))?;

        // Use `with_column` and correctly handle the returned mutable reference to DataFrame
        df = df.with_column(column)
            .map_err(|e| format!("Failed to replace column '{}': {}", column_name, e))?
            .clone(); // Cloning the DataFrame to match the expected return type
    }

    Ok(df)
}