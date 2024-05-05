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

