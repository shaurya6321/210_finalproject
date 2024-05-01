use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use polars::prelude::*;  // Ensures all necessary items from Polars are included.
use polars::frame::DataFrame;

mod data_distribution;
mod column_info;
mod analysis;

fn main() -> Result<(), Box<dyn Error>> {
    // Define paths for input and output files
    let input_files = ["games_metadata_profile_2024_01.csv", "games_metadata_profile.csv"];
    let output_files = [
        "subset_data_1.csv",
        "subset_data_2.csv",
        "subset_data_3.csv",
        "subset_data_4.csv",
        "subset_data_5.csv",
    ];

    // Print column information for input files
    column_info::print_column_info(&input_files)?;

    // Combine data from multiple CSV files, ensuring unique headers
    let (header, combined_data) = combine_csv_files(&input_files)?;

    // Distribute combined data into subsets for detailed analysis
    data_distribution::distribute_data(&combined_data, &header, &output_files)?;

    // Convert relevant columns in each subset to float for numerical analysis
    let dataframes = convert_columns_in_files(&output_files, &["WhiteElo", "BlackElo"])?;

    // Ensure combined_dataframe is declared mutable
    let mut combined_dataframe = data_distribution::combine_dataframes(dataframes)?;

    let output_file = "combined_data.csv";
    let file = File::create(output_file)?;
    let mut csv_writer = CsvWriter::new(file).has_header(true);

    // Write the combined_dataframe to the CSV file
    match csv_writer.finish(&mut combined_dataframe) {
        Ok(_) => println!("Combined data written to {}", output_file),
        Err(err) => eprintln!("Error writing combined data: {}", err),
    }

    // Analyze the combined game data for network and performance insights
    for subset_file in output_files.iter() {
        perform_game_data_analysis(subset_file)?;
    }

    Ok(())
}

/// Reads and combines CSV files while ensuring only one header is used.
fn combine_csv_files(files: &[&str]) -> Result<(String, Vec<String>), Box<dyn Error>> {
    let mut combined_data = Vec::new();
    let mut header = String::new();

    for &file in files {
        let file = File::open(file)?;
        let reader = BufReader::new(file);
        for (index, line) in reader.lines().enumerate() {
            let line = line?;
            if index == 0 {
                if header.is_empty() {
                    header = line;
                }
            } else {
                combined_data.push(line);
            }
        }
    }

    Ok((header, combined_data))
}

/// Converts specified columns to float in each CSV file.
fn convert_columns_in_files(files: &[&str], columns: &[&str]) -> Result<Vec<DataFrame>, Box<dyn Error>> {
    let mut dataframes = Vec::new();
    
    for &file in files {
        let mut df = column_info::convert_columns_to_float(file, columns)?;
        
        // Directly manipulate df which is mutable
        if let Some(event_column) = df.column("Event").ok() {
            if event_column.dtype() != &DataType::Utf8 {
                let event_column_string = event_column.cast(&DataType::Utf8)
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
                
                // Use replace instead of with_column to avoid type mismatch
                df.replace("Event", event_column_string)
                    .map_err(|e| Box::new(e) as Box<dyn Error>)?;
            }
        }
        
        // Push the DataFrame to the vector
        dataframes.push(df);
    }
    
    Ok(dataframes)
}











/// Performs comprehensive game data analysis.
fn perform_game_data_analysis(game_data_path: &str) -> Result<(), Box<dyn Error>> {
    let games = analysis::read_games(game_data_path)?;
    let graph = analysis::build_graph(&games);
    let pagerank_scores = analysis::calculate_pagerank(&graph);
    let betweenness_centrality = analysis::calculate_betweenness_centrality(&graph);
    let closeness_centrality = analysis::calculate_closeness_centrality(&graph);

    analysis::export_centrality_data(&pagerank_scores, &graph, "pagerank_scores.csv")?;
    analysis::export_centrality_data(&betweenness_centrality, &graph, "betweenness_centrality.csv")?;
    analysis::export_centrality_data(&closeness_centrality, &graph, "closeness_centrality.csv")?;

    let performance = analysis::track_player_performance(&games);
    analysis::export_performance(&performance, "player_performance.csv")
}