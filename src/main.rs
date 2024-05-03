use std::error::Error;
use std::fs::File;
use std::io::BufRead;
use std::path::Path;
use polars::prelude::*;
use std::io::BufReader;
use csv::Writer;

mod data_distribution;
// mod column_info;
mod analysis;

fn main() -> Result<(), Box<dyn Error>> {
    // Construct relative file paths
    let current_dir = std::env::current_dir()?;
    let input_files = [
        current_dir.join(Path::new("games_metadata_profile_2024_01.csv")),
        current_dir.join(Path::new("games_metadata_profile.csv")),
    ];
    let output_files = [
        current_dir.join(Path::new("subset_data_1.csv")),
        current_dir.join(Path::new("subset_data_2.csv")),
        current_dir.join(Path::new("subset_data_3.csv")),
        current_dir.join(Path::new("subset_data_4.csv")),
        current_dir.join(Path::new("subset_data_5.csv")),
    ];

    // Print the file paths for debugging
    println!("Input files:");
    for file in &input_files {
        println!("{}", file.display());
    }
    
    println!("Output files:");
    for file in &output_files {
        println!("{}", file.display());
    }

    // Print column information for input files
    // column_info::print_column_info(&input_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>())?;

    // Combine data from multiple CSV files, ensuring unique headers
    let (header, combined_data) = combine_csv_files(&input_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>())?;
    // Distribute combined data into subsets for detailed analysis
    data_distribution::distribute_data(&combined_data, &header, &output_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>())?;

    // Convert relevant columns in each subset to float where necessary (only for numeric columns)
    let numeric_columns = ["WhiteElo", "BlackElo", "WhiteRatingDiff", "BlackRatingDiff"];
    // let dataframes = convert_columns_in_files(&output_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>(), &numeric_columns)?;

    // Combine the DataFrames for a single output
    // let mut combined_dataframe = data_distribution::combine_dataframes(dataframes)?;

    // Write combined DataFrame to CSV
    // let output_file = current_dir.join("combined_data.csv");
    // let file = File::create(&output_file)?;
    // let mut csv_writer = CsvWriter::new(file);
    // csv_writer.finish(&mut combined_dataframe)
    //     .map_err(|e| Box::new(e) as Box<dyn Error>)?;

    // Analyze the combined game data and export all analysis results to a single CSV file
    let analysis_output_file = current_dir.join("analysis_output.csv");
    perform_game_data_analysis(&[output_files[0].to_str().unwrap()], &analysis_output_file)?;    
    Ok(())
}

fn combine_csv_files(files: &[&str]) -> Result<(String, Vec<String>), Box<dyn Error>> {
    let mut combined_data = Vec::new();
    let mut header = String::new();

    for file_path in files {
        let file = File::open(Path::new(file_path))?;
        let reader = BufReader::new(file);
        let mut is_first_file = true;

        for (index, line) in reader.lines().enumerate() {
            let line = line?;
            if index == 0 && is_first_file {
                header = line;
                is_first_file = false;
            } else {
                combined_data.push(line);
            }
        }
    }

    Ok((header, combined_data))
}

/*
fn convert_columns_in_files(files: &[&str], columns: &[&str]) -> Result<Vec<DataFrame>, Box<dyn Error>> {
    let mut dataframes = Vec::new();
    for file_path in files {
        let df = CsvReader::from_path(Path::new(file_path))?
            .infer_schema(None)
            .has_header(true)
            .finish()?;

        let df_string = df.to_string();
        let modified_df = column_info::convert_columns_to_float(&df_string, columns)
            .map_err::<Box<dyn Error>, _>(Box::from)?;

        dataframes.push(modified_df);
    }
    Ok(dataframes)
}
*/

fn perform_game_data_analysis(input_files: &[&str], output_file: &Path) -> Result<(), Box<dyn Error>> {
    println!("Analysis output file: {}", output_file.display());

    // Create the output directory in the current working directory
    std::fs::create_dir_all("./out")?;

    // Create separate CSV files for each type of analysis result
    let pr_scores_file = "./out/pr_scores.csv";
    let btw_scores_file = "./out/btw_scores.csv";
    let cls_scores_file = "./out/cls_scores.csv";
    let player_perf_file = "./out/player_perf.csv";

    for input_file in input_files {
        let df = CsvReader::from_path(Path::new(input_file))?
            .infer_schema(None)
            .has_header(true)
            .finish()?;

        let games = analysis::read_games_from_dataframe(&df)?;
        let graph = analysis::build_graph(&games);
        let pagerank_scores = analysis::calculate_pagerank(&graph);
        let betweenness_centrality = analysis::calculate_betweenness_centrality(&graph);
        let closeness_centrality = analysis::calculate_closeness_centrality(&graph);
        let performance = analysis::track_player_performance(&games);

        // Export pagerank scores using the export_centrality_data function
        analysis::export_centrality_data(&pagerank_scores, &graph, pr_scores_file)?;

        // Export betweenness centrality scores using the export_centrality_data function
        analysis::export_centrality_data(&betweenness_centrality, &graph, btw_scores_file)?;

        // Export closeness centrality scores using the export_centrality_data function
        analysis::export_centrality_data(&closeness_centrality, &graph, cls_scores_file)?;

        // Export player performance data using the export_performance function
        analysis::export_performance(&performance, player_perf_file)?;
    }

    // Combine the separate analysis result files into a single output file
    let mut output_writer = Writer::from_path(output_file)?;
    
    // Write headers for the combined output file
    output_writer.write_record(&["Analysis Type", "Player", "Score"])?;

    // Append pagerank scores to the output file
    let mut pr_reader = csv::Reader::from_path(pr_scores_file)?;
    for result in pr_reader.records() {
        let record = result?;
        output_writer.write_record(&["PageRank", &record[0], &record[1]])?;
    }

    // Append betweenness centrality scores to the output file
    let mut btw_reader = csv::Reader::from_path(btw_scores_file)?;
    for result in btw_reader.records() {
        let record = result?;
        output_writer.write_record(&["Betweenness Centrality", &record[0], &record[1]])?;
    }

    // Append closeness centrality scores to the output file
    let mut cls_reader = csv::Reader::from_path(cls_scores_file)?;
    for result in cls_reader.records() {
        let record = result?;
        output_writer.write_record(&["Closeness Centrality", &record[0], &record[1]])?;
    }

    // Append player performance data to the output file
    let mut perf_reader = csv::Reader::from_path(player_perf_file)?;
    for result in perf_reader.records() {
        let record = result?;
        output_writer.write_record(&[
            "Player Performance",
            &record[0],
            &record[1],
            &record[2],
            &record[3],
            &record[4],
            &record[5],
            &record[6],
        ])?;
    }

    output_writer.flush()?;

    Ok(())
}