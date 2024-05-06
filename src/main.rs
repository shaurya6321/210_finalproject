use std::error::Error;
use std::fs::File;
use std::io::BufRead;
use std::path::Path;
use polars::prelude::*;
use std::io::BufReader;
use csv::Writer;
use std::collections::HashMap;



mod data_distribution;
mod column_info;
mod analysis;
mod strategy_analysis;

fn main() -> Result<(), Box<dyn Error>> {
    let current_dir = std::env::current_dir()?;
    let input_files = [
        current_dir.join(Path::new("game1.csv")),
        current_dir.join(Path::new("game2.csv")),
    ];
    let output_files = [
        current_dir.join(Path::new("subset_data_1.csv")),
        current_dir.join(Path::new("subset_data_2.csv")),
        current_dir.join(Path::new("subset_data_3.csv")),
        current_dir.join(Path::new("subset_data_4.csv")),
        current_dir.join(Path::new("subset_data_5.csv")),
    ];


    for file in &input_files {
        println!("{}", file.display());
    }
    

    for file in &output_files {
        println!("{}", file.display());
    }

    column_info::print_column_info(&input_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>())?;

    let (header, combined_data) = combine_csv_files(&input_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>())?;

    data_distribution::distribute_data(&combined_data, &header, &output_files.iter().map(|p| p.to_str().unwrap_or_default()).collect::<Vec<_>>())?;

    
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

fn perform_game_data_analysis(input_files: &[&str], output_file: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all("./out")?;

    let pr_scores_file = "./out/pr_scores.csv";
    let btw_scores_file = "./out/btw_scores.csv";
    let cls_scores_file = "./out/cls_scores.csv";
    let player_perf_file = "./out/player_perf.csv";
    let in_out_degree_file = "./out/in_out_degree.csv";
    let weighted_centrality_file = "./out/weighted_centrality.csv";
    let mean_mode_metrics_file = "./out/mean_mode_metrics.csv";

    let mut output_writer = Writer::from_path(output_file)?;

    for input_file in input_files {
        let file = File::open(input_file)?;
        let reader = BufReader::new(file);
        let df = CsvReader::new(reader)
            .infer_schema(None)
            .has_header(true)
            .finish()?;

        let games = analysis::read_games_from_dataframe(&df)?;
        let graph = analysis::build_graph(&games);
        let pagerank_scores = analysis::calculate_pagerank(&graph);
        let betweenness_centrality = analysis::calculate_betweenness_centrality(&graph);
        let closeness_centrality = analysis::calculate_closeness_centrality(&graph);
        let performance = analysis::track_player_performance(&games);


        let in_out_degree_centrality = analysis::calculate_in_out_degree_centrality(&graph);


        let (weighted_betweenness, weighted_closeness) = analysis::calculate_weighted_centrality(&graph);


        let mean_mode_metrics = analysis::calculate_mean_mode(&games);


        let mut player_eco_classifications: HashMap<String, HashMap<String, u32>> = HashMap::new();
        for (eco, game_group) in strategy_analysis::classify_games_by_eco(&games).iter() {
            for game in game_group {
                let white_player = &game.white;
                let black_player = &game.black;

                player_eco_classifications
                    .entry(white_player.clone())
                    .or_insert_with(HashMap::new)
                    .entry(eco.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);

                player_eco_classifications
                    .entry(black_player.clone())
                    .or_insert_with(HashMap::new)
                    .entry(eco.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }

        output_writer.write_record(&["Player", "ECO", "Count", "", "", "", ""])?;
        for (player, eco_counts) in &player_eco_classifications {
            for (eco, count) in eco_counts {
                output_writer.write_record(&[player.as_str(), eco.as_str(), &count.to_string(), "", "", "", ""])?;
            }
        }

        analysis::export_centrality_data(&pagerank_scores, &graph, pr_scores_file)?;
        analysis::export_centrality_data(&betweenness_centrality, &graph, btw_scores_file)?;
        analysis::export_centrality_data(&closeness_centrality, &graph, cls_scores_file)?;
        analysis::export_performance(&performance, player_perf_file)?;
        analysis::export_in_out_degree_centrality(&in_out_degree_centrality, &graph, in_out_degree_file)?;
        analysis::export_weighted_centrality(&weighted_betweenness, &weighted_closeness, &graph, weighted_centrality_file)?;
        analysis::export_mean_mode_metrics(&mean_mode_metrics, mean_mode_metrics_file)?;
    }

    output_writer.write_record(&["Analysis Type", "Player", "Score", "Win Rate", "Draws", "Mean Rating Diff", "Game Count"])?;

    let mut pr_reader = csv::Reader::from_path(pr_scores_file)?;
    for result in pr_reader.records() {
        let record = result?;
        output_writer.write_record(&["PageRank", &record[0], &record[1], "", "", "", ""])?;
    }

    let mut btw_reader = csv::Reader::from_path(btw_scores_file)?;
    for result in btw_reader.records() {
        let record = result?;
        output_writer.write_record(&["Betweenness Centrality", &record[0], &record[1], "", "", "", ""])?;
    }

    let mut cls_reader = csv::Reader::from_path(cls_scores_file)?;
    for result in cls_reader.records() {
        let record = result?;
        output_writer.write_record(&["Closeness Centrality", &record[0], &record[1], "", "", "", ""])?;
    }

    let mut perf_reader = csv::Reader::from_path(player_perf_file)?;
    for result in perf_reader.records() {
        let record = result?;
        output_writer.write_record(&[
            "Player Performance",
            &record[0],
            "",
            &record[6],
            &record[3],
            &record[5],
            &record[1],
        ])?;
    }

    let mut in_out_degree_reader = csv::Reader::from_path(in_out_degree_file)?;
    for result in in_out_degree_reader.records() {
        let record = result?;
        output_writer.write_record(&["In-Degree", &record[0], &record[1], "", "", "", ""])?;
        output_writer.write_record(&["Out-Degree", &record[0], &record[1], "", "", "", ""])?;
    }

    let mut weighted_centrality_reader = csv::Reader::from_path(weighted_centrality_file)?;
    for result in weighted_centrality_reader.records() {
        let record = result?;
        output_writer.write_record(&["Weighted Betweenness", &record[0], &record[1], "", "", "", ""])?;
        output_writer.write_record(&["Weighted Closeness", &record[0], &record[1], "", "", "", ""])?;
    }

    let mut mean_mode_metrics_reader = csv::Reader::from_path(mean_mode_metrics_file)?;
    for result in mean_mode_metrics_reader.records() {
        let record = result?;
        output_writer.write_record(&[
            "Mean/Mode Metrics",
            &record[0],
            "",
            &record[1],
            &record[2],
            &record[3],
            &record[4],
        ])?;
    }

    output_writer.flush()?;

    Ok(())
}





#[cfg(test)]
mod tests {
    use crate::analysis::{Game, build_graph, calculate_in_out_degree_centrality, export_in_out_degree_centrality};
    use std::fs::File;
    use std::io::Read;
    use tempfile::tempdir;

    #[test]
    fn test_build_graph() {
        let games = vec![
            Game {
                game_id: "1".to_string(),
                white: "Player1".to_string(),
                black: "Player2".to_string(),
                result: "1-0".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "2".to_string(),
                white: "Player2".to_string(),
                black: "Player3".to_string(),
                result: "0-1".to_string(),
                ..Default::default()
            },
        ];

        let graph = build_graph(&games);
        assert_eq!(graph.node_count(), 3);
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_calculate_pagerank() {
        let games = vec![
            Game {
                game_id: "1".to_string(),
                white: "Player1".to_string(),
                black: "Player2".to_string(),
                result: "1-0".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "2".to_string(),
                white: "Player2".to_string(),
                black: "Player3".to_string(),
                result: "0-1".to_string(),
                ..Default::default()
            },
        ];

        let graph = build_graph(&games);
        let pagerank_scores = calculate_pagerank(&graph);
        assert_eq!(pagerank_scores.len(), 3);

    }

    #[test]
    fn test_calculate_in_out_degree_centrality() {
        let games = vec![
            Game {
                game_id: "1".to_string(),
                white: "Player1".to_string(),
                black: "Player2".to_string(),
                result: "1-0".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "2".to_string(),
                white: "Player2".to_string(),
                black: "Player3".to_string(),
                result: "0-1".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "3".to_string(),
                white: "Player3".to_string(),
                black: "Player1".to_string(),
                result: "1/2-1/2".to_string(),
                ..Default::default()
            },
        ];

        let graph = build_graph(&games);
        let in_out_degree_centrality = calculate_in_out_degree_centrality(&graph);
        assert_eq!(in_out_degree_centrality.len(), 3);

        assert_eq!(in_out_degree_centrality[&graph.node_indices().nth(0).unwrap()], (1, 1));
        assert_eq!(in_out_degree_centrality[&graph.node_indices().nth(1).unwrap()], (1, 1));
        assert_eq!(in_out_degree_centrality[&graph.node_indices().nth(2).unwrap()], (1, 1));
    }

    use crate::analysis::calculate_pagerank;

    #[test]
    fn test_export_in_out_degree_centrality() {
        let games = vec![
            Game {
                game_id: "1".to_string(),
                white: "Player1".to_string(),
                black: "Player2".to_string(),
                result: "1-0".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "2".to_string(),
                white: "Player2".to_string(),
                black: "Player3".to_string(),
                result: "0-1".to_string(),
                ..Default::default()
            },
        ];
    
        let graph = build_graph(&games);
        let in_out_degree_centrality = calculate_in_out_degree_centrality(&graph);
    
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("in_out_degree_centrality.csv");
        export_in_out_degree_centrality(&in_out_degree_centrality, &graph, file_path.to_str().unwrap()).unwrap();
    
        let mut file = File::open(file_path).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
    
        let mut actual_lines: Vec<&str> = contents.lines().collect();
        actual_lines.sort();
        let actual_contents = actual_lines.join("\n");
    
        let expected_lines: Vec<&str> = ["Player1,0,1", "Player2,1,1", "Player3,1,0"].to_vec();
        let expected_contents = expected_lines.join("\n");
    
        assert_eq!(actual_contents, expected_contents);
    }
    

}
