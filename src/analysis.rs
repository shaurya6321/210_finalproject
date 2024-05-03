use petgraph::graph::DiGraph;
use petgraph::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::BufWriter;
use simple_pagerank::Pagerank;
use rustworkx_core::centrality::{betweenness_centrality, closeness_centrality};
use polars::prelude::*;
use csv::Writer;

#[derive(Debug, Deserialize)]
pub struct Game {
    pub game_id: String,
    pub event: String,
    pub white: String,
    pub white_elo: Option<u32>,
    pub white_rating_diff: Option<f32>,
    pub white_tos_violation: Option<bool>,
    pub white_play_time_total: Option<String>,
    pub white_count_all: Option<u32>,
    pub black: String,
    pub black_elo: Option<u32>,
    pub black_rating_diff: Option<f32>,
    pub black_tos_violation: Option<bool>,
    pub black_play_time_total: Option<String>,
    pub black_count_all: Option<u32>,
    pub moves: String,
    pub total_moves: Option<u32>,
    pub eco: String,
    pub opening: String,
    pub time_control: String,
    pub result: String,
}

#[derive(Default, Debug, Serialize)]
pub struct PlayerPerformance {
    pub games_played: u32,
    pub games_won: u32,
    pub games_lost: u32,
    pub games_drawn: u32,
    pub total_rating_change: f32,
    pub win_rate: f64,
}

impl PlayerPerformance {
    pub fn update(&mut self, result: &str, rating_diff: f32) {
        self.games_played += 1;
        self.total_rating_change += rating_diff;
        match result {
            "1-0" => self.games_won += 1,
            "0-1" => self.games_lost += 1,
            "1/2-1/2" => self.games_drawn += 1,
            _ => {}
        }
        self.win_rate = self.calculate_win_rate();
    }

    fn calculate_win_rate(&self) -> f64 {
        if self.games_played == 0 {
            0.0
        } else {
            self.games_won as f64 / self.games_played as f64
        }
    }
}

pub fn read_games_from_dataframe(df: &DataFrame) -> Result<Vec<Game>, Box<dyn Error>> {
    let mut games = Vec::new();
    let row_count = df.height(); // Limit to the first 100 rows -- std::cmp::min(df.height(), 100)

    // Retrieve columns once to avoid repeated lookups
    let game_id_col = df.column("GameID")?.utf8()?;
    let event_col = df.column("Event")?.utf8()?;
    let white_col = df.column("White")?.utf8()?;
    let white_elo_col = df.column("WhiteElo")?.i64()?;
    let white_rating_diff_col = df.column("WhiteRatingDiff")?.f64()?;
    let white_tos_violation_col = df.column("White_tosViolation")?.bool()?;
    let white_play_time_total_col = df.column("White_playTime_total")?.f64()?;
    let white_count_all_col = df.column("White_count_all")?.f64()?; // Change to f64
    let black_col = df.column("Black")?.utf8()?;
    let black_elo_col = df.column("BlackElo")?.i64()?;
    let black_rating_diff_col = df.column("BlackRatingDiff")?.f64()?;
    let black_tos_violation_col = df.column("Black_tosViolation")?.bool()?;
    let black_play_time_total_col = df.column("Black_playTime_total")?.f64()?;
    let black_count_all_col = df.column("Black_count_all")?.f64()?; // Change to f64
    let moves_col = df.column("Moves")?.utf8()?;
    let total_moves_col = df.column("TotalMoves")?.i64()?;
    let eco_col = df.column("ECO")?.utf8()?;
    let opening_col = df.column("Opening")?.utf8()?;
    let time_control_col = df.column("TimeControl")?.utf8()?;
    let result_col = df.column("Result")?.utf8()?;

    for idx in 0..row_count {
        let game = Game {
            game_id: game_id_col.get(idx).unwrap_or_default().to_string(),
            event: event_col.get(idx).unwrap_or_default().to_string(),
            white: white_col.get(idx).unwrap_or_default().to_string(),
            white_elo: white_elo_col.get(idx).map(|v| v as u32),
            white_rating_diff: white_rating_diff_col.get(idx).map(|v| v as f32),
            white_tos_violation: white_tos_violation_col.get(idx),
            white_play_time_total: white_play_time_total_col.get(idx).map(|v| v.to_string()),
            white_count_all: white_count_all_col.get(idx).map(|v| v as u32), // Convert to u32
            black: black_col.get(idx).unwrap_or_default().to_string(),
            black_elo: black_elo_col.get(idx).map(|v| v as u32),
            black_rating_diff: black_rating_diff_col.get(idx).map(|v| v as f32),
            black_tos_violation: black_tos_violation_col.get(idx),
            black_play_time_total: black_play_time_total_col.get(idx).map(|v| v.to_string()),
            black_count_all: black_count_all_col.get(idx).map(|v| v as u32), // Convert to u32
            moves: moves_col.get(idx).unwrap_or_default().to_string(),
            total_moves: total_moves_col.get(idx).map(|v| v as u32),
            eco: eco_col.get(idx).unwrap_or_default().to_string(),
            opening: opening_col.get(idx).unwrap_or_default().to_string(),
            time_control: time_control_col.get(idx).unwrap_or_default().to_string(),
            result: result_col.get(idx).unwrap_or_default().to_string(),
        };
        games.push(game);
    }

    Ok(games)
}


pub fn build_graph(games: &[Game]) -> DiGraph<String, u32> {
    let mut graph = DiGraph::new();
    let mut player_indices = HashMap::new();

    for game in games {
        // Directly use the strings since they are not optional
        let white_index = *player_indices
            .entry(game.white.clone())
            .or_insert_with(|| graph.add_node(game.white.clone()));
        let black_index = *player_indices
            .entry(game.black.clone())
            .or_insert_with(|| graph.add_node(game.black.clone()));

        // Add an edge from white to black
        graph.add_edge(white_index, black_index, 1);
    }

    graph
}


pub fn calculate_pagerank(graph: &DiGraph<String, u32>) -> HashMap<NodeIndex, f64> {
    let mut pr = Pagerank::new();
    let mut node_indices = HashMap::new();

    // Assign a unique numeric index to each node
    for node_index in graph.node_indices() {
        node_indices.insert(graph[node_index].clone(), node_index.index() as u64);
    }

    for edge in graph.edge_references() {
        let source_index = node_indices[&graph[edge.source()]];
        let target_index = node_indices[&graph[edge.target()]];
        pr.add_edge(source_index.to_string(), target_index.to_string());
    }

    pr.calculate();
    let mut pagerank_scores = HashMap::new();

    for (node_index, score) in pr.nodes().iter() {
        if let Some(index) = graph.node_indices().find(|&i| i.index() == node_index.parse::<usize>().unwrap()) {
            pagerank_scores.insert(index, *score); // Dereference the score value
        }
    }

    pagerank_scores
}


pub fn calculate_betweenness_centrality(graph: &DiGraph<String, u32>) -> HashMap<NodeIndex, f64> {
    let num_samples = graph.node_count();
    let centrality_scores = betweenness_centrality(graph, true, true, num_samples);
    graph.node_indices().zip(centrality_scores.into_iter()).filter_map(|(i, s)| s.map(|score| (i, score))).collect()
}

pub fn calculate_closeness_centrality(graph: &DiGraph<String, u32>) -> HashMap<NodeIndex, f64> {
    let centrality_scores = closeness_centrality(graph, true);
    graph.node_indices().zip(centrality_scores.into_iter()).filter_map(|(i, s)| s.map(|score| (i, score))). collect()
}

pub fn export_centrality_data(centrality_scores: &HashMap<NodeIndex, f64>, graph: &DiGraph<String, u32>, filepath: &str) -> Result<(), Box<dyn Error>> {
    let file = OpenOptions::new().write(true).create(true).open(filepath)?;
    let mut wtr = Writer::from_writer(BufWriter::new(file));
    for (node, &score) in centrality_scores.iter() {
        wtr.serialize((graph[*node].clone(), score))?;
    }
    wtr.flush()?;
    Ok(())
}

pub fn export_performance(performance: &HashMap<String, PlayerPerformance>, filepath: &str) -> Result<(), Box<dyn Error>> {
    let file = OpenOptions::new().write(true). create(true).open(filepath)?;
    let mut wtr = Writer::from_writer(BufWriter::new(file));
    for (player, stats) in performance.iter() {
        wtr.serialize((player.clone(), stats))?;
    }
    wtr.flush()?;
    Ok(())
}

pub fn track_player_performance(games: &[Game]) -> HashMap<String, PlayerPerformance> {
    let mut performance: HashMap<String, PlayerPerformance> = HashMap::new();

    for game in games {
        let white_result = match game.result.as_ref() {
            "1-0" => ("1-0", "0-1"),
            "0-1" => ("0-1", "1-0"),
            "1/2-1/2" => ("1/2-1/2", "1/2-1/2"),
            _ => continue,
        };

        performance.entry(game.white.clone())
            .or_default()
            .update(white_result.0, game.white_rating_diff.unwrap_or(0.0));

        performance.entry(game.black.clone())
            .or_default()
            .update(white_result.1, game.black_rating_diff.unwrap_or(0.0));
    }

    performance
}
