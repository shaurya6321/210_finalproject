use csv::{Reader, Writer};
use petgraph::graph::DiGraph;
use petgraph::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use simple_pagerank::Pagerank;
use rustworkx_core::centrality::{betweenness_centrality, closeness_centrality};

#[derive(Debug, Deserialize)]
pub struct Game {
    pub white: Option<String>,
    pub black: Option<String>,
    pub result: Option<String>,
    pub white_elo: Option<u32>,
    pub black_elo: Option<u32>,
    pub white_rating_diff: Option<i32>,
    pub black_rating_diff: Option<i32>,
}

#[derive(Default, Debug, Serialize)]
pub struct PlayerPerformance {
    games_played: u32,
    games_won: u32,
    games_lost: u32,
    games_drawn: u32,
    total_rating_change: i32,
    win_rate: f64, // Include win_rate in the serialization directly
}

impl PlayerPerformance {
    pub fn update(&mut self, result: &str, rating_diff: i32) {
        self.games_played += 1;
        self.total_rating_change += rating_diff;
        match result {
            "1-0" => self.games_won += 1,
            "0-1" => self.games_lost += 1,
            "1/2-1/2" => self.games_drawn += 1,
            _ => {}
        }
        self.win_rate = self.calculate_win_rate();  // Update win_rate each time performance is updated
    }

    fn calculate_win_rate(&self) -> f64 {
        if self.games_played == 0 {
            0.0
        } else {
            self.games_won as f64 / self.games_played as f64
        }
    }
}

pub fn read_games(filepath: &str) -> Result<Vec<Game>, Box<dyn Error>> {
    let file = File::open(filepath)?;
    let mut rdr = Reader::from_reader(BufReader::new(file));
    let mut games = Vec::new();
    for result in rdr.deserialize() {
        let game: Game = result?;
        if game.white.is_some() && game.black.is_some() {
            games.push(game);
        }
    }
    Ok(games)
}

pub fn build_graph(games: &[Game]) -> DiGraph<String, u32> {
    let mut graph = DiGraph::new();
    let mut player_indices = HashMap::new();
    for game in games {
        if let (Some(white), Some(black)) = (game.white.as_ref(), game.black.as_ref()) {
            let white_index = *player_indices.entry(white.clone()).or_insert_with(|| graph.add_node(white.clone()));
            let black_index = *player_indices.entry(black.clone()).or_insert_with(|| graph.add_node(black.clone()));
            graph.add_edge(white_index, black_index, 1);
        }
    }
    graph
}

pub fn calculate_pagerank(graph: &DiGraph<String, u32>) -> HashMap<NodeIndex, f64> {
    let mut pr = Pagerank::new();
    for edge in graph.edge_references() {
        pr.add_edge(graph[edge.source()].clone(), graph[edge.target()].clone());
    }
    pr.calculate();
    let mut pagerank_scores = HashMap::new();
    for (node, score) in pr.nodes().iter() {
        let index = graph.node_indices().find(|&i| graph[i] == **node).unwrap();
        pagerank_scores.insert(index, *score);
    }
    pagerank_scores
}

pub fn calculate_betweenness_centrality(graph: &DiGraph<String, u32>) -> HashMap<NodeIndex, f64> {
    let num_samples = graph.node_count();
    let centrality_scores = betweenness_centrality(graph, None, true, true, num_samples);
    graph.node_indices().zip(centrality_scores.into_iter()).filter_map(|(i, s)| s.map(|score| (i, score))).collect()
}

pub fn calculate_closeness_centrality(graph: &DiGraph<String, u32>) -> HashMap<NodeIndex, f64> {
    let centrality_scores = closeness_centrality(graph, true);
    graph.node_indices().zip(centrality_scores.into_iter()).filter_map(|(i, s)| s.map(|score| (i, score))).collect()
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
    let file = OpenOptions::new().write(true).create(true).open(filepath)?;
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
        let white_result = match game.result.as_ref().map(|s| s.as_str()) {
            Some("1-0") => ("1-0", "0-1"),
            Some("0-1") => ("0-1", "1-0"),
            Some("1/2-1/2") => ("1/2-1/2", "1/2-1/2"),
            _ => continue,
        };

        performance
            .entry(game.white.clone().unwrap_or_default())
            .or_default()
            .update(white_result.0, game.white_rating_diff.unwrap_or(0));
        performance
            .entry(game.black.clone().unwrap_or_default())
            .or_default()
            .update(white_result.1, game.black_rating_diff.unwrap_or(0));
    }

    performance
}