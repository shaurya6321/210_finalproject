use std::collections::HashMap;
use crate::analysis::Game;

pub fn classify_games_by_eco(games: &[Game]) -> HashMap<String, Vec<&Game>> {
    let mut eco_classifications = HashMap::new();

    for game in games {
        let eco = &game.eco;
        eco_classifications
            .entry(eco.clone())
            .or_insert_with(Vec::new)
            .push(game);
    }

    eco_classifications
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_games_by_eco() {
        let games = vec![
            Game {
                game_id: "1".to_string(),
                eco: "C60".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "2".to_string(),
                eco: "C60".to_string(),
                ..Default::default()
            },
            Game {
                game_id: "3".to_string(),
                eco: "D02".to_string(),
                ..Default::default()
            },
        ];

        let eco_classifications = classify_games_by_eco(&games);

        assert_eq!(eco_classifications.len(), 2);
        assert_eq!(eco_classifications["C60"].len(), 2);
        assert_eq!(eco_classifications["D02"].len(), 1);
    }
}