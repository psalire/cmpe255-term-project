# CMPE 255 Term Project - NBA Predictor

## Files

- `datasets/`
    - `.csv` datasets
    - `data.py`: module to retrieve dataframes of our .csv datasets
    - `build_cumulative_dataset.py`: script used to build our training dataset for predicting game winners (`cumulative_games_stats.csv`)
- `nba-api/`
    - `api.py`: module to call NBA's cumulative API
    - `get_2017_cumulative_games_stats.py`: script used to build dataset `2017_cumulative_games_stats.csv`
