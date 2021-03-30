"""Functions to get datasets as dataframes"""

import pandas as pd

# DATASETS_DIR = 'datasets/'
DATASETS_DIR = './'

def get_games_and_winners_dataframe(path=DATASETS_DIR):
    """games.csv"""

    games_df = pd.read_csv(path+'games.csv')
    games_df.sort_values(by='GAME_DATE_EST', inplace=True)
    winners_df = games_df['HOME_TEAM_WINS']
    del games_df['HOME_TEAM_WINS']
    del games_df['GAME_STATUS_TEXT'] # Always 'Final'

    return games_df, winners_df

def get_cumulative_games_stats_dataframe(path=DATASETS_DIR):
    """cumulative_games_stats.csv"""

    return pd.read_csv(path+'cumulative_games_stats.csv')

def get_games_details_dataframe(path=DATASETS_DIR):
    """games_details.csv"""

    return pd.read_csv(path+'games_details.csv')

def get_teams_dataframe(path=DATASETS_DIR):
    """teams.csv"""

    teams_df = pd.read_csv(path+'teams.csv')
    del teams_df['LEAGUE_ID'] # Always 0
    del teams_df['YEARFOUNDED'] # Redundant w/ 'MIN_YEAR'
    del teams_df['ARENA'] # Name of arena not needed
    del teams_df['DLEAGUEAFFILIATION'] # Name of arena not needed

    return teams_df

def get_players_dataframe(path=DATASETS_DIR):
    """players.csv"""

    return pd.read_csv(path+'players.csv')

def get_ranking_dataframe(path=DATASETS_DIR):
    """ranking.csv"""

    ranking_df = pd.read_csv(path+'ranking.csv')
    ranking_df.sort_values(by='STANDINGSDATE', inplace=True)
    del ranking_df['LEAGUE_ID'] # Always 0

    return ranking_df


def get_all_dataset_dataframes(path=DATASETS_DIR):
    """Return dataset dataframes"""

    return (
        *get_games_and_winners_dataframe(path),
        get_games_details_dataframe(path),
        get_teams_dataframe(path),
        get_players_dataframe(path),
        get_ranking_dataframe(path),
    )
