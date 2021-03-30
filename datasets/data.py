"""Functions to get datasets as dataframes"""

import pandas as pd

# DATASETS_DIR = 'datasets/'
DATASETS_DIR = './'

def get_games_and_winners_dataframe(dir=DATASETS_DIR):
    """games.csv"""

    games_df = pd.read_csv(dir+'games.csv')
    games_df.sort_values(by='GAME_DATE_EST', inplace=True)
    winners_df = games_df['HOME_TEAM_WINS']
    del games_df['HOME_TEAM_WINS']
    del games_df['GAME_STATUS_TEXT'] # Always 'Final'

    return games_df, winners_df

def get_games_details_dataframe(dir=DATASETS_DIR):
    """games_details.csv"""

    return pd.read_csv(dir+'games_details.csv')

def get_teams_dataframe(dir=DATASETS_DIR):
    """teams.csv"""

    teams_df = pd.read_csv(dir+'teams.csv')
    del teams_df['LEAGUE_ID'] # Always 0
    del teams_df['YEARFOUNDED'] # Redundant w/ 'MIN_YEAR'
    del teams_df['ARENA'] # Name of arena not needed
    del teams_df['DLEAGUEAFFILIATION'] # Name of arena not needed

    return teams_df

def get_players_dataframe(dir=DATASETS_DIR):
    """players.csv"""

    return pd.read_csv(dir+'players.csv')

def get_ranking_dataframe(dir=DATASETS_DIR):
    """ranking.csv"""

    ranking_df = pd.read_csv(dir+'ranking.csv')
    ranking_df.sort_values(by='STANDINGSDATE', inplace=True)
    del ranking_df['LEAGUE_ID'] # Always 0

    return ranking_df


def get_all_dataset_dataframes(dir=DATASETS_DIR):
    """Return dataset dataframes"""

    return (
        *get_games_and_winners_dataframe(dir),
        get_games_details_dataframe(dir),
        get_teams_dataframe(dir),
        get_players_dataframe(dir),
        get_ranking_dataframe(dir),
    )
