"""Functions to get datasets as dataframes"""

import pandas as pd

# DATASETS_DIR = 'datasets/'
DATASETS_DIR = './'

def get_games_and_targets_dataframe(path=DATASETS_DIR):
    """games.csv"""

    games_df = pd.read_csv(path+'games.csv')
    games_df.sort_values(by='GAME_DATE_EST', inplace=True)

    winners_df = games_df['HOME_TEAM_WINS']
    greater_fgp = games_df['HOME_HIGHER_FG_PCT']
    greater_fg3 = games_df['HOME_HIGHER_FG3_PCT']
    greater_ft = games_df['HOME_HIGHER_FT_PCT']
    greater_ast = games_df['HOME_HIGHER_AST']
    greater_reb = games_df['HOME_HIGHER_REB']

    del games_df['TEAM_ID_home'] # Redundant with HOME_TEAM_ID
    del games_df['TEAM_ID_away'] # Redundant with VISITOR_TEAM_ID
    del games_df['HOME_TEAM_WINS']
    del games_df['HOME_HIGHER_FG_PCT']
    del games_df['HOME_HIGHER_FG3_PCT']
    del games_df['HOME_HIGHER_FT_PCT']
    del games_df['HOME_HIGHER_AST']
    del games_df['HOME_HIGHER_REB']
    del games_df['GAME_STATUS_TEXT'] # Always 'Final'

    return games_df, winners_df, greater_fgp, greater_fg3, greater_ft, greater_ast, greater_reb

def get_cumulative_games_stats_dataframe(path=DATASETS_DIR):
    """combined_cumulative_games_stats.csv"""

    stats_df = pd.read_csv(path+'combined_cumulative_games_stats.csv')
    del stats_df['PTS_total_home']
    del stats_df['PTS_total_away']
    del stats_df['REB_total_home']
    del stats_df['REB_total_away']
    del stats_df['AST_total_home']
    del stats_df['AST_total_away']

    return stats_df

def get_home_visitor_games_stats_dataframe(path=DATASETS_DIR):
    """cumulative_games_stats_dataframe.csv"""

    stats_df = pd.read_csv(path+'cumulative_games_stats.csv')
    del stats_df['PTS_total_home']
    del stats_df['PTS_total_away']
    del stats_df['REB_total_home']
    del stats_df['REB_total_away']
    del stats_df['AST_total_home']
    del stats_df['AST_total_away']

    return stats_df

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
        *get_games_and_targets_dataframe(path),
        get_cumulative_games_stats_dataframe(path),
        get_games_details_dataframe(path),
        get_teams_dataframe(path),
        get_players_dataframe(path),
        get_ranking_dataframe(path),
    )

def get_2017_cumulative_games_stats_dataframe(path=DATASETS_DIR):
    """2017_cumulative_games_stats.csv"""

    return pd.read_csv(path+'2017_cumulative_games_stats.csv')
