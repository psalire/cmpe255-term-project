"""Use nba's api to build datasets"""

import sys
from api import API as NBA_API
# import pandas as pd
sys.path.append('../')
from datasets import data as Local ## Local module to get dataframes
from pprint import PrettyPrinter

DATASETS_DIR = '../datasets/'

class DatasetBuilder:
    """Keeps track of games visited per team to use with the nba api"""

    def __init__(self, dataset_dir=DATASETS_DIR):
        self.stats_df = Local.get_cumulative_games_stats_dataframe(DATASETS_DIR)
        self.visited_game_ids = {} # {team_id: {season:{game_ids}}}
        self.api = NBA_API()
        self.dataset_dir = dataset_dir

    def add_game_id(self, game_id, team_id, season):
        """Appends current game_id to visited_game_ids"""

        game_id=str(game_id)
        team_id=str(team_id)
        season=str(season)

        # Add current game_id to set of visited game_ids
        if team_id not in self.visited_game_ids:
            self.visited_game_ids[team_id] = {}
        if season not in self.visited_game_ids[team_id]:
            self.visited_game_ids[team_id][season] = set()
        self.visited_game_ids[team_id][season].add(game_id)

    def get_team_stats(self, team_id, season):
        """Calls API to get cumulative stats using visited_game_ids"""

        team_id=str(team_id)
        season=str(season)

        return self.api.get_cumulative_team_stats(
            self.visited_game_ids[team_id][season],
            team_id,
            season
        )

    def update_dataframe(self, date, json, save_to_csv='cumulative_games_stats.csv'):
        """Updates dataframe with json"""


def main():
    """Main script"""

    # Open games dataset
    games_df, _ = Local.get_games_and_winners_dataframe(DATASETS_DIR)
    games_df = games_df[[
        'GAME_DATE_EST',
        'GAME_ID',
        'HOME_TEAM_ID',
        'VISITOR_TEAM_ID',
        'SEASON',
    ]]

    dataset_builder = DatasetBuilder()
    # dataset_builder.add_game_id('0022000677','1610612737','2020')

    i=0
    for _, row in games_df.iterrows():
        dataset_builder.add_game_id(
            row['GAME_ID'],
            row['HOME_TEAM_ID'],
            row['SEASON'],
        )
        dataset_builder.add_game_id(
            row['GAME_ID'],
            row['VISITOR_TEAM_ID'],
            row['SEASON'],
        )
        if i==50:
            break
        i+=1
    PrettyPrinter().pprint(dataset_builder.visited_game_ids)

    # dataset_builder = DatasetBuilder()
    # dataset_builder.add_game_id('0022000677','1610612737','2020')
    # dataset_builder.add_game_id('0022000660','1610612737','2020')
    # dataset_builder.add_game_id('0022000640','1610612737','2020')
    # print(dataset_builder.visited_game_ids)
    #
    # PrettyPrinter().pprint(dataset_builder.get_team_stats('1610612737','2020'))

if __name__=='__main__':
    main()
