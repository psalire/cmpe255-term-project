"""Use nba's api to build datasets"""

import sys
import json
from api import API as NBA_API
# import pandas as pd
sys.path.append('../')
from datasets import data as Local ## Local module to get dataframes
# from pprint import PrettyPrinter

DATASETS_DIR = '../datasets/'

class DatasetBuilder:
    """
        Keeps track of games visited per team to use with the nba api
        REQUIRED: cumulative_games_stats.csv must be empty except for the required headers:
                  DATE,CITY,NICKNAME,TEAM_ID,W,L,W_HOME,
                  L_HOME,W_ROAD,L_ROAD,TEAM_TURNOVERS,
                  TEAM_REBOUNDS,GP,GS,ACTUAL_MINUTES,
                  ACTUAL_SECONDS,FG,FGA,FG_PCT,FG3,FG3A,
                  FG3_PCT,FT,FTA,FT_PCT,OFF_REB,DEF_REB,
                  TOT_REB,AST,PF,STL,TOTAL_TURNOVERS,BLK,
                  PTS,AVG_REB,AVG_PTS,DQ
    """

    def __init__(self, dataset_dir=DATASETS_DIR):
        self.stats_df = Local.get_cumulative_games_stats_dataframe(DATASETS_DIR)
        self.api = NBA_API()
        self.dataset_dir = dataset_dir
        with open(self.dataset_dir+'cumulative_dict.json') as json_file:
            self.visited_game_ids = json.load(json_file)

    def add_game_id(self, game_id, team_id, season):
        """Appends current game_id to visited_game_ids"""

        game_id=str(game_id)
        team_id=str(team_id)
        season=str(season)

        # Add current game_id to set of visited game_ids
        if team_id not in self.visited_game_ids:
            self.visited_game_ids[team_id] = {}
        if season not in self.visited_game_ids[team_id]:
            self.visited_game_ids[team_id][season] = []
        self.visited_game_ids[team_id][season].append(game_id)

    def _get_team_stats(self, team_id, season):
        """Calls API to get cumulative stats using visited_game_ids"""

        team_id=str(team_id)
        season=str(season)

        return self.api.get_cumulative_team_stats(
            self.visited_game_ids[team_id][season],
            team_id,
            season
        )

    def update_dataframe(self, date, team_id, season):
        """Updates dataframe with json"""

        # Fetch json
        print(f'[INFO] Fetching JSON for {date}, teamID-{team_id}, season-{season}:')
        json_res = self._get_team_stats(team_id, season)
        stats_vals = json_res['resultSets'][1]['rowSet'][0]
        stats_dict = dict(zip(self.stats_df.columns[1:],stats_vals))

        # Update dataframe
        self.stats_df = self.stats_df.append(
            {'DATE': date, **stats_dict},
            ignore_index=True,
        )

    def save_dataframe_to_csv(self, filename):
        """Save dataframe to csv at self.dataset_dir/filename, and json dict"""

        self.stats_df.to_csv(self.dataset_dir+filename)
        with open(self.dataset_dir+'cumulative_dict.json', 'w') as json_file:
            json.dump(self.visited_game_ids, json_file)

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
    # Endpoint only supports 2017 season and after
    games_df = games_df[
        (games_df['SEASON'] >= 2017)
    ]

    dataset_builder = DatasetBuilder()

    requests = 0
    for _, row in games_df.iterrows():
        requests+=1
        print(f'REQUEST #{requests}')
        # Update dataframe for both home & visiting team
        for team_id in [row['HOME_TEAM_ID'], row['VISITOR_TEAM_ID']]:
            # Add entry
            dataset_builder.add_game_id(
                row['GAME_ID'],
                team_id,
                row['SEASON'],
            )
            # Update dataframe, and save csv every 10 requests
            dataset_builder.update_dataframe(
                row['GAME_DATE_EST'],
                team_id,
                row['SEASON'],
            )
        if requests%10 == 0:
            dataset_builder.save_dataframe_to_csv('cumulative_games_stats.csv')

    dataset_builder.save_dataframe_to_csv('cumulative_games_stats.csv')

if __name__=='__main__':
    main()
