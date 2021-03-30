"""Use nba's api to build datasets"""

import sys
import json
import time
from api import API as NBA_API
# import pandas as pd
sys.path.append('../')
from datasets import data as Local ## Local module to get dataframes
# from pprint import PrettyPrinter

DATASETS_DIR = '../datasets/'

class DatasetBuilder:
    """
        Keeps track of games visited per team to use with the nba api
        REQUIRED: cumulative_games_stats.csv must have the required headers:
                  DATE,SEASONTYPE,HOME,CITY,NICKNAME,TEAM_ID,W,L,W_HOME,
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
        self.max_games_allowed = 72
        with open(self.dataset_dir+'cumulative_dict.json') as json_file:
            self.visited_game_ids = json.load(json_file)

    def add_game_id(self, game_id, team_id, season):
        """
            Appends current game_id to visited_game_ids
            returns bool, whether at limit of max_games_allowed
        """

        game_id=str(game_id)
        team_id=str(team_id)
        season=str(season)

        season_type=game_id[0] # 1-pre,2-reg,4-post

        # Add current game_id to set of visited game_ids
        if team_id not in self.visited_game_ids:
            self.visited_game_ids[team_id] = {}
        if season not in self.visited_game_ids[team_id]:
            self.visited_game_ids[team_id][season] = {}
        if season_type not in self.visited_game_ids[team_id][season]:
            self.visited_game_ids[team_id][season][season_type] = []
        self.visited_game_ids[team_id][season][season_type].append(game_id)

        return len(self.visited_game_ids[team_id][season][season_type]) > \
                self.max_games_allowed

    def _get_team_stats(self, team_id, season_type, season):
        """Calls API to get cumulative stats using visited_game_ids"""

        team_id=str(team_id)
        season=str(season)

        if season_type=='1':
            season_type_str='Pre Season'
        elif season_type=='2':
            season_type_str='Regular Season'
        elif season_type=='4':
            season_type_str='Playoffs'
        else:
            print(f'Unexpected season_type {season_type}')
            return None
        # print(self.visited_game_ids[team_id][season][season_type])
        return self.api.get_cumulative_team_stats(
            self.visited_game_ids[team_id][season][season_type],
            team_id,
            season,
            season_type=season_type_str
        )

    def update_dataframe(self, date, at_home, team_id, season_type, season):
        """Updates dataframe with json"""

        # Fetch json
        print(
            f'[INFO] Fetching JSON for {date}, teamID-{team_id}, '+
            f'gameType-{season_type} season-{season}:'
        )

        backoff=2
        while True:
            json_res = self._get_team_stats(team_id, season_type, season)

            if json_res is None:
                # Invalid season_type
                return

            if 'resultSets' in json_res:
                break

            # If api failed due to internal error
            if 'Message' in json_res and json_res['Message']=='An error has occurred.':
                print('***Received 500 error***')

                if backoff>=8:
                    print('Retries failed.')

                    print('Retrying with previous game removed...')
                    # Try remove previous value
                    popped_val = \
                        self.visited_game_ids[str(team_id)][str(season)][str(season_type)].pop(-2)
                    json_res = self._get_team_stats(team_id, season_type, season)
                    # if successful
                    if 'resultSets' in json_res:
                        print('Success!')
                        break
                    if 'Message' not in json_res or json_res['Message']!='An error has occurred.':
                        print('Unexpected error:')
                        print(json_res)
                        print('Exiting...')
                        sys.exit(1)

                    print('Fail, popping current game and adding empty row...')
                    # Remove the current bad game and restore previously popped game
                    self.visited_game_ids[str(team_id)][str(season)][str(season_type)].pop()
                    self.visited_game_ids[str(team_id)][str(season)][str(season_type)].append(
                        popped_val
                    )
                    # Update dataframe with empty row
                    self.stats_df = self.stats_df.append(
                        {
                            **dict(zip(
                                self.stats_df.columns[3:],
                                [None]*len(self.stats_df.columns[3:])
                            )),
                            'DATE': date,
                            'SEASONTYPE': str(season_type),
                            'HOME': at_home,
                            'TEAM_ID': str(team_id)
                        },
                        ignore_index=True,
                    )
                    return

                print(f'Trying again in {backoff} seconds...')
                time.sleep(backoff)
                backoff*=2
            else:
                print('Unexpected error:')
                print(json_res)
                print('Exiting...')
                sys.exit(1)

        stats_vals = json_res['resultSets'][1]['rowSet'][0]

        assert len(self.stats_df.columns)-3==len(stats_vals)
        stats_dict = dict(zip(
            self.stats_df.columns[3:], # Skip DATE, SEASONTYPE, HOME
            stats_vals
        ))

        # Update dataframe
        self.stats_df = self.stats_df.append(
            {
                'DATE': date,
                'SEASONTYPE': season_type,
                'HOME': at_home,
                **stats_dict
            },
            ignore_index=True,
        )

    def save_dataframe_to_csv(self, filename):
        """Save dataframe to csv at self.dataset_dir/filename, and json dict"""

        self.stats_df.to_csv(self.dataset_dir+filename, index=False)
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

    if len(dataset_builder.stats_df) > 0:
        # Start at index if continuing after crash
        start_index = (len(dataset_builder.stats_df)+1)//2
    else:
        start_index = 0

    print(f'Starting at {start_index}...')

    # print(games_df.iloc[start_index-1:start_index+1])
    # return

    if start_index>0 and \
      ((dataset_builder.stats_df.iloc[len(dataset_builder.stats_df)-2]['TEAM_ID']
      != games_df.iloc[start_index-1]['HOME_TEAM_ID']) or \
      (dataset_builder.stats_df.iloc[len(dataset_builder.stats_df)-1]['TEAM_ID']
      != games_df.iloc[start_index-1]['VISITOR_TEAM_ID'])):
        print('Failed continuation check. Exiting...')
        sys.exit(1)

    games_df = games_df[start_index:]

    requests = 0
    for _, row in games_df.iterrows():
        requests+=1
        print(f'REQUEST #{requests}')
        # Update dataframe for both home & visiting team
        at_home=True # First game is at home
        for team_id in [row['HOME_TEAM_ID'], row['VISITOR_TEAM_ID']]:
            # Add entry
            at_max = dataset_builder.add_game_id(
                row['GAME_ID'],
                team_id,
                row['SEASON'],
            )
            # Update dataframe, and save csv every 10 requests
            ## API only allows adding 72 games at a time
            ## so if at limit, don't make request
            if not at_max:
                dataset_builder.update_dataframe(
                    row['GAME_DATE_EST'],
                    at_home,
                    team_id,
                    str(row['GAME_ID'])[0],
                    row['SEASON'],
                )
            else:
                print('[INFO] At max, skipping and adding empty row...')
                # Update dataframe with empty row
                dataset_builder.stats_df = dataset_builder.stats_df.append(
                    {
                        **dict(zip(
                            dataset_builder.stats_df.columns[3:],
                            [None]*len(dataset_builder.stats_df.columns[3:])
                        )),
                        'DATE': str(row['GAME_DATE_EST']),
                        'SEASONTYPE': str(row['GAME_ID'])[0],
                        'HOME': at_home,
                        'TEAM_ID': str(team_id)
                    },
                    ignore_index=True,
                )
            at_home=False # Second game is visitor
        if requests%10 == 0:
            dataset_builder.save_dataframe_to_csv('2017_cumulative_games_stats.csv')

    dataset_builder.save_dataframe_to_csv('2017_cumulative_games_stats.csv')

if __name__=='__main__':
    main()
