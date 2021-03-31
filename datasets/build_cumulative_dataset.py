"""Build cumulative dataset from games.csv"""

import sys
import pandas as pd
import numpy as np
import data as Local ## Local module to get dataframes

DATASETS_DIR = '../datasets/'

class DatasetBuilder:
    """Class to build cumulative dataframe from games.csv"""

    def __init__(self, datasets_dir=DATASETS_DIR, out_filename='out'):
        self.games_df, _ = Local.get_games_and_winners_dataframe(DATASETS_DIR)
        self.cumulative_stats = {}
        # Don't add averages, only countable values
        self.avg_cols = {
            'FG_PCT_total','FT_PCT_total','FG3_PCT_total',
            'PPG_total','APG_total','RPG_total',
        }
        self.countable_cols = {
            'GAMES_PLAYED','PTS_total','AST_total','REB_total',
        }

        self.stats_df = pd.DataFrame(columns= \
            ['DATE','GAME_ID','TEAM_ID','SEASON_TYPE','SEASON','AT_HOME']+ \
            list(self.countable_cols) + \
            list(self.avg_cols)
        )
        self.datasets_dir = datasets_dir
        self.out_filename = out_filename

    def accumulate_values(self, row):
        """Add values to existing dict val, or add to dict if DNE"""

        for team_id, home_team in zip(
            [row['HOME_TEAM_ID'],row['VISITOR_TEAM_ID']], [True,False]
        ):
            if team_id not in self.cumulative_stats:
                self.cumulative_stats[team_id] = {}
            if row['SEASON'] not in self.cumulative_stats[team_id]:
                self.cumulative_stats[team_id][row['SEASON']] = {
                    'TEAM_ID': str(team_id),
                    'SEASON': row['SEASON'],
                    'AT_HOME': home_team,
                    'GAMES_PLAYED': 0,
                }
            self.cumulative_stats[team_id][row['SEASON']]['GAME_ID']=str(row['GAME_ID'])
            self.cumulative_stats[team_id][row['SEASON']]['SEASON_TYPE']=str(row['GAME_ID'])[0]
            self.cumulative_stats[team_id][row['SEASON']]['DATE']=row['GAME_DATE_EST']

            tag = 'home' if home_team else 'away'
            none_val = False
            for key in [
                'PTS_{}',
                'AST_{}',
                'REB_{}',
                'sum_FG_PCT_{}',
                'sum_FT_PCT_{}',
                'sum_FG3_PCT_{}',
            ]:
                curr_key = key.format('total')
                val = row[key.format(tag).replace('sum_','')]
                if str(val)=='nan':
                    val = 0
                    none_val = True
                if curr_key in self.cumulative_stats[team_id][row['SEASON']]:
                    self.cumulative_stats[team_id][row['SEASON']][curr_key] += val
                else:
                    self.cumulative_stats[team_id][row['SEASON']][curr_key] = val

            self.cumulative_stats[team_id][row['SEASON']]['GAMES_PLAYED']+=int(not none_val)

            # Calculate averages
            key_and_totals = [
                ('FG_PCT_total','sum_FG_PCT_total'),
                ('FT_PCT_total','sum_FT_PCT_total'),
                ('FG3_PCT_total','sum_FG3_PCT_total',),
                ('PPG_total','PTS_total'),
                ('APG_total','AST_total'),
                ('RPG_total','REB_total'),
            ]
            if self.cumulative_stats[team_id][row['SEASON']]['GAMES_PLAYED']>0:
                for key,tot in key_and_totals:
                    self.cumulative_stats[team_id][row['SEASON']][key] = \
                        self.cumulative_stats[team_id][row['SEASON']][tot] / \
                        self.cumulative_stats[team_id][row['SEASON']]['GAMES_PLAYED']
            else:
                for key,tot in key_and_totals:
                    self.cumulative_stats[team_id][row['SEASON']][key] = 0


    def add_current_values_to_df(self, team_id, season):
        """Add row to dataframe with values of current dict val"""

        self.stats_df = self.stats_df.append(
            self.cumulative_stats[team_id][season],
            ignore_index=True,
        )

    def save_to_csv(self):
        """Save dataframe to out_filename.csv"""

        self.stats_df.to_csv(self.datasets_dir+self.out_filename+'.csv', index=False)

def main():
    """Main function"""

    dataset_builder = DatasetBuilder()

    rows=0
    print('Start...')
    for _, row in dataset_builder.games_df.iterrows():
        dataset_builder.accumulate_values(row)
        for team_id in [row['HOME_TEAM_ID'],row['VISITOR_TEAM_ID']]:
            dataset_builder.add_current_values_to_df(team_id, row['SEASON'])
        rows+=1
        if rows%1000 == 0:
            print(f'Row {rows}...')
            # dataset_builder.save_to_csv()
    dataset_builder.save_to_csv()

if __name__=='__main__':
    main()
