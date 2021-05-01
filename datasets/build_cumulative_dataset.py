"""Build cumulative dataset from games.csv"""

import pandas as pd
# import numpy as np
import data as Local ## Local module to get dataframes

DATASETS_DIR = '../datasets/'

class DatasetBuilder:
    """Class to build cumulative dataframe from games.csv"""

    def __init__(self, datasets_dir=DATASETS_DIR, out_filename='out'):
        games_data = Local.get_games_and_targets_dataframe(DATASETS_DIR)
        self.games_df = games_data[0]
        self.ranking_df = Local.get_ranking_dataframe(DATASETS_DIR)
        self.cumulative_stats = {}
        # Don't add averages, only countable values
        self.avg_cols = {
            'FG_PCT_total','FT_PCT_total','FG3_PCT_total',
            'PPG_total','APG_total','RPG_total',
        }
        self.countable_cols = {
            'GAMES_PLAYED','PTS_total','AST_total','REB_total',
        }

        self.stats_df_columns = \
            ['DATE','GAME_ID','TEAM_ID','SEASON_TYPE','SEASON','AT_HOME']+ \
            list(self.countable_cols) + \
            list(self.avg_cols)
        self.stats_df = pd.DataFrame(columns=self.stats_df_columns)
        self.datasets_dir = datasets_dir
        self.out_filename = out_filename

    def accumulate_values(self, row):
        """Add values to existing dict val, or add to dict if DNE"""

        for team_id, home_team in zip(
            [row['HOME_TEAM_ID'],row['VISITOR_TEAM_ID']],
            [True,False]
        ):

            if team_id not in self.cumulative_stats:
                # Add entry for team if DNE
                self.cumulative_stats[team_id] = {}
            if row['SEASON'] not in self.cumulative_stats[team_id]:
                # Add entry for sesason if DNE
                self.cumulative_stats[team_id][row['SEASON']] = {
                    'TEAM_ID': str(team_id),
                    'SEASON': row['SEASON'],
                    'GAMES_PLAYED': 0,
                    'GAMES_WON': 0
                }
            team_stats_dict = self.cumulative_stats[team_id][row['SEASON']]

            # Add stats values
            team_stats_dict['GAME_ID']=str(row['GAME_ID'])
            team_stats_dict['SEASON_TYPE']=str(row['GAME_ID'])[0]
            team_stats_dict['DATE']=row['GAME_DATE_EST']
            team_stats_dict['AT_HOME']=home_team

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
                # Add computed stats values

                curr_key = key.format('total')
                val = row[key.format(tag).replace('sum_','')]
                if str(val)=='nan':
                    val = 0
                    none_val = True
                if curr_key in team_stats_dict:
                    team_stats_dict[curr_key] += val
                else:
                    team_stats_dict[curr_key] = val

            team_stats_dict['GAMES_PLAYED'] += int(not none_val)
            team_stats_dict['GAMES_WON'] += int(
                (row['PTS_home']>row['PTS_away']) if home_team \
                else (row['PTS_away']>row['PTS_home'])
            )
            if team_stats_dict['GAMES_PLAYED']:
                team_stats_dict['W_PCT'] = team_stats_dict['GAMES_WON']/team_stats_dict['GAMES_PLAYED']
            else:
                team_stats_dict['W_PCT'] = 0.0

            # Calculate averages
            key_and_totals = [
                ('FG_PCT_total','sum_FG_PCT_total'),
                ('FT_PCT_total','sum_FT_PCT_total'),
                ('FG3_PCT_total','sum_FG3_PCT_total',),
                ('PPG_total','PTS_total'),
                ('APG_total','AST_total'),
                ('RPG_total','REB_total'),
            ]
            if team_stats_dict['GAMES_PLAYED']>0:
                for key,tot in key_and_totals:
                    team_stats_dict[key] = \
                        team_stats_dict[tot] / \
                        team_stats_dict['GAMES_PLAYED']
            else:
                for key,tot in key_and_totals:
                    team_stats_dict[key] = 0


    def add_current_values_to_df(self, team_id, row_vals):
        """Add row to dataframe with values of current dict val"""
        season = row_vals['SEASON']
        if team_id in self.cumulative_stats and season in self.cumulative_stats[team_id]:
            current_vals = {
                k:self.cumulative_stats[team_id][season][k] \
                for k in self.cumulative_stats[team_id][season] \
                if k not in {'GAMES_WON','sum_FG_PCT_total','sum_FT_PCT_total','sum_FG3_PCT_total'}
            }
        else:
            # If values dne, i.e. first game of season
            current_vals = {k:0 for k in self.stats_df_columns}
            current_vals['TEAM_ID'] = team_id
            current_vals['W_PCT'] = 0
            current_vals['GAMES_PLAYED'] = 0

        # Use metadata of current game, not previous
        current_vals['AT_HOME'] = team_id==row_vals['HOME_TEAM_ID']
        current_vals['GAME_ID'] = row_vals['GAME_ID']
        current_vals['SEASON_TYPE'] = str(row_vals['GAME_ID'])[0]
        current_vals['DATE'] = row_vals['GAME_DATE_EST']
        current_vals['SEASON'] = season

        self.stats_df = self.stats_df.append(
            current_vals,
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
        for team_id in [row['HOME_TEAM_ID'],row['VISITOR_TEAM_ID']]:
            dataset_builder.add_current_values_to_df(team_id, row)
        dataset_builder.accumulate_values(row)
        rows+=1
        # if rows%60 == 0:
        #     break
        if rows%1000 == 0:
            print(f'Row {rows}...')
            dataset_builder.save_to_csv()
    dataset_builder.save_to_csv()

if __name__=='__main__':
    main()
