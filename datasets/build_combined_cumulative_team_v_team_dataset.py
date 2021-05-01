"""Build cumulative dataset with home/visitor stats on same row from games.csv"""

import pandas as pd
from build_cumulative_dataset import DatasetBuilder

DATASETS_DIR = '../datasets/'

class CombinedDatasetBuilder(DatasetBuilder):
    """
    Class to build cumulative dataframe with home/visitor stats on same row from games.csv
    Inherits DatasetBuilder
    """

    def __init__(self, datasets_dir=DATASETS_DIR, out_filename='out'):
        super().__init__(datasets_dir, out_filename)

        self.stats_df = pd.DataFrame(columns= \
            ['DATE','GAME_ID','SEASON_TYPE','SEASON']+ \
            [k+'_home' for k in self.countable_cols] + \
            [k+'_away' for k in self.countable_cols] + \
            [k+'_home' for k in self.avg_cols] + \
            [k+'_away' for k in self.avg_cols]
        )

    def accumulate_values(self, row):
        """Add values to existing dict val, or add to dict if DNE"""

        for team_id, opposing_team_id, home_team in zip(
            [row['HOME_TEAM_ID'],row['VISITOR_TEAM_ID']],
            [row['VISITOR_TEAM_ID'],row['HOME_TEAM_ID']],
            [True,False]
        ):
            if team_id not in self.cumulative_stats:
                # Add entry for team if DNE
                self.cumulative_stats[team_id] = {}
            if opposing_team_id not in self.cumulative_stats[team_id]:
                self.cumulative_stats[team_id][opposing_team_id] = {}
            team_stats_dict = self.cumulative_stats[team_id][opposing_team_id]

            if row['SEASON'] not in team_stats_dict:
                # Add entry for sesason if DNE
                team_stats_dict[row['SEASON']] = {
                    'TEAM_ID': str(team_id),
                    'SEASON': row['SEASON'],
                    'GAMES_PLAYED': 0,
                    'GAMES_WON': 0
                }
            team_stats_dict = team_stats_dict[row['SEASON']]

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

    def add_combined_current_values_to_df(self, input_row):
        """Add row to dataframe with values of current dict val"""

        home_team_id = input_row['HOME_TEAM_ID']
        away_team_id = input_row['VISITOR_TEAM_ID']
        season = input_row['SEASON']

        row = {}
        exclude = {'sum_FG_PCT_total','sum_FT_PCT_total','sum_FG3_PCT_total','AT_HOME','GAMES_WON'}
        shared = {'DATE','GAME_ID','SEASON_TYPE','SEASON'}

        home_entry_exists = home_team_id in self.cumulative_stats and \
                            away_team_id in self.cumulative_stats[home_team_id] and \
                            season in self.cumulative_stats[home_team_id][away_team_id]
        away_entry_exists = away_team_id in self.cumulative_stats and \
                            home_team_id in self.cumulative_stats[away_team_id] and \
                            season in self.cumulative_stats[away_team_id][home_team_id]

        if home_entry_exists:
            for k_home in self.cumulative_stats[home_team_id][away_team_id][season]:
                if k_home not in exclude.union(shared):
                    row[k_home+'_home'] = \
                        self.cumulative_stats[home_team_id][away_team_id][season][k_home]
        else:
            for k_home in self.stats_df_columns:
                if k_home not in exclude.union(shared):
                    row[k_home+'_home'] = 0
                row['TEAM_ID_home'] = home_team_id
                row['W_PCT_home'] = 0
                row['GAMES_PLAYED_home'] = 0

        if away_entry_exists:
            for k_away in self.cumulative_stats[away_team_id][home_team_id][season]:
                if k_away not in exclude.union(shared):
                    row[k_away+'_away'] = \
                        self.cumulative_stats[away_team_id][home_team_id][season][k_away]
        else:
            for k_away in self.stats_df_columns:
                if k_away not in exclude.union(shared):
                    row[k_away+'_away'] = 0
                row['TEAM_ID_away'] = away_team_id
                row['W_PCT_away'] = 0
                row['GAMES_PLAYED_away'] = 0

        # Shared vals
        row['GAME_ID'] = input_row['GAME_ID']
        row['SEASON_TYPE'] = str(row['GAME_ID'])[0]
        row['DATE'] = input_row['GAME_DATE_EST']
        row['SEASON'] = input_row['SEASON']

        self.stats_df = self.stats_df.append(
            row,
            ignore_index=True,
        )

def main():
    """Main function"""

    combined_dataset_builder = CombinedDatasetBuilder()

    rows=0
    print('Start...')
    for _, row in combined_dataset_builder.games_df.iterrows():
        combined_dataset_builder.add_combined_current_values_to_df(row)
        combined_dataset_builder.accumulate_values(row)
        rows+=1
        # if rows%300 == 0:
        #     break
        if rows%1000 == 0:
            print(f'Row {rows}...')
    combined_dataset_builder.save_to_csv()

if __name__=='__main__':
    main()
