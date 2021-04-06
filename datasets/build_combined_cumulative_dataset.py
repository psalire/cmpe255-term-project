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

    def add_combined_current_values_to_df(self, home_team_id, away_team_id, season):
        """Add row to dataframe with values of current dict val"""

        row = {}
        exclude = {'sum_FG_PCT_total','sum_FT_PCT_total','sum_FG3_PCT_total','AT_HOME'}
        shared = {'DATE','GAME_ID','SEASON_TYPE','SEASON'}
        for k_home in self.cumulative_stats[home_team_id][season]:
            if k_home not in exclude.union(shared):
                row[k_home+'_home'] = self.cumulative_stats[home_team_id][season][k_home]
        for k_away in self.cumulative_stats[away_team_id][season]:
            if k_away not in exclude.union(shared):
                row[k_away+'_away'] = self.cumulative_stats[away_team_id][season][k_away]
        for k_shared in shared:
            assert self.cumulative_stats[away_team_id][season][k_shared] == \
                    self.cumulative_stats[home_team_id][season][k_shared]
            row[k_shared] = self.cumulative_stats[away_team_id][season][k_shared]
        # row['HOME_TEAM_ID'] = home_team_id
        # row['VISITOR_TEAM_ID'] = away_team_id

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
        combined_dataset_builder.accumulate_values(row)
        combined_dataset_builder.add_combined_current_values_to_df(
            row['HOME_TEAM_ID'],
            row['VISITOR_TEAM_ID'],
            row['SEASON']
        )
        rows+=1
        if rows%1000 == 0:
            print(f'Row {rows}...')
            # dataset_builder.save_to_csv()
    combined_dataset_builder.save_to_csv()

if __name__=='__main__':
    main()
