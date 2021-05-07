"""Build cumulative player stats dataset from games_details.csv"""

# import re
from multiprocessing import Process, Lock, Manager
import pandas as pd
import data as Local ## Local module to get dataframes

DATASETS_DIR = '../datasets/'

class DatasetBuilder:
    """Class to build cumulative player stats dataframe from games_details.csv"""

    def __init__(self, datasets_dir=DATASETS_DIR, out_filename='out'):
        self.player_stats_df = Local.get_games_details_dataframe()
        del self.player_stats_df['START_POSITION']

        self.avg_cols = {
            'FG_PCT_total','FT_PCT_total','FG3_PCT_total',
            'PPG_total','APG_total','RPG_total',
            # 'MPG_total',
            'DRPG_total','ORPG_total', 'BPG_total', 'SPG_total'
        }
        self.countable_cols = {
            'GAMES_PLAYED','PTS_total','AST_total','REB_total',
            # 'MIN_total',
            'FGA_total','FGM_total','FTA_total','FTM_total',
            'FG3A_total','FG3M_total','OREB_total','DREB_total',
            'STL_total','BLK_total',
        }
        self.stats_df_columns = \
            ['SEASON','GAME_ID','PLAYER_ID','TEAM_ID','PLAYER_NAME','TEAM_CITY']+ \
            list(self.countable_cols) + \
            list(self.avg_cols)
        self.stats_df = pd.DataFrame(columns=self.stats_df_columns)
        self.cumulative_stats = {}
        # self.minutes_re = re.compile(r'^(\d+)(?::(\d+))?')

        self.datasets_dir = datasets_dir
        self.out_filename = out_filename

    def accumulate_values(self, row):
        """Add values to existing dict val, or add dict if DNE"""

        # Initialize if dne
        if row['PLAYER_ID'] not in self.cumulative_stats:
            self.cumulative_stats[row['PLAYER_ID']] = {}
        if row['SEASON'] not in self.cumulative_stats:
            self.cumulative_stats[row['PLAYER_ID']][row['SEASON']] = {
                'TEAM_ID': str(row['TEAM_ID']),
                'TEAM_CITY': row['TEAM_CITY'],
                'PLAYER_NAME': row['PLAYER_NAME'],
                'SEASON': row['SEASON'],
            }
            for k in self.countable_cols:
                self.cumulative_stats[row['PLAYER_ID']][row['SEASON']][k] = 0

        player_stats_dict = self.cumulative_stats[row['PLAYER_ID']][row['SEASON']]

        if str(row['MIN'])=='nan':
            return

        for k_col in self.countable_cols.difference({
            'GAMES_PLAYED',
            # 'MIN_total',
        }):
            player_stats_dict[k_col] += float(row[k_col.replace('_total','')])
        player_stats_dict['GAMES_PLAYED'] += 1

        # m_minutes = self.minutes_re.search(row['MIN'])
        # if m_minutes is None:
        #     print('no match:',row['MIN'])
        # seconds = m_minutes.group(2)
        # player_stats_dict['MIN_total'] = int(m_minutes.group(1))
        # if seconds and int(seconds) >= 30:
        #     player_stats_dict['MIN_total'] += 1

        if player_stats_dict['FGA_total']>0:
            player_stats_dict['FG_PCT_total'] = \
                player_stats_dict['FGM_total']/player_stats_dict['FGA_total']
        else:
            player_stats_dict['FG_PCT_total'] = 0
        if player_stats_dict['FG3A_total']>0:
            player_stats_dict['FG3_PCT_total'] = \
                player_stats_dict['FG3M_total']/player_stats_dict['FG3A_total']
        else:
            player_stats_dict['FG3_PCT_total'] = 0
        if player_stats_dict['FTA_total']>0:
            player_stats_dict['FT_PCT_total'] = \
                player_stats_dict['FTM_total']/player_stats_dict['FTA_total']
        else:
            player_stats_dict['FT_PCT_total'] = 0
        for k_avg, k_add in [
            ('RPG_total', 'REB_total'),
            ('ORPG_total', 'OREB_total'),
            ('DRPG_total', 'DREB_total'),
            ('PPG_total', 'PTS_total'),
            ('BPG_total', 'BLK_total'),
            ('SPG_total', 'STL_total'),
            # ('MPG_total', 'MIN_total'),
            ('APG_total', 'AST_total'),
        ]:
            player_stats_dict[k_avg] = player_stats_dict[k_add]/player_stats_dict['GAMES_PLAYED']

    def add_current_values_to_df(self, row):
        """Add row to dataframe with values of current dict val"""

        season = row['SEASON']
        player_id = row['PLAYER_ID']
        if player_id in self.cumulative_stats and season in self.cumulative_stats[player_id]:
            current_vals = {
                k:self.cumulative_stats[player_id][season][k] \
                for k in set(self.cumulative_stats[player_id][season].keys()).difference(
                    {'GAMES_WON','sum_FG_PCT_total','sum_FT_PCT_total','sum_FG3_PCT_total'}
                )
            }
        else:
            # If values dne, i.e. first game of season
            current_vals = {k:0 for k in self.stats_df_columns}
            current_vals['GAMES_PLAYED'] = 0

        # Use metadata of current game, not previous
        current_vals['GAME_ID'] = row['GAME_ID']
        current_vals['PLAYER_ID'] = player_id
        current_vals['PLAYER_NAME'] = row['PLAYER_NAME']
        current_vals['TEAM_CITY'] = row['TEAM_CITY']
        current_vals['TEAM_ID'] = row['TEAM_ID']
        current_vals['DATE'] = row['DATE']
        current_vals['SEASON'] = season

        self.stats_df = self.stats_df.append(
            current_vals,
            ignore_index=True,
        )

    def save_to_csv(self):
        """Save dataframe to out_filename.csv"""

        self.stats_df.to_csv(self.datasets_dir+self.out_filename+'.csv', index=False)

def build_cumulative_dataframe(i, lock, return_arr, df):
    '''Build cumulative stats dataframe from df'''

    dataset_builder = DatasetBuilder()
    rows = 0
    for _, row in df.iterrows():
        dataset_builder.add_current_values_to_df(row)
        dataset_builder.accumulate_values(row)
        rows+=1
        # if rows%1600 == 0:
        #     break
        if rows%1000 == 0:
            print(f'{i}: Row {rows}...')
    lock.acquire()
    return_arr.append(dataset_builder)
    lock.release()

def main():
    """Main function"""

    dataset = DatasetBuilder().player_stats_df
    procs = []

    manager = Manager()
    df_per_season = manager.list()

    max_procs = 3
    lock = Lock()

    for i, year in enumerate([*range(2003,2021)]):
        print(year)
        procs.append(
            Process(
                target=build_cumulative_dataframe,
                args=(i, lock, df_per_season, dataset[dataset['SEASON']==year],)
            )
        )
        procs[-1].start()
        if len(procs)==max_procs:
            for proc in procs:
                proc.join()
            procs.clear()

    for proc in procs:
        proc.join()

    out_df = df_per_season[0].stats_df
    for i in range(1,len(df_per_season)):
        out_df = out_df.append(df_per_season[i].stats_df)
    out_df.sort_values(by='DATE', inplace=True)
    out_df.to_csv(df_per_season[i].datasets_dir+df_per_season[i].out_filename+'.csv', index=False)

if __name__=='__main__':
    main()
