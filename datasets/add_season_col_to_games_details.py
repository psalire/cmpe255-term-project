'''Add season column to games_details.py'''

import sys
import pandas as pd
import numpy as np
sys.path.append('../datasets/')
import data as Local ## Local module to get dataframes
DATASET_DIR = '../datasets/'

player_stats_df = Local.get_games_details_dataframe(DATASET_DIR).sort_values(by=['GAME_ID'])
games_stats_df  = Local.get_cumulative_games_stats_dataframe(DATASET_DIR)

player_stats_df.insert(0, 'SEASON', '')

print('Start...')
for i in range(len(player_stats_df)):
    player_stats_df['SEASON'].iat[i] = games_stats_df[
        games_stats_df['GAME_ID']==player_stats_df['GAME_ID'].iat[i]
    ]['SEASON'].to_string(header=False, index=False)
    if i%10000==0:
        print(i)

# print('apply()...')
# player_stats_df['SEASON'] = player_stats_df.apply(
#     lambda row: games_stats_df[games_stats_df['GAME_ID']==row['GAME_ID']]['SEASON'],
#     axis=1
# )

player_stats_df.to_csv('out.csv', index=False)

# print(player_stats_df.head(20))
# print(games_stats_df.head(20))
# print(games_stats_df[games_stats_df['GAME_ID']==10300001]['SEASON'])
