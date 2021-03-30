"""Use nba's api to build datasets"""

import pandas as pd
import sys
sys.path.append('../')
from datasets import data as Local ## Local module to get dataframes
from api import API
# from pprint import PrettyPrinter

DATASETS_DIR = '../datasets/'

def main():
    """Main script"""

    # Open games dataset
    games_df, _ = Local.get_games_and_winners_dataframe(DATASETS_DIR)
    output_df = Local.get_cumulative_games_stats_dataframe(DATASETS_DIR)
    # print(games_df.columns)
    print(output_df.columns)
    # pp = PrettyPrinter()
    nba_api = API()
    # nba_api.get_cumulative_team_stats(['0012000034'],'1610612759','2020')
    # pp.pprint(x['resultSets'][1])
    # nba_api.get_cumulative_player_stats(['0012000034'],'1610612759','2020')


if __name__=='__main__':
    main()
