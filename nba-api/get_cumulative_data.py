"""Use nba's api to build datasets"""

import time
import requests
import pandas as pd

class API:
    """Get jsons from nba api"""
    # Headers needed to pass filter
    __HEADERS = {
        'Accept': 'application/json, text/plain, */*',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
        'Origin': 'https://www.nba.com',
        'Referer': 'https://www.nba.com/',
    }

    def __init__(self):
        self.req = requests.Session()
        self.req.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0) Gecko/20100101 Firefox/87.0',
        })

    def __get_json(self, url, headers=None):
        """Return a json from a get request"""

        print('GET... ')
        time.sleep(1.5) ## Sleep before request to not flood api
        res = self.req.get(url, headers=headers if headers else {})
        print(res.status_code)
        # print(res.json())
        return res.json()

    def get_cumulative_team_stats(
        self, game_ids, team_id, season, season_type='Regular Season', league_id='00'
    ):
        """Call endpoint to get cumulative team stats"""

        game_ids = '|'.join([s.zfill(10) for s in game_ids])
        return self.__get_json(
            'https://stats.nba.com/stats/cumestatsteam?'+
            f'GameIDs={game_ids}&LeagueID={league_id}&Season={season}&'+
            f'SeasonType={season_type}&TeamID={team_id}',
            headers=self.__HEADERS,
        )

    def get_cumulative_player_stats(
        self, game_ids, player_id, season, season_type='Regular Season', league_id='00'
    ):
        """Call endpoint to get cumulative player stats"""

        game_ids = '|'.join([s.zfill(10) for s in game_ids])
        return self.__get_json(
            'https://stats.nba.com/stats/cumestatsplayer?'+
            f'GameIDs={game_ids}&LeagueID={league_id}&PlayerID={player_id}&'+
            f'Season={season}&SeasonType={season_type}',
            headers=self.__HEADERS,
        )

def main():
    """Main script"""

    # Open games dataset
    games_df = pd.read_csv('../datasets/games.csv')
    # print(games_df.columns)
    nba_api = API()
    nba_api.get_cumulative_team_stats(['0012000034'],'1610612759','2020')
    # nba_api.get_cumulative_player_stats(['0012000034'],'1610612759','2020')


if __name__=='__main__':
    main()
