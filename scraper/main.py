
import requests
# Headers needed to pass filter
HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-origin': 'stats',
    'x-nba-stats-token': 'true',
    'Origin': 'https://www.nba.com',
    'Referer': 'https://www.nba.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0) Gecko/20100101 Firefox/87.0',
}

def get_cumulative_team_stats(game_id, team_id, season, season_type='Regular Season', league_id='00'):
    print('GET...')
    res = requests.get(
        'https://stats.nba.com/stats/cumestatsteam?'+
        f'GameIDs={game_id}&LeagueID={league_id}&Season={season}&'+
        f'SeasonType={season_type}&TeamID={team_id}',
        headers=HEADERS,
    )
    res_json = res.json()
    print(res.status_code)
    print(res_json)

def get_cumulative_player_stats(game_id, player_id, season, season_type='Regular Season', league_id='00'):
    print('GET...')
    res = requests.get(
        'https://stats.nba.com/stats/cumestatsplayer?'+
        f'GameIDs={game_id}&LeagueID={league_id}&PlayerID={player_id}&'+
        f'Season={season}&SeasonType={season_type}',
        headers=HEADERS,
    )
    res_json = res.json()
    print(res.status_code)
    print(res_json)

get_cumulative_team_stats('0012000034','1610612759','2020')
get_cumulative_player_stats('0012000034','1610612759','2020')
