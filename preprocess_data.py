import pandas as pd

CSV_FILE_PATH = 'NFL_dataset.csv'

df = pd.read_csv(CSV_FILE_PATH, delimiter=',', low_memory=False)

df = df[['GameID', 'qtr', 'down', 'TimeUnder', 'SideofField',
         'yrdln', 'ydstogo', 'PlayType', 'Season', 'posteam',
         'DefensiveTeam', 'PosTeamScore', 'DefTeamScore',
         'Yards.Gained', 'HomeTeam', 'AwayTeam']]

df.columns = ['game_id', 'quarter', 'down', 'time_under', 'side_of_field',
              'yard_line', 'yards_to_go', 'play_type', 'year', 'offense_team',
              'defense_team', 'offense_team_score', 'defense_team_score',
              'yards_gained', 'home_team', 'away_team']

pd.set_option('display.max_columns', None)
# print(df.isna().sum())
# print(df.head())

df = df.dropna()

df20 = df.sample(frac=0.2, random_state=42)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)

df.to_csv('NFL_dataset80_processed.csv', index=False)
df20.to_csv('NFL_dataset20_processed.csv', index=False)