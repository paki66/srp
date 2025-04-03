import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, BigInteger
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

CSV_FILE_PATH = 'NFL_dataset80_processed.csv'

df = pd.read_csv(CSV_FILE_PATH, delimiter=',')

Base = declarative_base()


# DDL shema baze
# -------------------------------------
class Season(Base):
    __tablename__ = 'season'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, unique=True)


class Game(Base):
    __tablename__ = 'game'
    id = Column(Integer, primary_key=True)
    season_fk = Column(Integer, ForeignKey('season.id'))
    home_team = Column(String)
    away_team = Column(String)


class Quarter(Base):
    __tablename__ = 'quarter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, ForeignKey('game.id'))
    number = Column(Integer)


class Offense(Base):
    __tablename__ = 'offense'
    id = Column(Integer, primary_key=True, autoincrement=True)
    score = Column(Float)
    team_name = Column(String)


class Defense(Base):
    __tablename__ = 'defense'
    id = Column(Integer, primary_key=True, autoincrement=True)
    score = Column(Float)
    team_name = Column(String)


class PlayType(Base):
    __tablename__ = 'play_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)


class Plays(Base):
    __tablename__ = 'plays'
    id = Column(Integer, primary_key=True, autoincrement=True)
    play_type_fk = Column(Integer, ForeignKey('play_type.id'))
    offense_fk = Column(BigInteger, ForeignKey('offense.id'))
    defense_fk = Column(BigInteger, ForeignKey('defense.id'))
    quarter_fk = Column(Integer, ForeignKey('quarter.id'))
    down = Column(Float)
    yards_to_go = Column(Integer)
    side_of_field = Column(String)
    yard_line = Column(Float)
    time_under = Column(Integer)
    yards_gained = Column(Integer)


# kreiranje konekcije na bazu
engine = create_engine("postgresql://paolo_srp:paolo_srp@localhost/srp_db", echo=False)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# kreiranje sesije
Session = sessionmaker(bind=engine)
session = Session()


# Umetanje sezona
seasons = df[['year']].drop_duplicates()
seasons_list = seasons.to_dict(orient='records')

session.bulk_insert_mappings(Season, seasons_list)
session.commit()

season_map = {s.year: s.id for s in
              session.query(Season).all()}


# Umetanje utakmica
games = df[['game_id', 'year', 'home_team', 'away_team']].drop_duplicates().rename(
    columns={'game_id': 'id'})

games['season_fk'] = games['year'].map(season_map)
games = games.drop(columns=['year'])

games_list = games.to_dict(orient='records')

session.bulk_insert_mappings(Game, games_list)
session.commit()


# Umetanje četvrtina
quarters = df[['quarter', 'game_id']].drop_duplicates().rename(
    columns={'quarter': 'number'})
quarters_list = quarters.to_dict(orient='records')

session.bulk_insert_mappings(Quarter, quarters_list)
session.commit()

quarter_map = {(q.game_id, q.number): q.id for q in
               session.query(Quarter).all()}


# Umetanje napadačkih timova
offenses = df[['offense_team', 'offense_team_score']].drop_duplicates().rename(
    columns={'offense_team': 'team_name', 'offense_team_score': 'score'})
offenses_list = offenses.to_dict(orient='records')

session.bulk_insert_mappings(Offense, offenses_list)
session.commit()

offense_map = {(o.team_name, o.score): o.id for o in
                session.query(Offense).all()}


# Umetanje obrambenih timova
defenses = df[['defense_team', 'defense_team_score']].drop_duplicates().rename(
    columns={'defense_team': 'team_name', 'defense_team_score': 'score'})
defenses_list = defenses.to_dict(orient='records')

session.bulk_insert_mappings(Defense, defenses_list)
session.commit()

defense_map = {(d.team_name, d.score): d.id for d in
               session.query(Defense).all()}


# Umetanje tipova akcija
play_types = df[['play_type']].drop_duplicates().rename(
    columns={'play_type': 'name'})
play_types_list = play_types.to_dict(orient='records')

session.bulk_insert_mappings(PlayType, play_types_list)
session.commit()

play_type_map = {pt.name: pt.id for pt in
                 session.query(PlayType).all()}


# Umetanje akcija
plays_data = df[['yards_gained', 'down', 'yards_to_go', 'side_of_field', 'yard_line',
                 'time_under', 'play_type', 'defense_team', 'defense_team_score',
                 'offense_team_score', 'offense_team', 'quarter', 'game_id']].copy()

plays_data['play_type_fk'] = plays_data['play_type'].map(play_type_map)

plays_data['offense_fk'] = [
    offense_map.get((team, score), None)
    for team, score in zip(plays_data['offense_team'], plays_data['offense_team_score'])
]

# plays_data["offense_fk"] = plays_data["offense_fk"].astype(int)

plays_data['defense_fk'] = [
    defense_map.get((team, score), None)
    for team, score in zip(plays_data['defense_team'], plays_data['defense_team_score'])
]

# plays_data["defense_fk"] = plays_data["defense_fk"].astype(int)

plays_data['quarter_fk'] = [
    quarter_map.get((game_id, quarter), None)
    for game_id, quarter in zip(plays_data['game_id'], plays_data['quarter'])
]


plays_list = plays_data.drop(columns=['play_type', 'defense_team', 'defense_team_score', 'offense_team_score',
                                      'offense_team', 'quarter', 'game_id']).to_dict(orient='records')
session.bulk_insert_mappings(Plays, plays_list)
session.commit()

print("Data imported successfully!")
