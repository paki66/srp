from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


DATABASE_URL = "postgresql://paolo_srp:paolo_srp@localhost/srp_db"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class DimGame(Base):
    __tablename__ = 'dim_game'
    __table_args__ = {'schema': 'public'}

    game_tk = Column(BigInteger, primary_key=True)
    home_team = Column(String)
    away_team = Column(String)
    date = Column(DateTime)
    year = Column(Integer)


class DimTime(Base):
    __tablename__ = 'dim_time'
    __table_args__ = {'schema': 'public'}

    time_tk = Column(BigInteger, primary_key=True)
    quarter = Column(Integer)
    time_under = Column(Integer)


class DimOffense(Base):
    __tablename__ = 'dim_offense'
    __table_args__ = {'schema': 'public'}

    offense_tk = Column(BigInteger, primary_key=True)
    team = Column(String)
    score = Column(Float)


class DimDefense(Base):
    __tablename__ = 'dim_defense'
    __table_args__ = {'schema': 'public'}

    defense_tk = Column(BigInteger, primary_key=True)
    team = Column(String)
    score = Column(Float)


class DimPosition(Base):
    __tablename__ = 'dim_position'
    __table_args__ = {'schema': 'public'}

    position_tk = Column(BigInteger, primary_key=True)
    side_of_field = Column(String)
    yard_line = Column(Float)


class DimGoal(Base):
    __tablename__ = 'dim_goal'
    __table_args__ = {'schema': 'public'}

    goal_tk = Column(BigInteger, primary_key=True)
    yards_to_go = Column(Integer)
    down = Column(Float)


class FactPlays(Base):
    __tablename__ = 'fact_plays'
    __table_args__ = {'schema': 'public'}

    fact_plays_tk = Column(BigInteger, primary_key=True)
    game_id = Column(Integer, ForeignKey('public.dim_game.game_tk'))
    defense_id = Column(Integer, ForeignKey('public.dim_offense.offense_tk'))
    offense_id = Column(Integer, ForeignKey('public.dim_offense.offense_tk'))
    position_id = Column(Integer, ForeignKey('public.dim_position.position_tk'))
    goal_id = Column(Integer, ForeignKey('public.dim_goal.goal_tk'))
    time_id = Column(Integer, ForeignKey('public.dim_time.time_tk'))
    yards_gained = Column(Integer)



Base.metadata.create_all(engine)

print("Dimensional model tables created successfully!")
