import unittest
import pandas as pd
import sqlalchemy
from pandas.testing import assert_frame_equal
from sqlalchemy import text

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Spajanje na bazu podataka
        self.engine = sqlalchemy.create_engine('postgresql://paolo_srp:paolo_srp@localhost/srp_db')
        self.connection = self.engine.connect()

        # Učitavanje CSV datoteke
        self.df = pd.read_csv("NFL_dataset80_processed.csv")

        # Upit na bazu koji dohvaća sve podatke u tablicama u obliku dataframe-a
        query = text("""
        SELECT g.id AS game_id
              , q.number AS quarter
              , p.down AS down
              , p.time_under AS time_under
              , p.side_of_field AS side_of_field
              , p.yard_line AS yard_line
              , p.yards_to_go AS yards_to_go
              , pt.name AS play_type
              , s.year AS year
              , o.team_name AS offense_team
              , d.team_name AS defense_team
              , o.score AS offense_team_score
              , d.score AS defense_team_score
              , p.yards_gained AS yards_gained
              , g.home_team AS home_team
              , g.away_team AS away_team
        FROM plays p
        JOIN play_type pt ON p.play_type_fk = pt.id
        JOIN offense o ON p.offense_fk = o.id
        JOIN defense d ON p.defense_fk = d.id
        JOIN quarter q ON p.quarter_fk = q.id
        JOIN game g ON q.game_id = g.id
        JOIN season s ON g.season_fk = s.id
        ORDER BY p.id ASC
        """)
        result = self.connection.execute(query) # Izvršavanje upita
        self.db_df = pd.DataFrame(result.fetchall()) # Dohvaćanje rezultata upita
        self.db_df.columns = result.keys() # Dohvaćanje naziva stupaca

    # Testiranje stupaca
    def test_columns(self):
        self.assertListEqual(list(self.df.columns), list(self.db_df.columns))

    # Testiranje podataka
    def test_dataframes(self):
        self.df = self.df.reset_index(drop=True)
        self.db_df = self.db_df.reset_index(drop=True)
        assert_frame_equal(self.df, self.db_df)

    # Zatvaranje konekcije
    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()