from etl.transform.dimensions.game_dim import transform_game_dim
from etl.transform.dimensions.offense_dim import transform_offense_dim
from etl.transform.dimensions.defense_dim import transform_defense_dim
from etl.transform.dimensions.time_dim import transform_time_dim
from etl.transform.dimensions.position_dim import transform_position_dim
from etl.transform.dimensions.goal_dim import transform_goal_dim
from etl.transform.facts.plays_fact import transform_plays_fact


def run_transformations(raw_data):
    # Transform dimensions
    game_dim = transform_game_dim(
        raw_data["game"],
        raw_data["season"],
        csv_plays_df=raw_data.get("csv_plays")
    )
    print("1️⃣ Game dimension complete")

    offense_dim = transform_offense_dim(
        raw_data["offense"],
        csv_offense_df=raw_data.get("csv_plays")
    )
    print("2️⃣ Offense dimension complete")

    defense_dim = transform_defense_dim(
        raw_data["defense"],
        csv_defense_df=raw_data.get("csv_plays")
    )
    print("3️⃣ Defense dimension complete")

    time_dim = transform_time_dim()
    print("4️⃣ Time dimension complete")

    position_dim = transform_position_dim(
        raw_data["plays"],
        csv_plays_df=raw_data.get("csv_plays")
    )
    print("4️⃣ Position dimension complete")


    goal_dim = transform_goal_dim(
        raw_data["plays"],
        csv_plays_df=raw_data.get("csv_plays")
    )
    print("4️⃣ Goal dimension complete")

    fact_plays = transform_plays_fact(
        raw_data,
        game_dim,
        offense_dim,
        defense_dim,
        time_dim,
        position_dim,
        goal_dim
    )
    print("5️⃣ Plays fact table complete")
    return {
        "dim_game": game_dim,
        "dim_offense": offense_dim,
        "dim_defense": defense_dim,
        "dim_time": time_dim,
        "dim_position": position_dim,
        "dim_goal": goal_dim,
        "fact_plays": fact_plays
    }

