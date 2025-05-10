from pyspark.sql import Window
from pyspark.sql.functions import col, upper, trim, row_number


def transform_plays_fact(
        raw_data,
        game_dim,
        offense_dim,
        defense_dim,
        time_dim,
        position_dim,
        goal_dim
):
    plays_df = raw_data["plays"]
    season_df = raw_data["season"]
    game_df = raw_data["game"]
    quarter_df = raw_data["quarter"]
    offense_df = raw_data["offense"]
    defense_df = raw_data["defense"]
    play_type = raw_data["play_type"]
    csv_plays_df = raw_data.get("csv_plays")

    enriched_psql_plays = (
        plays_df.alias("p")
        .join(quarter_df.alias("q"), col("p.quarter_fk") == col("q.id"), "left")
        .join(game_df.alias("g"), col("q.game_id") == col("g.id"), "left")
        .join(season_df.alias("s"), col("g.season_fk") == col("s.id"), "left")
        .join(offense_df.alias("o"), col("p.offense_fk") == col("o.id"), "left")
        .join(defense_df.alias("d"), col("p.defense_fk") == col("d.id"), "left")
        .join(play_type.alias("pt"), col("p.play_type_fk") == col("pt.id"), "left")
        .select(
            col("q.number").alias("quarter_number"),
            col("g.id").alias("game_id"),
            col("o.team_name").alias("offense_team_name"),
            col("o.score").alias("offense_score"),
            col("d.team_name").alias("defense_team_name"),
            col("d.score").alias("defense_score"),
            col("pt.name").alias("play_type"),
            col("yards_gained").cast("int"),
            col("down").cast("double"),
            col("yards_to_go").cast("int"),
            col("side_of_field"),
            col("yard_line").cast("double"),
            col("time_under").cast("int")
        )
    )

    if csv_plays_df:
        cleaned_csv_plays_df = (
            csv_plays_df
            .withColumn("quarter_number", col("quarter").cast("int"))
            .withColumn("game_id", col("game_id").cast("int"))
            .withColumn("offense_team_name", upper(trim(col("offense_team"))))
            .withColumn("offense_score", col("offense_team_score").cast("int"))
            .withColumn("defense_team_name", upper(trim(col("defense_team"))))
            .withColumn("defense_score", col("defense_team_score").cast("int"))
            .withColumn("play_type", upper(trim(col("play_type"))))
            .withColumn("yards_gained", col("yards_gained").cast("int"))
            .withColumn("down", col("down").cast("double"))
            .withColumn("yards_to_go", col("yards_to_go").cast("int"))
            .withColumn("side_of_field", trim(col("side_of_field")))
            .withColumn("yard_line", col("yard_line").cast("int"))
            .withColumn("time_under", col("time_under").cast("int"))
            .select("quarter_number", "game_id", "offense_team_name", "offense_score", "defense_team_name",
                    "defense_score", "play_type", "yards_gained", "down", "yards_to_go", "side_of_field",
                    "yard_line", "time_under")
        )

    else:
        cleaned_csv_plays_df = None

    combined_plays = enriched_psql_plays
    if cleaned_csv_plays_df:
        combined_plays = combined_plays.unionByName(cleaned_csv_plays_df)

    fact_df = (
        combined_plays.alias("p")
        .join(defense_dim.alias("d"), (col("p.defense_team_name") == col("d.team")) & (col("p.defense_score") == col("d.score")), "left")
        .join(offense_dim.alias("o"), (col("p.offense_team_name") == col("o.team")) & (col("p.offense_score") == col("o.score")), "left")
        .join(game_dim.alias("g"), col("p.game_id") == col("g.game_id"), "left")
        .join(time_dim.alias("t"), (col("p.time_under") == col("t.time_under")) & (col("p.quarter_number") == col("t.quarter")), "left")
        .join(position_dim.alias('ps'), (col("p.side_of_field") == col("ps.side_of_field")) & (col("p.yard_line") == col("ps.yard_line")), "left")
        .join(goal_dim.alias('gl'), (col("p.yards_to_go") == col("gl.yards_to_go")) & (col("p.down") == col("gl.down")), "left")
        .select(
            col("game_tk"),
            col("defense_tk"),
            col("offense_tk"),
            col("position_tk"),
            col("goal_tk"),
            col("time_tk"),
            col("yards_gained"),
            col("play_type")
        )
    )

    fact_df = fact_df.withColumn(
        "fact_plays_tk",
        row_number().over(Window.orderBy("game_tk", "time_tk"))
    )

    # assert fact_df.count() ==
    return fact_df
