from pyspark.sql.functions import col

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
            col("")
        )
    )

    if csv_plays_df:
        cleaned_csv_plays_df = csv_plays_df.withColumn()

    else:
        cleaned_csv_plays_df = None

    combined_plays = enriched_psql_plays
    if cleaned_csv_plays_df:
        combined_plays = combined_plays.unionByName(cleaned_csv_plays_df)

