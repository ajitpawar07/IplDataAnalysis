# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,BooleanType,DateType,DecimalType
from pyspark.sql.functions import col,sum,min,max,row_number

# COMMAND ----------

from pyspark.sql import SparkSession
#create session
spark=SparkSession.builder.appName("IPL Data Analysis").getOrCreate()


# COMMAND ----------

spark

# COMMAND ----------

ball_by_ball_df=spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .load("/FileStore/tables/Ball_By_Ball.csv")

# COMMAND ----------

display(ball_by_ball_df)

# COMMAND ----------

from pyspark.sql.functions import*
ball_by_ball_df.show()

# COMMAND ----------

ball_by_ball_schema=StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

ball_by_ball_df=spark.read.schema(ball_by_ball_schema).format("csv").option("header","true").load("/FileStore/tables/Ball_By_Ball.csv")

# COMMAND ----------

from pyspark.sql.types import*
from pyspark.sql.functions import*
from pyspark.sql.window import Window

# COMMAND ----------

match_schema=StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

match_df=spark.read.schema(match_schema).format("csv").option("header","true").load("/FileStore/tables/Match.csv")

# COMMAND ----------

players_schema= StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

players_df=spark.read.format("csv").option("header",True).load("/FileStore/tables/Player.csv")

# COMMAND ----------

player_match_schema=StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(18, 2), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

# COMMAND ----------

player_match_df=spark.read.format("csv").option("header",True).load("/FileStore/tables/Player_match.csv")

# COMMAND ----------

player_match_df.show()

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

# COMMAND ----------

team_df=spark.read.format("csv").option("header","True").load("/FileStore/tables/Team.csv")

# COMMAND ----------

team_df.show()

# COMMAND ----------

# filter to include only valid deliveries(excluding extras like wide and no balls)
from pyspark.sql.functions import col,filter
ball_by_ball_df=ball_by_ball_df.filter(((col("wides")==0))&((col("noballs")==0)))

# COMMAND ----------

ball_by_ball_df.printSchema()

# COMMAND ----------

ball_by_ball_df.show(5)

# COMMAND ----------

# Aggregration : calculate the total and average runs scored in each match and innings

total_and_avg_runs=ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("avg_runs")
).show(5)

# COMMAND ----------

# Window function: Calculate running total of each match for each over

windowSpec=Window.partitionBy("match_id","innings_no").orderBy(col("over_id"))

ball_by_ball_df=ball_by_ball_df.withColumn(
    "running_total_runs",
    sum("runs_scored").over(windowSpec)
).show(5)

# COMMAND ----------

total_runs_and_avg=ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("avg_runs")
    )
total_runs_and_avg.show(5)    

# COMMAND ----------

ball_by_ball_df=ball_by_ball_df.withColumn("total_runs",sum("runs_scored").over(
    Window.partitionBy("match_id","innings_no")))
#ball_by_ball_df.printSchema() 
ball_by_ball_df.display() 
