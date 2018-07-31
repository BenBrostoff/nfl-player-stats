from os import environ
import mysql.connector
from mysql.connector import errorcode

try:
  cnx = mysql.connector.connect(
      user=environ.get('NFL_DB_USER'),
      database=environ.get('NFL_DB_DATABASE'),
      password=environ.get('NFL_DB_PASS'),
      host=environ.get('NFL_DB_HOST'),
  )
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    print('Connection success.')

cursor = cnx.cursor()

INSERT_STATEMENT = """
INSERT INTO GAMELOGS
(kick_return_attempts,
passing_rating,
game_won,
passing_interceptions,
receiving_yards,
date,
punt_return_attempts,
passing_attempts,
receiving_receptions,
defense_safeties,
passing_yards,
defense_tackles,
passing_sacks,
kick_return_yards,
opponent,
point_after_attemps,
punt_return_yards,
defense_interception_yards,
passing_completions,
receiving_targets,
defense_interceptions,
game_number,
punting_yards,
opponent_score,
defense_tackle_assists,
punt_return_touchdowns,
year,
player_id,
game_location,
rushing_touchdowns,
punting_attempts,
passing_sacks_yards_lost,
defense_sacks,
rushing_attempts,
punting_blocked,
field_goal_attempts,
team,
point_after_makes,
field_goal_makes,
kick_return_touchdowns,
game_id,
passing_touchdowns,
receiving_touchdowns,
player_team_score,
rushing_yards,
defense_interception_touchdowns,
age,
name,
position
)
VALUES (%(kick_return_attempts)s,
%(passing_rating)s,
%(game_won)s,
%(passing_interceptions)s,
%(receiving_yards)s,
%(date)s,
%(punt_return_attempts)s,
%(passing_attempts)s,
%(receiving_receptions)s,
%(defense_safeties)s,
%(passing_yards)s,
%(defense_tackles)s,
%(passing_sacks)s,
%(kick_return_yards)s,
%(opponent)s,
%(point_after_attemps)s,
%(punt_return_yards)s,
%(defense_interception_yards)s,
%(passing_completions)s,
%(receiving_targets)s,
%(defense_interceptions)s,
%(game_number)s,
%(punting_yards)s,
%(opponent_score)s,
%(defense_tackle_assists)s,
%(punt_return_touchdowns)s,
%(year)s,
%(player_id)s,
%(game_location)s,
%(rushing_touchdowns)s,
%(punting_attempts)s,
%(passing_sacks_yards_lost)s,
%(defense_sacks)s,
%(rushing_attempts)s,
%(punting_blocked)s,
%(field_goal_attempts)s,
%(team)s,
%(point_after_makes)s,
%(field_goal_makes)s,
%(kick_return_touchdowns)s,
%(game_id)s,
%(passing_touchdowns)s,
%(receiving_touchdowns)s,
%(player_team_score)s,
%(rushing_yards)s,
%(defense_interception_touchdowns)s,
%(age)s,
%(name)s,
%(position)s
)
"""


def write_row(row, profile):
    stringified_row = {}
    for k, v in row.items():
        stringified_row[k] = str(v)

    stringified_row['name'] = str(profile['name'])
    stringified_row['position'] = str(profile['position'])

    cursor.execute(INSERT_STATEMENT, stringified_row)
    cnx.commit()


def close():
    cursor.close()
    cnx.close()
