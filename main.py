from pymongo import MongoClient
from datetime import datetime
import csv

# MongoDB Connection
client = MongoClient('mongodb://localhost:27017/')
db = client['world_cup_db']  # Replace with your database name

# Define the COUNTRY document schema
def create_country_document(country_data, player_data, world_cup_history):
    try:
        population = float(country_data['Population'])  # Convert population to float
    except ValueError:
        # Handle the error or set a default value
        population = None  # or use 0, or log an error, etc.

    return {
        "Country_Name": country_data['Country_Name'],
        "Capital": country_data['Capital'],
        "Population": population,
        "Manager": country_data['Manager'],
        "Players": player_data,
        "WorldCupWonHistory": world_cup_history
    }

# Define the STADIUM document schema
def create_stadium_document(stadium_name, city, matches):
    return {
        "Stadium": stadium_name,
        "City": city,
        "Matches": matches
    }

# Read and process player data
def read_player_data():
    players = {}
    # Read basic player information
    with open('C://Users//onanuga_d54306//Documents//COSC_6315//Project_2//Players.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            # Convert Height to integer
            try:
                row['Height'] = int(row['Height'])
            except ValueError:
                # Handle the error or set a default value
                row['Height'] = 0  # or use None, or log an error, etc.

            # Initialize cards data with default values
            row['No_of_Yellow_cards'] = 0
            row['No_of_Red_cards'] = 0

            # Convert Is-captain to boolean
            row['Is_captain'] = row['Is_captain'].lower() == 'true'

            players[row['Player_id']] = row

    # Add assists, goals, and card information
    with open('C://Users//onanuga_d54306//Documents//COSC_6315//Project_2//Player_Assists_Goals.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if row['Player_id'] in players:
                # Convert Goals to integer
                try:
                    row['Goals'] = int(row['Goals'])
                except ValueError:
                    row['Goals'] = 0

                players[row['Player_id']].update(row)

    with open('C://Users//onanuga_d54306//Documents//COSC_6315//Project_2//Player_Cards.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if row['Player_id'] in players:
                # Convert No_of_Yellow_cards and No_of_Red_cards to integers
                try:
                    players[row['Player_id']]['No_of_Yellow_cards'] = int(row['No_of_Yellow_cards'])
                    players[row['Player_id']]['No_of_Red_cards'] = int(row['No_of_Red_cards'])
                except ValueError:
                    # If conversion fails, keep the default values
                    pass

    return list(players.values())

# Read World Cup history data and create a mapping of winners
def read_worldcup_winners():
    winners = {}
    with open('C://Users//onanuga_d54306//Documents//COSC_6315//Project_2//Worldcup_History.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            winner = row['Winner']
            winners[winner] = winners.get(winner, []) + [{"Year": row['Year'], "Host": row['Host']}]
    return winners

# Read country data
def read_country_data():
    countries = []
    player_data = read_player_data()
    worldcup_winners = read_worldcup_winners()

    with open('C://Users//onanuga_d54306//Documents//COSC_6315//Project_2//Country.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            country_players = [player for player in player_data if player['Country'] == row['Country_Name']]
            country_wins = worldcup_winners.get(row['Country_Name'], [])
            if len(country_wins) == int(row['No_of_Worldcup_won']):
                country = create_country_document(row, country_players, country_wins)
                countries.append(country)

    return countries

# Read stadium data and match results
def read_stadium_data():
    stadiums = {}
    with open('C://Users//onanuga_d54306//Documents//COSC_6315//Project_2//Match_results.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            stadium_name = row['Stadium']
            if stadium_name not in stadiums:
                stadiums[stadium_name] = {
                    "Stadium": stadium_name,
                    "City": row['Host_city'],  # Assuming city is in Match_results.csv
                    "Matches": []
                }

            # Convert Team1_score and Team2_score to integers
            try:
                row['Team1_score'] = int(row['Team1_score'])
                row['Team2_score'] = int(row['Team2_score'])
            except ValueError:
                # Handle the error or set a default value
                row['Team1_score'] = 0  # or use None, or log an error, etc.
                row['Team2_score'] = 0  # or use None, or log an error, etc.

            stadiums[stadium_name]['Matches'].append(row)

    return list(stadiums.values())

# Insert data into MongoDB
def insert_data():
    country_data = read_country_data()
    stadium_data = read_stadium_data()

    db.country_collection.insert_many(country_data)
    db.stadium_collection.insert_many(stadium_data)

# Execute the data insertion
insert_data()
