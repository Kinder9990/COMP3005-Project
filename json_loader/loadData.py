import psycopg2
import json
import os

conn = psycopg2.connect(
    dbname="statsbomb",
    user="postgres",
    password="example",
    host="localhost",
    port="5433"
)

def should_insert(entry):
    laLiga = ['2020/2021', '2019/2020', '2018/2019']
    if (entry['competition_name'] == 'La Liga' and entry['season_name'] in laLiga) or \
       (entry['competition_name'] == 'Premier League' and entry['season_name'] == '2003/2004'):
        return True
    return False


def insert_manager(cursor, manager_data):
    insert_country(cursor, manager_data['country']['id'], manager_data['country']['name'])
    cursor.execute('''
        INSERT INTO manager (id, name, dob, country_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    ''', (manager_data['id'], manager_data['name'], manager_data['dob'], manager_data['country']['id']))

def insert_stadium(cursor, stadium_data):
    if stadium_data :
        insert_country(cursor, stadium_data['country']['id'], stadium_data['country']['name'])
        cursor.execute('''
            INSERT INTO stadium (id, name, country_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        ''', (stadium_data['id'], stadium_data['name'], stadium_data['country']['id']))

def insert_referee(cursor, referee_data):
    if referee_data :
        insert_country(cursor, referee_data['country']['id'], referee_data['country']['name'])
        cursor.execute('''
            INSERT INTO referee (id, name, country_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        ''', (referee_data['id'], referee_data['name'], referee_data['country']['id']))

def insert_home_team(cursor, team_data):
    insert_country(cursor, team_data['country']['id'], team_data['country']['name'])
    if 'managers' in team_data and team_data['managers']:
        insert_manager(cursor, team_data['managers'][0])
    cursor.execute('''
        INSERT INTO team (id, name, gender, country_id, manager_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    ''', (team_data['home_team_id'], team_data['home_team_name'], team_data['home_team_gender'], team_data['country']['id'],team_data.get('managers', [{}])[0].get('id')))

def insert_away_team(cursor, team_data):
    insert_country(cursor, team_data['country']['id'], team_data['country']['name'])
    if 'managers' in team_data and team_data['managers']:
        insert_manager(cursor, team_data['managers'][0])
    cursor.execute('''
        INSERT INTO team (id, name, gender, country_id, manager_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    ''', (team_data['away_team_id'], team_data['away_team_name'], team_data['away_team_gender'], team_data['country']['id'], team_data.get('managers', [{}])[0].get('id')))

def insert_country(cursor, country_id, country_name):
    cursor.execute('''
        INSERT INTO country (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING;
    ''', (country_id, country_name))


def insert_competition_stage(cursor, stage_data):
    cursor.execute('''
        INSERT INTO competition_stage (id, name)
        VALUES (%s, %s)
        ON CONFLICT (id) DO NOTHING;
    ''', (stage_data['id'], stage_data['name']))

cursor = conn.cursor()

with open('./data/competitions.json') as f:
    data = json.load(f)

competition_season_list = []
for entry in data:
    if should_insert(entry):
        season_id = entry.get('season_id')
        season_name = entry.get('season_name')

        if season_id is not None and season_name is not None:
            cursor.execute('''
                INSERT INTO seasons 
                (season_id, name) 
                VALUES (%s, %s) ON CONFLICT (season_id) DO NOTHING
            ''', (season_id, season_name))

            cursor.execute('''
                INSERT INTO competitions 
                (competition_id, season_id, competition_name, competition_gender, country_name) 
                VALUES (%s, %s, %s, %s, %s)
            ''', (
                entry['competition_id'],
                season_id,
                entry['competition_name'],
                entry['competition_gender'],
                entry['country_name']
            ))
            competition_season_list.append((entry['competition_id'], season_id))
        else:
            print("Warning: Missing season_id or season_name in entry:", entry)
        
for ids in competition_season_list:
    competition_id, season_id = ids
    file_path = f"./data/matches/{competition_id}/{season_id}.json"
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            for entry in data:
                events_path = f"./data/events/{entry['match_id']}.json"
                if os.path.exists(events_path):
                    with open(events_path, "r", encoding="utf-8") as event_file:
                        referee_id = entry['referee']['id'] if 'referee' in entry else None
                        stadium_id = entry['stadium']['id'] if 'stadium' in entry else None
                        event_file_data = json.load(event_file)
                        insert_stadium(cursor, entry.get('stadium', {}))
                        insert_referee(cursor, entry.get('referee', {}))
                        insert_home_team(cursor, entry['home_team'])
                        insert_away_team(cursor, entry['away_team'])
                        insert_competition_stage(cursor, entry['competition_stage'])
                        cursor.execute('''
                            INSERT INTO matches 
                            (match_id, competition_id, season_id, match_date, kick_off, home_team_id,away_team_id,
                            stadium_id, referee_id, home_score, away_score, match_week, competition_stage_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        ''', (
                            entry['match_id'],
                            competition_id,
                            season_id,
                            entry['match_date'],
                            entry['kick_off'],
                            entry['home_team']['home_team_id'],
                            entry['away_team']['away_team_id'],
                            stadium_id,
                            referee_id,
                            entry['home_score'],
                            entry['away_score'],
                            entry['match_week'],
                            entry['competition_stage']['id'],
                        ))
                        for event_data in event_file_data:
                            cursor.execute('''
                            INSERT INTO match_events 
                            (id, index, period, timestamp, minute, second, type, possession, possession_details, play_pattern, team, player, position, location, duration, under_pressure, off_camera, out, related_events, tactics, fifty_fifty, bad_behaviour, ball_receipt, ball_recovery, block, carry, clearance, dribble, dribbled_past, duel, foul_committed, foul_won, goalkeeper, half_end, injury_stoppage, interception, miscontrol, pass, player_off, pressure, shot, substitution, match_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        ''', (
                            event_data['id'],
                            event_data['index'],
                            event_data['period'],
                            event_data['timestamp'],
                            event_data['minute'],
                            event_data['second'],
                            json.dumps(event_data.get('type')), 
                            event_data['possession'],
                            json.dumps(event_data.get('possession_team')),  
                            json.dumps(event_data.get('play_pattern')), 
                            json.dumps(event_data.get('team')),  
                            json.dumps(event_data.get('player')), 
                            json.dumps(event_data.get('position')), 
                            json.dumps(event_data.get('location')),  
                            event_data.get('duration'),
                            event_data.get('under_pressure'),
                            event_data.get('off_camera'),
                            event_data.get('out'),
                            json.dumps(event_data.get('related_events')), 
                            json.dumps(event_data.get('tactics')),  
                            json.dumps(event_data.get('50-50')), 
                            json.dumps(event_data.get('bad_behaviour')),  
                            json.dumps(event_data.get('ball_receipt')),                            
                            json.dumps(event_data.get('ball_recovery')), 
                            json.dumps(event_data.get('block')),  
                            json.dumps(event_data.get('carry')),  
                            json.dumps(event_data.get('clearance')),  
                            json.dumps(event_data.get('dribble')), 
                            json.dumps(event_data.get('dribbled_past')), 
                            json.dumps(event_data.get('duel')), 
                            json.dumps(event_data.get('foul_committed')), 
                            json.dumps(event_data.get('foul_won')),  
                            json.dumps(event_data.get('goalkeeper')),  
                            json.dumps(event_data.get('half_end')),  
                            json.dumps(event_data.get('injury_stoppage')), 
                            json.dumps(event_data.get('interception')),  
                            json.dumps(event_data.get('miscontrol')), 
                            json.dumps(event_data.get('pass')), 
                            json.dumps(event_data.get('player_off')),  
                            json.dumps(event_data.get('pressure')),  
                            json.dumps(event_data.get('shot')), 
                            json.dumps(event_data.get('substitution')),
                            entry['match_id']
                        ))
                else:
                    print(f"File {events_path} not found.")

    except FileNotFoundError:
        print(f"File {file_path} not found.")

conn.commit()
conn.close()
