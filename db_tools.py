import psycopg2
import psycopg2.extras
from datetime import datetime, date


def get_cursor(cursor_type=None):
    conn = psycopg2.connect(database="seth", user="seth", password="abc123",
                            host="localhost", port="5432")
    if cursor_type == 'dict':
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    else:
        cur = conn.cursor()

    return cur, conn


def get_teams(cur):
    query = """SELECT * FROM raw_teams"""
    cur.execute(query)
    rows = cur.fetchall()

    return rows


def format_strings(string):
    return string.replace("'", "&apos;")


def team_exists(team, col='ncaa', cur=None):
    if cur is None:
        cur, conn = get_cursor()

    query = """SELECT %s
            FROM raw_teams
            WHERE %s = '%s'
            """ % (col, col, format_strings(team))

    cur.execute(query)
    result = cur.fetchone()
    if result is None:
        return False
    else:
        return True


def list_tables(cur):
    cur.execute("""select relname from pg_class where
                relkind='r' and relname !~ '^(pg_|sql_)';""")

    return cur.fetchall()


def alter_table(cur, table_name):
    add_col = """ALTER TABLE %s
                 ADD COLUMN pts int NOT NULL
              """ % table_name
    cur.execute(add_col)


def store_box_row(row_dict):
    header_dict = {'Player': 'team', 'MP': 'mp', 'FGM': 'fgm',
                   'FGA': 'fga', '3FG': 'tpm', '3FGA': 'tpa', 'FT': 'ftm',
                   'FTA': 'fta', 'PTS': 'pts', 'ORebs': 'oreb',
                   'DRebs': 'dreb', 'Tot Reb': 'reb', 'AST': 'ast',
                   'TO': 'turnover', 'STL': 'stl', 'BLK': 'blk',
                   'Fouls': 'pf'}

    for category in row_dict:
        insert = """INSERT INTO box_stats

                """


def insert_values(table_name, val_dict, cur=None):
    if cur is None:
        cur, conn = get_cursor()

    keys = val_dict.keys()
    values = [val_dict[k] for k in keys]
    sqlCommand = 'INSERT INTO {table} ({keys}) VALUES ({placeholders});'.format(
      table = table_name,
      keys = ', '.join(keys),
      placeholders = ', '.join([ "%s" for v in values ])  # extra quotes may not be necessary
    )

    cur.execute(sqlCommand,values)


def get_team_id(team, cur=None, col1 = 'ncaaid', col2='ncaa'):
    if cur is None:
        cur, conn = get_cursor()

    team = format_strings(team)
    query = """SELECT %s
               FROM raw_teams
               WHERE %s = '%s'
            """ % (col2, col1, team)

    cur.execute(query)
    result = cur.fetchone()
    if result is None:
        return None
    else:
        return result[0]

def create_table(cur, table_name, query, drop=False):
    if drop:
        cur.execute("""DROP TABLE IF EXISTS '%s';""" % table_name)

    cur.execute(query)

def create_tables():
    #print list_tables(cur)

    #cur.execute("""DROP TABLE IF EXISTS box_stats;""")
    #cur.execute("""DROP TABLE IF EXISTS games;""")
    q_dict = {}

    q_dict['games'] = """CREATE TABLE games
    (
        ID SERIAL PRIMARY KEY  NOT NULL,
        home_team text         NOT NULL,
        away_team text         NOT NULL,
        home_outcome text      NOT NULL,
        home_score int         NOT NULL,
        away_score INT         NOT NULL,
        home_first int         NOT NULL,
        away_first int         NOT NULL,
        home_second int        NOT NULL,
        away_second int        NOT NULL,
        neutral_site BOOLEAN   NOT NULL,
        officials text                 ,
        attendance INT                 ,
        venue text                     ,
        dt DATE                NOT NULL,
        UNIQUE(home_team, away_team, dt)
    );"""
    q_dict['box_stats'] = """CREATE TABLE box_stats
    (
        ID SERIAL PRIMARY KEY     NOT NULL,
        gameid int NOT NULL REFERENCES games(ID),
        teamid text            NOT NULL,
        pts INT                NOT NULL,
        mp int                 NOT NULL,
        fgm int                        ,
        fga int                        ,
        tpm int                        ,
        tpa int                        ,
        ftm int                        ,
        fta int                        ,
        oreb int                       ,
        dreb int                       ,
        reb int                        ,
        ast int                        ,
        stl int                        ,
        blk int                        ,
        turnover int                   ,
        pf int                          
    );"""
    q_dict['teams'] = """CREATE TABLE teams
    (
        ncaaid int PRIMARY KEY NOT NULL,
        statsheet text                 ,
        ncaa text                      ,
        kenpom text                    ,
        espn text                      ,
        cbs1 text                      ,
        cbs2 text                        
    );"""
    
    return q_dict


def main():
    cur, conn = get_cursor()
    #insert_game(cur,this_game)
    #create_tables(cur)
    q_dict = create_tables()
    create_table(cur, 'teams', q_dict['teams'])
    #team_exists(cur, 'Indiana')
    conn.commit()
    conn.close()

if __name__ == '__main__':
      main()