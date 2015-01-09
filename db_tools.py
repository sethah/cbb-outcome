import psycopg2
import psycopg2.extras


def main():
    cur, conn = get_cursor()
    team_exists(cur, 'Indiana')
    return None
    create_box_table(cur)
    # alter_table(cur, "box_stats")
    conn.commit()
    conn.close()


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


def team_exists(cur, team, col='ncaa'):
    query = """
            SELECT %s
            FROM raw_teams
            WHERE %s = '%s'
            """ % (col, col, team)

    result = cur.execute(query)
    if result is not None:
        print result.fetchone()
    else:
        print 'Team does not exist'
    # print result


def list_tables(cur):
    cur.execute("""select relname from pg_class where
                relkind='r' and relname !~ '^(pg_|sql_)';""")

    return cur.fetchall()


def alter_table(cur,table_name):
    add_col = """ALTER TABLE %s 
                 ADD COLUMN pts int NOT NULL
              """ % table_name
    cur.execute(add_col)


def create_box_table(cur):
    print list_tables(cur)
    create_games = """CREATE TABLE games
    (
        ID INT PRIMARY KEY     NOT NULL,
        home_team text         NOT NULL,
        away_team text         NOT NULL,
        home_outcome text      NOT NULL,
        home_score int         NOT NULL,
        away_score INT         NOT NULL,
        neutral_site BOOLEAN   NOT NULL,
        dt DATE                NOT NULL
    );"""
    create_box = """CREATE TABLE box_stats
    (
        ID INT PRIMARY KEY     NOT NULL,
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
    cur.execute(create_games)
    cur.execute(create_box)
    print list_tables(cur)


if __name__ == '__main__':
      main()  