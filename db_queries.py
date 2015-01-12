import psycopg2
import psycopg2.extras
from datetime import datetime, date
import db_tools


def show_games():
    q = """WITH tmp AS
           (SELECT g.dt, g.home_team, g.away_team, r.ncaa, r.ncaaid
           FROM games g
           JOIN raw_teams r
           ON r.ncaaid = g.home_team OR r.ncaaid = g.away_team
           ORDER BY g.home_team)
            
           SELECT
                a.dt, a.ncaa AS home_team, b.ncaa AS away_team
           FROM tmp a
           JOIN tmp b
           ON a.ncaa != b.ncaa 
           AND (a.dt = b.dt AND a.ncaaid = a.home_team AND b.ncaaid = a.away_team)
           ORDER BY a.dt;
        """
    return q

def new_games_table(cur):
    cur.execute("""DROP TABLE IF EXISTS games2;""")


    q = """CREATE TABLE games2 AS
                (WITH tmp AS
                   (SELECT g.*, r.ncaa, r.ncaaid
                   FROM games g
                   JOIN raw_teams r
                   ON r.ncaaid = g.home_team OR r.ncaaid = g.away_team
                   ORDER BY g.home_team)
                    
                   SELECT
                        a.id,
                        a.home_team,
                        a.away_team,
                        a.home_outcome,
                        a.home_score,
                        a.away_score,
                        a.home_first,
                        a.away_first,
                        a.home_second,
                        a.away_second,
                        a.neutral_site,
                        a.officials,
                        a.attendance,
                        a.venue,
                        a.dt, 
                        a.ncaa as home_team_string, 
                        b.ncaa as away_team_string
                   FROM tmp a
                   JOIN tmp b
                   ON a.ncaa != b.ncaa 
                   AND (a.dt = b.dt AND a.ncaaid = a.home_team AND b.ncaaid = a.away_team)
                   ORDER BY a.dt);
        """
    cur.execute(q)

    print 'Created new games table'

def show_attendance(col):
    q = """WITH tmp AS
           (SELECT 
                g.dt, 
                g.home_team, 
                g.away_team, 
                r.ncaa, 
                r.ncaaid,
                g.%s
           FROM games g
           JOIN raw_teams r
           ON r.ncaaid = g.home_team OR r.ncaaid = g.away_team
           ORDER BY g.home_team)
            
           SELECT
                a.dt, a.ncaa AS home_team, b.ncaa AS away_team, a.%s
           FROM tmp a
           JOIN tmp b
           ON a.ncaa != b.ncaa 
           AND (a.dt = b.dt AND a.ncaaid = a.home_team AND b.ncaaid = a.away_team)
           ORDER BY a.%s DESC;
        """ % (col, col, col)
    return q


def get_team_strings(table_name):
    q = """SELECT
                a.dt, a.ncaa AS home_team, b.ncaa AS away_team
           FROM %s a
           JOIN %s b
           ON a.ncaa != b.ncaa 
           AND (a.dt = b.dt AND a.ncaaid = a.home_team AND b.ncaaid = a.away_team)
           ORDER BY a.dt DESC;""" % (table_name, table_name)

    return q


def show_officials():
    q = """SELECT
                dt,
                away_team_string,
                home_team_string,
                split_part(officials, ',', 1),
                split_part(officials, ',', 2),
                split_part(officials, ',', 3)
           FROM games2;"""
    return q


def home_win_pct():
    q = """SELECT CAST(COUNT(*) AS REAL) / (SELECT COUNT(*) FROM games2)
           FROM games2
           WHERE home_outcome = 'W'
        """

    return q


def get_big_ten():
    q = """SELECT ncaaid, statsheet
           FROM raw_teams
           WHERE statsheet = 'indiana'
           OR statsheet = 'purdue'
           OR statsheet = 'ohio-state'
           OR statsheet = 'michigan-state'
           OR statsheet = 'michigan'
           OR statsheet = 'northwestern'
           OR statsheet = 'iowa'
           OR statsheet = 'illinois'
           OR statsheet = 'wisconsin'
           OR statsheet = 'minnesota'
           OR statsheet = 'nebraska'
           OR statsheet = 'penn-state'
           """

    return q


def show_results(results):
    for result in results:
        print ''
        for item in result:
            print item,
            print '|',


def main():
    cur, conn = db_tools.get_cursor()
    q = home_win_pct()
    q = get_big_ten()
    #new_games_table(cur)
    cur.execute(q)
    show_results(cur.fetchall())
    big_ten = {'301', '306', '312', '418', '416', '428',
               '463', '509', '539', '518', '559', '796'}
    #conn.commit()
    conn.close()


if __name__ == '__main__':
    main()