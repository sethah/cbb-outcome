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
            
           SELECT DISTINCT
                a.dt, a.ncaa AS home_team, b.ncaa AS away_team
           FROM tmp a
           JOIN tmp b
           ON a.ncaa != b.ncaa 
           AND (a.dt = b.dt AND a.ncaaid = a.home_team AND b.ncaaid = a.away_team)
           ORDER BY a.dt;
        """
    return q


def main():
    cur, conn = db_tools.get_cursor()
    cur.execute(show_games())
    for result in cur.fetchall():
        print result


if __name__ == '__main__':
    main()