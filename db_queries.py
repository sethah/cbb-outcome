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


def big_ten_games():
    q = """CREATE TABLE big_ten AS
           (SELECT * 
           FROM games2
           WHERE dt > timestamp '2013-12-30'::date);
        """
    return q

def big_ten_stats():
    q = """CREATE TABLE big_ten_stats AS
           SELECT a.*
           FROM box_stats a
           JOIN big_ten b
           ON a.gameid = b.id;
        """
    return q


def box_advanced_stats():

    prefix = """CREATE TABLE advanced_stats AS"""

    opp = """opp AS
             (SELECT
                b.gameid,
                a.teamid,
                b.teamid as oppid
              FROM big_ten_stats a
              JOIN big_ten_stats b
              ON a.gameid = b.gameid AND a.teamid != b.teamid),
          """

    pos = """pos AS
            (SELECT 
                gameid,
                teamid,
                fga - oreb + turnover + 0.475*fta AS pos
             FROM big_ten_stats),
          """
    ppp = """ppp AS
                (SELECT
                    b.gameid,
                    b.pts / p.pos AS ppp,
                    b.teamid
                 FROM big_ten_stats b
                 JOIN pos p
                 ON b.gameid = p.gameid AND b.teamid = p.teamid),
          """
    dppp = """dppp AS
                (SELECT
                    a.gameid,
                    a.teamid,
                    b.ppp as dppp
                 FROM ppp a
                 JOIN ppp b
                 ON a.gameid = b.gameid AND a.teamid != b.teamid),
          """
    efg = """efg AS
                (SELECT
                    (fgm + 0.5*tpm) / fga AS efg,
                    teamid,
                    gameid
                 FROM big_ten_stats),
          """
    ort = """ort AS
                (SELECT
                    CAST (oreb AS REAL) / (fga - fgm) AS ort,
                    teamid,
                    gameid
                 FROM big_ten_stats),
          """
    topp = """topp AS
                (SELECT
                    b.gameid,
                    b.turnover / p.pos AS topp,
                    b.teamid
                 FROM big_ten_stats b
                 JOIN pos p
                 ON b.gameid = p.gameid AND b.teamid = p.teamid)
         """

    suffix = '''SELECT pos.gameid, pos.teamid,
                      (SELECT opp.oppid
                          FROM opp 
                          WHERE (opp.gameid = pos.gameid)
                          AND (opp.teamid = pos.teamid)),
                      (SELECT ppp.ppp 
                          FROM ppp 
                          WHERE (ppp.gameid = pos.gameid)
                          AND (ppp.teamid = pos.teamid)),
                      (SELECT dppp.dppp
                          FROM dppp 
                          WHERE (dppp.gameid = pos.gameid)
                          AND (dppp.teamid = pos.teamid)),
                      (SELECT efg.efg
                          FROM efg 
                          WHERE (efg.gameid = pos.gameid)
                          AND (efg.teamid = pos.teamid)),
                      (SELECT ort.ort
                          FROM ort 
                          WHERE (ort.gameid = pos.gameid)
                          AND (ort.teamid = pos.teamid)),
                      (SELECT topp.topp
                          FROM topp 
                          WHERE (topp.gameid = pos.gameid)
                          AND (topp.teamid = pos.teamid)),
                      pos.pos
               FROM pos;
            '''

    q = prefix + '\nWITH ' + pos + '\n' + opp + '\n' + ppp + '\n' + \
        dppp + '\n' +efg + '\n' + ort + '\n' + topp + '\n' \
        + suffix
    return q

def agg_box_stats():
    pass


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
    q = box_advanced_stats()
    #q = big_ten_stats()
    print q
    #new_games_table(cur)
    cur.execute(q)
    #show_results(cur.fetchall())
    big_ten = {'301', '306', '312', '418', '416', '428',
               '463', '509', '539', '518', '559', '796'}
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()