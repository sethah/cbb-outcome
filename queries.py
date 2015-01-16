import psycopg2
import psycopg2.extras
from datetime import datetime, date
import db_tools
import matplotlib.pyplot as plt
import pandas as pd


class Query(object):

    def __init__(self):
        self.query = ''

    def get_cursor(self, cursor_type=None):
        conn = psycopg2.connect(database="seth", user="seth", password="abc123",
                                host="localhost", port="5432")
        if cursor_type == 'dict':
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cur = conn.cursor()

        return cur, conn

    def execute(self, conn=None, cur=None, commit=False, limit=0):
        if conn is None:
            cur, conn = self.get_cursor()

        if limit != 0:
            # add a limit statement to the end of query
            self.query = self.query.replace(';', '')
            self.query += ' LIMIT %d' % limit

        try:
            cur.execute(self.query)
            if commit:
                conn.commit()
        except:
            conn.rollback()

        self.results = cur.fetchall()
        conn.close()

    def show_results(self):
        for result in self.results:
            print ''
            for item in result:
                print item,
                print '|',

    def get_big_ten(self):
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

        self.query = q

    def home_win_pct(self):
        q = """SELECT CAST(COUNT(*) AS REAL) / (SELECT COUNT(*) FROM games2)
               FROM games2
               WHERE home_outcome = 'W'
            """

        self.query = q

    def aggregate_box(self, date):
        date_string = datetime.strftime(date, '%Y-%m-%d')
        q = """ SELECT
                    SUM(pts) as pts,
                    SUM(fgm) as fgm,
                    SUM(fga) as fga,
                    SUM(tpm) as tpm,
                    SUM(tpa) as tpa,
                    SUM(ftm) as ftm,
                    SUM(fta) as fta,
                    SUM(oreb) as oreb,
                    SUM(dreb) as dreb,
                    SUM(reb) as reb,
                    SUM(ast) as stl,
                    SUM(blk) as blk,
                    SUM(turnover) as turnover,
                    SUM(pf) as pf,
                    COUNT(teamid) ngames,
                    teamid
                FROM box_stats
                GROUP BY teamid
            """
        self.query = q

    def box_advanced_stats(self, new_table='advanced_stats',
                           stats_table='box_stats'):
        prefix = """CREATE TABLE '%s' AS""" % new_table

        opp = """opp AS
                 (SELECT
                    b.gameid,
                    a.teamid,
                    b.teamid as oppid
                  FROM '%s' a
                  JOIN '%s' b
                  ON a.gameid = b.gameid AND a.teamid != b.teamid),
              """ % stats_table
        pos = """pos AS
                (SELECT 
                    gameid,
                    teamid,
                    fga - oreb + turnover + 0.475*fta AS pos
                 FROM '%s'),
              """ % stats_table
        ppp = """ppp AS
                    (SELECT
                        b.gameid,
                        b.pts / p.pos AS ppp,
                        b.teamid
                     FROM '%s' b
                     JOIN pos p
                     ON b.gameid = p.gameid AND b.teamid = p.teamid),
              """ % stats_table
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
                     FROM '%s'),
              """ % stats_table
        ort = """ort AS
                    (SELECT
                        CAST (oreb AS REAL) / (fga - fgm) AS ort,
                        teamid,
                        gameid
                     FROM '%s'),
              """ % stats_table
        topp = """topp AS
                    (SELECT
                        b.gameid,
                        b.turnover / p.pos AS topp,
                        b.teamid
                     FROM '%s' b
                     JOIN pos p
                     ON b.gameid = p.gameid AND b.teamid = p.teamid)
             """ % stats_table

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
        self.query = q

def main():
    q = Query()
    q.aggregate_box(date(2014, 3, 3))
    q.execute(limit=10)
    q.show_results()


if __name__ == '__main__':
    main()