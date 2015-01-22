import psycopg2
import psycopg2.extras
from datetime import datetime, date
import db_tools


class Query(object):

    def __init__(self):
        self.query = ''
        self.get_cursor()

    def get_cursor(self, cursor_type=None):
        conn = psycopg2.connect(database="seth", user="seth", password="abc123",
                                host="localhost", port="5432")
        if cursor_type == 'dict':
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        else:
            cur = conn.cursor()

        self.cursor = cur
        self.conn = conn

    def execute(self, conn=None, cur=None, commit=False, limit=0):
        if limit != 0:
            # add a limit statement to the end of query
            self.query = self.query.replace(';', '')
            self.query += ' LIMIT %d' % limit

        try:
            self.cursor.execute(self.query)
            if commit:
                self.conn.commit()
        except:
            print 'Error executing query'
            print self.query
            self.conn.rollback()

        # fetch results if there are some
        try:
            self.results = self.cursor.fetchall()
        except psycopg2.ProgrammingError:
            pass

        #self.conn.close()

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

    def detailed_box(self):
        q = """CREATE TABLE detailed_box AS
               SELECT b.*, r.statsheet
               FROM box_stats b
               JOIN raw_teams r
               ON r.ncaaid = b.teamid
            """
        self.query = q

    def box_stats(self, the_date):
        date_string = datetime.strftime(the_date, '%Y-%m-%d')
        date_seq = datetime.strftime(the_date, '%Y%m%d')
        start_date = self.get_min_date(the_date)
        q = """CREATE TABLE box_stats_%s AS
               (SELECT b.*, g.dt
               FROM box_stats b
               JOIN games g
               ON g.id = b.gameid AND (g.dt BETWEEN '%s' AND '%s'))
            """ % (date_seq, start_date, date_string)

        self.query = q

    def home_win_pct(self):
        q = """SELECT CAST(COUNT(*) AS REAL) / (SELECT COUNT(*) FROM games2)
               FROM games2
               WHERE home_outcome = 'W'
            """

        self.query = q

    def aggregate_box(self, the_date):
        date_string = datetime.strftime(the_date, '%Y-%m-%d')
        date_seq = datetime.strftime(the_date, '%Y%m%d')
        start_date = self.get_min_date(the_date)
        q = """ CREATE TABLE agg_box_stats_%s AS
                SELECT
                    SUM(b.pts) as pts,
                    SUM(b.fgm) as fgm,
                    SUM(b.fga) as fga,
                    SUM(b.tpm) as tpm,
                    SUM(b.tpa) as tpa,
                    SUM(b.ftm) as ftm,
                    SUM(b.fta) as fta,
                    SUM(b.oreb) as oreb,
                    SUM(b.dreb) as dreb,
                    SUM(b.reb) as reb,
                    SUM(b.ast) as stl,
                    SUM(b.blk) as blk,
                    SUM(b.turnover) as turnover,
                    SUM(b.pf) as pf,
                    COUNT(b.teamid) ngames,
                    b.teamid
                FROM box_stats b
                JOIN games g
                ON g.id = b.gameid AND (g.dt BETWEEN '%s' AND '%s')
                GROUP BY b.teamid
            """ % (date_seq, datetime.strftime(start_date, '%Y-%m-%d'), date_string)
        self.query = q

    def kenpom(self, year):
        q = """SELECT k.adjoe, k.adjde, k.ncaaid, r.statsheet
               FROM kenpom k
               JOIN raw_teams r
               ON r.ncaaid = k.ncaaid
               WHERE year = %s
            """ % year

        self.query = q
    def get_min_date(self, dt):
        if dt.month < 6:
            year = dt.year - 1
        else:
            year = dt.year

        return date(year, 9, 1)

    def insert_values(self, table_name, val_dict):

        keys = val_dict.keys()
        values = [val_dict[k] for k in keys]
        sqlCommand = 'INSERT INTO {table} ({keys}) VALUES ({placeholders});'.format(
          table = table_name,
          keys = ', '.join(keys),
          placeholders = ', '.join([ "%s" for v in values ])
        )

        self.cursor.execute(sqlCommand, values)
        self.conn.commit()
        #self.conn.close()

    def adjusted_stats(self, the_date):
        date_string = datetime.strftime(the_date, '%Y-%m-%d')
        date_seq = datetime.strftime(the_date, '%Y%m%d')
        q = """CREATE TABLE adjusted_stats_%s
                (
                    id serial PRIMARY KEY  NOT NULL,
                    statsheet text NOT NULL,
                    adjoe real                     ,
                    adjde real                       
                );""" % date_seq
        print q
        self.query = q

    def possessions(self):
        q = """ CREATE TABLE features2 AS
                WITH poss AS
                (SELECT
                    a.teamid,
                    b.dt, 
                    sum(0.96*(a.fga - a.oreb + a.turnover + (0.475*a.fta))) OVER (PARTITION BY a.teamid ORDER BY b.dt) AS cum_poss,
                    count(a.teamid) OVER (PARTITION BY a.teamid ORDER BY b.dt) as ngames
                    FROM detailed_box a
                JOIN games b
                ON a.gameid = b.id),
                
                a AS
                (SELECT f.*, p.cum_poss / p.ngames AS home_poss, p.ngames AS home_ngames
                FROM features f
                JOIN poss p
                ON p.teamid = f.home_id AND p.dt = f.dt),
                
                b AS
                (SELECT
                    DISTINCT b.dt, 
                    sum(0.96*(a.fga - a.oreb + a.turnover + (0.475*a.fta))) OVER(ORDER BY b.dt) / (count(b.dt) OVER(ORDER BY b.dt))AS poss_game,
                    count(b.dt) OVER (ORDER BY b.dt) / 2 AS total_games
                FROM detailed_box a
                JOIN games b
                ON a.gameid = b.id
                ORDER BY b.dt)
                
                SELECT c.*, b.poss_game, b.total_games
                FROM
                  (SELECT a.*, p.cum_poss / p.ngames AS away_poss, p.ngames AS away_ngames
                  FROM a
                  JOIN poss p
                  ON p.teamid = a.away_id AND p.dt = a.dt
                  ORDER BY a.away_team) c
                JOIN b
                ON b.dt = c.dt;"""

        self.query = q


    def box_advanced_stats(self, new_table='advanced_stats',
                           stats_table='box_stats'):
        prefix = """CREATE TABLE %s AS""" % new_table

        opp = """opp AS
                 (SELECT
                    b.gameid,
                    a.teamid,
                    b.teamid as oppid
                  FROM %s a
                  JOIN %s b
                  ON a.gameid = b.gameid AND a.teamid != b.teamid),
              """ % (stats_table, stats_table)
        pos = """pos AS
                (SELECT 
                    dt,
                    gameid,
                    teamid,
                    fga - oreb + turnover + 0.475*fta AS pos
                 FROM %s),
              """ % stats_table
        ppp = """ppp AS
                    (SELECT
                        b.gameid,
                        b.pts / p.pos AS ppp,
                        b.teamid
                     FROM %s b
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
                     FROM %s),
              """ % stats_table
        ort = """ort AS
                    (SELECT
                        CAST (oreb AS REAL) / (fga - fgm) AS ort,
                        teamid,
                        gameid
                     FROM %s),
              """ % stats_table
        topp = """topp AS
                    (SELECT
                        b.gameid,
                        b.turnover / p.pos AS topp,
                        b.teamid
                     FROM %s b
                     JOIN pos p
                     ON b.gameid = p.gameid AND b.teamid = p.teamid)
             """ % stats_table

        suffix = '''SELECT pos.gameid, pos.teamid, pos.dt,
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

    def features(self):
        q = """ CREATE TABLE features AS

                WITH a AS
                (SELECT 
                      g.id,
                      g.home_team AS home_id,
                      r.statsheet AS home_team,
                      g.away_team AS away_id,
                      g.home_score,
                      g.away_score,
                      g.dt
                FROM games g
                JOIN raw_teams r
                ON g.home_team = r.ncaaid)
                
                SELECT
                      a.*,
                      r.statsheet as away_team
                FROM a
                JOIN raw_teams r
                ON a.away_id = r.ncaaid
            """

        self.query = q

def main():
    q = Query()
    q.possessions()
    q.execute(commit=True)
    print q.query
    return None
    the_date = date(2014, 2, 1)
    date_seq = datetime.strftime(the_date, '%Y%m%d')
    q.box_advanced_stats('advanced_stats_%s' % date_seq,'box_stats_%s' % date_seq)
    #q.box_stats(the_date)
    q.execute(commit=True)
    print q.query
    #q.show_results()


if __name__ == '__main__':
    main()