import numpy as np
import db_tools
from queries import Query
from scrape_tools import Scraper
from datetime import datetime, timedelta, date
from itertools import izip


class Adjusted(object):

    def __init__(self, dt, stat, query):
        self.dt = dt
        self.date_seq = datetime.strftime(dt, '%Y%m%d')
        self.date_string = datetime.strftime(dt, '%Y-%m-%d')
        self.query = query
        self.home_factor = 1.014
        self.scraper = Scraper()
        self.stat = stat

    def box_stats(self):
        self.query.box_stats(self.dt)
        self.query.execute(commit=True)

    def team_index(self):
        self.query.query = """SELECT ncaaid, statsheet FROM raw_teams"""
        self.query.execute()

        self.team_indices = {self.query.results[k][1]: k
                             for k in xrange(len(self.query.results))}
        self.nteams = len(self.team_indices)

    def stat_query(self):
        if self.stat == 'ppp':
            return self.query.ppp()
        elif self.stat == 'trt':
            return self.query.trt()
        elif self.stat == 'efg':
            return self.query.efg()

    def initialize(self):
        self.team_index()

        self.query.query = """WITH {stat} AS
                                ({stat_query}, 
                                    gameid, 
                                    teamid, 
                                    statsheet,
                                    g.dt,
                                    g.home_team,
                                    g.away_team
                                FROM detailed_box
                                JOIN games g
                                ON g.id = detailed_box.gameid 
                                AND (g.dt BETWEEN '{start_date}' 
                                AND '{end_date}')

                              SELECT {stat}.*,
                                    d{stat}.{stat} as d{stat},
                                    d{stat}.teamid as oppid,
                                    d{stat}.statsheet AS opp
                              FROM {stat}
                              JOIN {stat} AS d{stat}
                              ON {stat}.teamid != d{stat}.teamid
                              AND {stat}.gameid = d{stat}.gameid;
                           """.format(stat=self.stat,
                                      stat_query=self.stat_query(),
                                      start_date=self.query.get_min_date(self.dt),
                                      end_date=self.date_string)

        self.query.execute()

        raw_omat = np.empty((40, self.nteams))
        raw_dmat = np.empty((40, self.nteams))
        raw_omat.fill(np.nan)
        raw_dmat.fill(np.nan)
        ind_mat = np.empty((40, self.nteams))
        ind_mat.fill(np.nan)
        loc_mat = np.empty((40, self.nteams))
        loc_mat.fill(np.nan)

        for game in self.query.results:
            ostat = float(game[0])
            dstat = float(game[7])
            team = game[3]
            home_id = game[5]
            away_id = game[6]
            teamid = game[2]
            oppid = game[8]
            opp = game[9]

            if teamid == home_id:
                loc_factor = 1/self.home_factor
            else:
                loc_factor = self.home_factor

            r, c = raw_omat.shape
            team_idx = self.team_indices[team]
            opp_idx = self.team_indices[opp]
            last_entry = raw_omat[r - 1][team_idx]

            non_nan_o = np.count_nonzero(~np.isnan(raw_omat[:, team_idx]))
            non_nan_d = np.count_nonzero(~np.isnan(raw_dmat[:, team_idx]))
            raw_omat[non_nan_o][team_idx] = ostat
            raw_dmat[non_nan_d][team_idx] = dstat
            ind_mat[non_nan_o][team_idx] = opp_idx
            loc_mat[non_nan_o][team_idx] = loc_factor

        return raw_omat, raw_dmat, ind_mat, loc_mat, self.nteams

    def preseason_rank(self, team_indices):
        preseason_o = np.zeros((self.nteams, 1))
        preseason_d = np.zeros((self.nteams, 1))

        year = self.scraper.year_from_date(self.dt) - 1
        self.query.kenpom(year)
        self.query.execute()
        for team in self.query.results:
            adjo = team[0]/float(100)
            adjd = team[1]/float(100)
            teamid = team[3]
            preseason_d[self.team_indices[teamid]] = adjd
            preseason_o[self.team_indices[teamid]] = adjo

        return preseason_o, preseason_d

    def convert_index(self, ind_arr):

        ind_arr = ind_arr[~np.isnan(ind_arr)]
        ind_arr = ind_arr.astype(int)

        return ind_arr

    def team_rank(self):

        # get matrices of the
        raw_omat, raw_dmat, ind_mat, loc_mat, nteams = self.initialize()

        # return None
        # raw_de_mat, ind_mat, nteams = self.initialize('dppp')
        preseason_o, preseason_d = self.preseason_rank(self.team_indices)

        # get the national averages
        avg_o_all = np.nanmean(raw_omat)
        avg_d_all = np.nanmean(raw_dmat)

        # initialize adjusted vectors to average of raw efficiency
        adjo = np.zeros(shape=(self.nteams, 1))
        adjd = np.zeros(shape=(self.nteams, 1))
        for team, idx in self.team_indices.iteritems():
            adjo[idx][0] = np.nanmean(raw_o_mat[:, idx])
            adjd[idx][0] = np.nanmean(raw_d_mat[:, idx])

        cnt = 0
        r_off_arr = []
        r_def_arr = []
        r_off = 1
        r_def = 1
        tol = 0.0001
        max_cnt = 15
        while cnt < max_cnt and not(r_off < tol and r_def < tol):
            # keep the previous vectors to calculate residuals
            adj_d_prev = adjd*1
            adj_o_prev = adjo*1

            for team, idx in self.team_indices.iteritems():
                if preseason_d[idx] < 0.01:
                    preseason_d[idx] = avg_d_all
                if preseason_o[idx] < 0.01:
                    preseason_o[idx] = avg_o_all

                # get the team's raw efficiency vectors
                raw_dvec = raw_dmat[:, idx]
                raw_ovec = raw_omat[:, idx]
                loc_vec = loc_mat[:, idx]

                # strip out nan values
                ind_o = ind_mat[:, idx]
                ind_o = self.convert_index(ind_o)
                ind_d = self.convert_index(ind_o)
                raw_ovec = raw_ovec[~np.isnan(raw_ovec)]
                raw_ovec = raw_ovec.reshape(len(raw_ovec), 1)
                raw_dvec = raw_dvec[~np.isnan(raw_dvec)]
                raw_dvec = raw_dvec.reshape(len(raw_dvec), 1)
                loc_vec = loc_vec[~np.isnan(loc_vec)]
                loc_vec = loc_vec.reshape(len(loc_vec), 1)
                length = len(raw_oe_vec)

                w, w_pre = self.weights(length, '', preseason=True)
                new_o = np.sum(((raw_ovec / adjd[ind_o]) * w * loc_vec) * avg_o_all) + preseason_o[idx]*w_pre
                new_d = np.sum(((raw_dvec / adjo[ind_o]) * w* (1 / loc_vec)) * avg_d_all) + preseason_d[idx]*w_pre
                adjo[idx] = new_o
                adjd[idx] = new_d

            # calculate residuals
            r_off = np.linalg.norm(adj_o_prev - adjo)
            r_def = np.linalg.norm(adj_d_prev - adjd)

            r_off_arr.append(r_off)
            r_def_arr.append(r_def)
            cnt += 1

        # print r_def_arr
        # print r_off_arr
        self.adjo = adjo
        self.adjd = adjd
        # total_eff = adj_oe - adj_de

        # self.query.conn.close()

    def store_ranks2(self):
        self.query.query = """SELECT home_team, away_team, dt FROM features WHERE dt = '%s'""" % self.date_string
        self.query.execute()
        for game in self.query.results:
            home_idx = self.team_indices[game[0]]
            away_idx = self.team_indices[game[1]]
            home_adjoe = self.adj_oe[home_idx][0]
            home_adjde = self.adj_de[home_idx][0]
            away_adjoe = self.adj_oe[away_idx][0]
            away_adjde = self.adj_de[away_idx][0]
            stmt = """UPDATE features
                      SET
                        home_adjoe = %s,
                        home_adjde = %s,
                        away_adjoe = %s,
                        away_adjde = %s
                      WHERE home_team = '%s'
                      AND away_team = '%s'
                      AND dt = '%s'""" % (home_adjoe, home_adjde,
                                          away_adjoe, away_adjde,
                                          game[0], game[1], game[2])
            self.query.query = stmt
            self.query.execute(fetch=False)
        self.query.conn.commit()
        self.query.conn.close()

    def store_ranks(self):
        for team, idx in self.team_indices.iteritems():

            self.query.query = """UPDATE features SET home_adjoe = %s, home_adjde = %s WHERE dt = '%s' AND (home_team = '%s')""" % (self.adj_oe[idx][0], self.adj_de[idx][0], self.date_string, team)
            self.query.execute(commit=True)
            self.query.query = """UPDATE features SET away_adjoe = %s, away_adjde = %s WHERE dt = '%s' AND (away_team = '%s')""" % (self.adj_oe[idx][0], self.adj_de[idx][0], self.date_string, team)
            self.query.execute(commit=True)
            # self.query.query = """UPDATE features SET home_adjde = %s WHERE dt = '%s' AND (home_team = '%s')""" % (self.adj_de[idx][0], self.date_string, team)
            # self.query.execute(commit=True)
            # self.query.query = """UPDATE features SET away_adjde = %s WHERE dt = '%s' AND (away_team = '%s')""" % (self.adj_de[idx][0], self.date_string, team)
            # self.query.execute(commit=True)

        self.query.conn.close()

    def print_ranks(self):
        rank_arr = self.adj_oe - self.adj_de
        ranks = [(team, rank_arr[idx]) for team, idx in
                 self.team_indices.iteritems()]
        ranks = sorted(ranks, key=lambda tup: tup[1])

        for j in xrange(len(ranks)):
            print "#%d %s: %s" % (len(ranks) - j, ranks[j][0], ranks[j][1])

    def weights(self, n, wtype='linear', preseason=False):
        if n == 0:
            w = np.array([1])
            return w[:, np.newaxis], 0
        elif wtype == 'linear':
            w = np.array(xrange(1, n+1))
            w = w*(1/float(w.sum()))
        else:
            w = np.ones(n) / n

        w = w[:, np.newaxis]
        if preseason:
            c = 0.4
            n_pre = 10
            w_pre = c - c*n/(n_pre)
            w_pre = max(0, w_pre)  # don't return anything less than zero
            w = w*(1./(w.sum()/(1 - w_pre)))
            #w = np.concatenate((np.array([w_pre])[:, np.newaxis], w), axis=0)
        else:
            w_pre = 0

        return w, w_pre


def main():
    start_date = datetime(2013, 11, 9).date()
    end_date = datetime(2014, 3, 1).date()
    day_count = (end_date - start_date).days + 1

    rank = Adjusted(start_date, 'trt', Query())
    rank.team_rank()
    # rank.store_ranks2()
    return None

    for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
        print single_date
        q = Query()
        rank = Adjusted(single_date, q)
        rank.team_rank()
        rank.store_ranks2()
        # rank.print_ranks()


if __name__ == '__main__':
    main()
