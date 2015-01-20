import numpy as np
import psycopg2
import psycopg2.extras
import db_tools
import db_queries as dbq
from queries import Query
import scrape_tools as scrape
from datetime import date, datetime

class Adjusted(object):

    def __init__(self, dt, query):
        self.dt = dt
        self.date_seq = datetime.strftime(dt, '%Y%m%d')
        self.query = query

    def box_stats(self):
        self.query.box_stats(self.dt)
        self.query.execute(commit=True)

    def drop_box(self):
        self.query.query = """DROP TABLE IF EXISTS box_stats_%s""" % self.date_seq
        self.query.execute(commit=True)

    def drop_advanced(self):
        self.query.query = """DROP TABLE IF EXISTS advanced_stats_%s""" % self.date_seq
        self.query.execute(commit=True)

    def advanced_stats(self):
        self.query.box_advanced_stats('advanced_stats_%s' % self.date_seq,
                                      'box_stats_%s' % self.date_seq)
        self.query.execute(commit=True)

    def team_index(self):
        self.query.query = """SELECT ncaaid, statsheet FROM raw_teams"""
        self.query.execute()
        
        self.team_indices = {self.query.results[k][1]: k for k in xrange(len(self.query.results))}
        self.nteams = len(self.team_indices)

    def initialize(self, col_name):
        self.team_index()

        self.query.query = """SELECT %s, teamid, oppid FROM advanced_stats_%s""" % (col_name, self.date_seq)
        self.query.execute()

        raw_mat = np.empty((1, self.nteams))
        raw_mat.fill(np.nan)
        ind_mat = np.empty((1, self.nteams))
        ind_mat.fill(np.nan)
        for game in self.query.results:
            val = float(game[0])
            teamid = int(game[1])
            oppid = int(game[2])

            r, c = raw_mat.shape
            team_idx = self.team_indices[teamid]
            opp_idx = self.team_indices[oppid]
            last_entry = raw_mat[r - 1][team_idx]
            if not np.isnan(last_entry):
                # add a row
                new_row = np.empty((1, c))
                new_row.fill(np.nan)
                raw_mat = np.concatenate((raw_mat, new_row), axis=0)
                ind_mat = np.concatenate((ind_mat, new_row), axis=0)
                raw_mat[r][team_idx] = val
                ind_mat[r][team_idx] = opp_idx
            else:
                non_nan = np.count_nonzero(~np.isnan(raw_mat[:, team_idx]))
                raw_mat[non_nan][team_idx] = val
                ind_mat[non_nan][team_idx] = opp_idx


        # conn.close()

        return raw_mat, ind_mat, self.nteams


    def preseason_rank(self, team_indices):
        preseason_oe = np.zeros((self.nteams, 1))
        preseason_de = np.zeros((self.nteams, 1))

        year = scrape.year_from_date(self.dt) - 1
        self.query.kenpom(year)
        self.query.execute()
        for team in self.query.results:
            adjoe = team[0]/float(100)
            adjde = team[1]/float(100)
            teamid = team[3]
            preseason_de[self.team_indices[teamid]] = adjde
            preseason_oe[self.team_indices[teamid]] = adjoe

        return preseason_oe, preseason_de


    def convert_index(self, ind_arr):

        ind_arr = ind_arr[~np.isnan(ind_arr)]
        ind_arr = ind_arr.astype(int)

        return ind_arr


    def team_rank(self):

        # get matrices of the
        raw_oe_mat, ind_mat, nteams = self.initialize('ppp')
        raw_de_mat, ind_mat, nteams = self.initialize('dppp')
        preseason_oe, preseason_de = self.preseason_rank(self.team_indices)

        # get the national averages
        avg_oe_all = np.nanmean(raw_oe_mat)
        avg_de_all = np.nanmean(raw_de_mat)

        # initialize adjusted vectors to average of raw efficiency
        adj_oe = np.zeros(shape=(self.nteams, 1))
        adj_de = np.zeros(shape=(self.nteams, 1))
        for team, idx in team_indices.iteritems():
            adj_oe[idx][0] = np.nanmean(raw_oe_mat[:, idx])
            adj_de[idx][0] = np.nanmean(raw_de_mat[:, idx])

        cnt = 0
        r_off_arr = []
        r_def_arr = []
        r_off = 1
        r_def = 1
        tol = 0.0001
        max_cnt = 10
        while cnt < max_cnt and not(r_off < tol and r_def < tol):
            # keep the previous vectors to calculate residuals
            adj_de_prev = adj_de*1
            adj_oe_prev = adj_oe*1

            for team, idx in self.team_indices.iteritems():
                if preseason_de[idx] < 0.01:
                    preseason_de[idx] = avg_de_all
                if preseason_oe[idx] < 0.01:
                    preseason_oe[idx] = avg_oe_all


                # get the team's raw efficiency vectors
                raw_de_vec = raw_de_mat[:, idx]
                raw_oe_vec = raw_oe_mat[:, idx]
                
                
                # strip out nan values
                ind_oe = ind_mat[:, idx]
                ind_oe = self.convert_index(ind_oe)
                ind_de = self.convert_index(ind_oe)
                raw_oe_vec = raw_oe_vec[~np.isnan(raw_oe_vec)]
                raw_oe_vec = raw_oe_vec.reshape(len(raw_oe_vec),1)
                raw_de_vec = raw_de_vec[~np.isnan(raw_de_vec)]
                raw_de_vec = raw_de_vec.reshape(len(raw_de_vec),1)
                length = len(raw_oe_vec)
                
                w, w_pre = self.weights(length, '', preseason=True)
                #print w
                if team == '104':
                    print [(raw_oe_vec[k], adj_de[ind_oe][k]) for k in xrange(len(raw_oe_vec))]
                # get new efficiency for the team, using equal weights
                new_oe = np.sum(((raw_oe_vec / adj_de[ind_oe]) * w) * avg_oe_all) + preseason_oe[idx]*w_pre
                new_de = np.sum(((raw_de_vec / adj_oe[ind_oe]) * w) * avg_de_all) + preseason_de[idx]*w_pre
                adj_oe[idx] = new_oe
                adj_de[idx] = new_de

            # calculate residuals
            r_off = np.linalg.norm(adj_oe_prev - adj_oe)
            r_def = np.linalg.norm(adj_de_prev - adj_de)

            r_off_arr.append(r_off)
            r_def_arr.append(r_def)
            cnt += 1
        # return None

        print r_def_arr
        print r_off_arr
        total_eff = adj_oe - adj_de
        
        self.query.adjusted_stats(self.dt)
        self.query.execute(commit=True)
        print 'asdf'
        for team, idx in self.team_indices.iteritems():
            if team is None:
                continue
            l[idx] = team
            d = {}
            d['statsheet'] = team
            d['adjoe'] = float(adj_oe[idx])
            d['adjde'] = float(adj_de[idx])
            self.query.insert_values('adjusted_stats_%s' % self.date_seq, d)

        rank_arr = total_eff
        ranks = [(team, rank_arr[idx]) for team, idx in self.team_indices.iteritems()]
        ranks = sorted(ranks, key=lambda tup: tup[1])

        for j in xrange(len(ranks)):
            print "#%d %s: %s" % (len(ranks) - j, ranks[j][0], ranks[j][1]), preseason_oe[j]-preseason_de[j]

        self.query.conn.close()


    def weights(self, n, wtype='linear', preseason=False):
        if n == 0: 
            w = np.array([1])
            return w[:, np.newaxis], 0
        elif wtype == 'linear':
            w = np.array(xrange(1, n+1))
            w =  w*(1/float(w.sum()))
        else:
            w = np.ones(n) / n

        w = w[:, np.newaxis]
        if preseason:
            c = 0.3
            n_pre = 10
            w_pre = c - c*n/(n_pre)
            w_pre = max(0, w_pre) # don't return anything less than zero
            w = w*(1./(w.sum()/(1 - w_pre)))
            #w = np.concatenate((np.array([w_pre])[:, np.newaxis], w), axis=0)
        else:
            w_pre = 0

        return w, w_pre


def main():
    q = Query()
    rank = Adjusted(date(2014, 2, 1), q)
    # rank.box_stats()
    # rank.advanced_stats()
    rank.team_rank()
    # rank.drop_box()
    # rank.drop_advanced()
    # team_rank(date(2014, 2, 1))
    # for j in xrange(5):
    #     w = weights(j, wtype='uniform', preseason=True)
    #     print w
    #     print 'Sum: ', w.sum()
    #     print '*'*10
    # preseason_rank('', date(2014, 3, 1))


if __name__ == '__main__':
    main()
