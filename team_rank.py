import numpy as np
import psycopg2
import psycopg2.extras
import db_tools
import db_queries as dbq


def initialize(col_name, table_name):
    cur, conn = db_tools.get_cursor()
    q = dbq.get_big_ten()
    q = """SELECT ncaaid, statsheet FROM raw_teams"""
    cur.execute(q)
    result = cur.fetchall()
    team_indices = {result[k][0]: k for k in xrange(len(result))}
    nteams = len(team_indices)

    q = """SELECT %s, teamid, oppid FROM %s""" % (col_name, table_name)
    cur.execute(q)
    result = cur.fetchall()

    raw_mat = np.empty((1, nteams))
    raw_mat.fill(np.nan)
    ind_mat = np.empty((1, nteams))
    ind_mat.fill(np.nan)
    for game in result:
        val = float(game[0])
        teamid = game[1]
        oppid = game[2]

        r, c = raw_mat.shape
        team_idx = team_indices[teamid]
        opp_idx = team_indices[oppid]
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

    conn.close()

    return raw_mat, ind_mat, nteams


def convert_index(ind_arr):

    ind_arr = ind_arr[~np.isnan(ind_arr)]
    ind_arr = ind_arr.astype(int)

    return ind_arr

def weights(n, wtype='linear'):
    if wtype == 'linear':
        w = np.array(xrange(1, n+1))
        w =  w*(1/float(w.sum()))
        w = w.reshape(n, 1)
    else:
        w = np.ones(shape=(n, 1)) / n
    return w


def team_rank(date):
    # get all the big ten teams
    cur, conn = db_tools.get_cursor()
    q = dbq.get_big_ten()
    q = """SELECT ncaaid, statsheet FROM raw_teams"""
    cur.execute(q)
    result = cur.fetchall()
    team_indices = {result[k][0]: k for k in xrange(len(result))}
    nteams = len(team_indices)

    # get matrices of the
    raw_oe_mat, ind_mat, nteams = initialize('ppp', 'advanced_stats')
    raw_de_mat, ind_mat, nteams = initialize('dppp', 'advanced_stats')

    # get the national averages
    avg_oe_all = np.nanmean(raw_oe_mat)
    avg_de_all = np.nanmean(raw_de_mat)

    # initialize adjusted vectors to average of raw efficiency
    adj_oe = np.zeros(shape=(nteams, 1))
    adj_de = np.zeros(shape=(nteams, 1))
    for team, idx in team_indices.iteritems():
        adj_oe[idx][0] = np.nanmean(raw_oe_mat[:, idx])
        adj_de[idx][0] = np.nanmean(raw_de_mat[:, idx])

    cnt = 0
    r_off_arr = []
    r_def_arr = []
    r_off = 1
    r_def = 1
    tol = 0.0001
    max_cnt = 4
    while cnt < max_cnt and not(r_off < tol and r_def < tol):
        # keep the previous vectors to calculate residuals
        adj_de_prev = adj_de*1
        adj_oe_prev = adj_oe*1

        for team, idx in team_indices.iteritems():

            # get the team's raw efficiency vectors
            raw_de_vec = raw_de_mat[:, idx]
            raw_oe_vec = raw_oe_mat[:, idx]
            

            # strip out nan values
            ind_oe = ind_mat[:, idx]
            ind_oe = convert_index(ind_oe)
            #print db_tools.get_team_id(team, col1='ncaaid', col2='ncaa')
            ind_de = convert_index(ind_oe)
            raw_oe_vec = raw_oe_vec[~np.isnan(raw_oe_vec)]
            raw_oe_vec = raw_oe_vec.reshape(len(raw_oe_vec),1)
            raw_de_vec = raw_de_vec[~np.isnan(raw_de_vec)]
            raw_de_vec = raw_de_vec.reshape(len(raw_de_vec),1)
            length = len(raw_oe_vec)
            
            w = weights(length, '')
            #print w

            # get new efficiency for the team, using equal weights
            new_oe = np.sum(((raw_oe_vec / adj_de[ind_oe]) * w) * avg_oe_all)
            new_de = np.sum(((raw_de_vec / adj_oe[ind_oe]) * w) * avg_de_all)
            adj_oe[idx] = new_oe
            adj_de[idx] = new_de

        # calculate residuals
        r_off = np.linalg.norm(adj_oe_prev - adj_oe)
        r_def = np.linalg.norm(adj_de_prev - adj_de)

        r_off_arr.append(r_off)
        r_def_arr.append(r_def)

        cnt += 1

    # print r_def_arr
    # print r_off_arr

    total_eff = adj_oe - adj_de
    l = ['']*nteams
    for team, idx in team_indices.iteritems():
        l[idx] = team

    l = [db_tools.get_team_id(team, col1='ncaaid', col2='ncaa') for team in l]

    rank_arr = total_eff
    ranks = [(l[k], rank_arr[k]) for k in xrange(len(l))]
    ranks = sorted(ranks, key=lambda tup: tup[1])

    for j in xrange(len(ranks)):
        print "#%d %s: %s" % (len(ranks) - j, ranks[j][0], ranks[j][1])

    conn.close()


def main():
    team_rank('')


if __name__ == '__main__':
    main()
