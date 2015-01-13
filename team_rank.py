import numpy as np
import psycopg2
import psycopg2.extras
import db_tools
import db_queries as dbq

def team_rank(date):
    cur, conn = db_tools.get_cursor()
    q = dbq.get_big_ten()
    cur.execute(q)
    result = cur.fetchall()
    team_indices = {result[k][0]: k for k in xrange(len(result))}

    q = """SELECT ppp, teamid, oppid FROM advanced_stats"""
    cur.execute(q)
    result = cur.fetchall()
    nteams = len(team_indices)

    raw_oe_mat = np.empty((1,nteams))
    raw_oe_mat.fill(np.nan)
    ind_mat = np.empty((1,nteams), dtype=int)
    ind_mat.fill(np.nan)
    for game in result:
        ppp = float(game[0])
        teamid = game[1]
        oppid = game[2]

        r, c = raw_oe_mat.shape
        team_idx = team_indices[teamid]
        opp_idx = team_indices[oppid]
        last_entry = raw_oe_mat[r-1][team_idx]
        if not np.isnan(last_entry):
            # add a row
            new_row = np.empty((1,c))
            new_row.fill(np.nan)
            raw_oe_mat = np.concatenate((raw_oe_mat, new_row), axis=0)
            ind_mat = np.concatenate((ind_mat, new_row), axis=0)
            raw_oe_mat[r][team_idx] = ppp
            ind_mat[r][team_idx] = opp_idx
        else:
            non_nan = np.count_nonzero(~np.isnan(raw_oe_mat[:,team_idx]))
            raw_oe_mat[non_nan][team_idx] = ppp
            ind_mat[non_nan][team_idx] = opp_idx

    q = """SELECT dppp, teamid, oppid FROM advanced_stats"""
    cur.execute(q)
    result = cur.fetchall()

    raw_de_mat = np.empty((1,nteams))
    raw_de_mat.fill(np.nan)
    for game in result:
        ppp = float(game[0])
        teamid = game[1]
        oppid = game[2]

        r, c = raw_de_mat.shape
        team_idx = team_indices[teamid]
        opp_idx = team_indices[oppid]
        last_entry = raw_de_mat[r-1][team_idx]
        if not np.isnan(last_entry):
            # add a row
            new_row = np.empty((1,c))
            new_row.fill(np.nan)
            raw_de_mat = np.concatenate((raw_de_mat, new_row), axis=0)
            raw_de_mat[r][team_idx] = ppp
        else:
            non_nan = np.count_nonzero(~np.isnan(raw_de_mat[:,team_idx]))
            raw_de_mat[non_nan][team_idx] = ppp


    avg_oe_all =  np.nanmean(raw_oe_mat)
    avg_de_all =  np.nanmean(raw_de_mat)

    # initialize adjusted vectors
    adj_oe = np.zeros(shape=(12,1))
    for team, idx in team_indices.iteritems():
        adj_oe[idx][0] = np.mean(raw_oe_mat[:,idx])

    adj_de = np.zeros(shape=(12,1))
    for team, idx in team_indices.iteritems():
        adj_de[idx][0] = np.mean(raw_de_mat[:,idx])

    cnt = 0
    r_off_arr = []
    r_def_arr = []
    r_off = 1
    r_def = 1
    #print raw_de_dict
    while cnt < 3 and not(r_off < 0.0001 and r_def < 0.0001):
        adj_de_prev = adj_de*1
        adj_oe_prev = adj_oe*1

        for team, idx in team_indices.iteritems():
            ind_oe = ind_mat[:,idx]
            ind_oe = ind_oe[~np.isnan(ind_oe)]
            ind_oe = ind_oe.astype(int)
            raw_oe_vec = raw_oe_mat[:,idx]
            raw_oe_vec = raw_oe_vec[~np.isnan(raw_oe_vec)]
            new_oe = np.nanmean((raw_oe_vec / adj_de[ind_oe]) * avg_oe_all)
            adj_oe[idx] = new_oe

        for team, idx in team_indices.iteritems():
            ind_de = ind_mat[:,idx]
            ind_de = ind_de[~np.isnan(ind_de)]
            ind_de = ind_de.astype(int)
            raw_de_vec = raw_de_mat[:,idx]
            raw_de_vec = raw_de_vec[~np.isnan(raw_de_vec)]
            new_de = np.nanmean((raw_de_vec / adj_de[ind_de]) * avg_de_all)
            adj_de[idx] = new_de


        r_off = np.linalg.norm(adj_oe_prev - adj_oe)
        r_def = np.linalg.norm(adj_de_prev - adj_de)

        r_off_arr.append(r_off)
        r_def_arr.append(r_def)

        cnt += 1

    print r_def_arr
    print r_off_arr

    total_eff = adj_oe - adj_de
    l = ['']*nteams
    for team, idx in team_indices.iteritems():
        l[idx] = team
    
    l = [db_tools.get_team_id(team,col1='ncaaid', col2='ncaa') for team in l]
    
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