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
    #teams = {result[k][1]: k for k in xrange(len(result))}
    nteams = len(teams)
    print teams
    raw_oe_dict = {team: {'oe': np.zeros(shape=(0,1)),
                          'ind': np.zeros(shape=(0,1),dtype=int)} for team in teams}

    raw_oe_mat = np.empty((1,nteams))
    raw_oe_mat.fill(np.nan)
    ind_mat = np.empty((1,nteams))
    ind_mat.fill(np.nan)
    for game in result:
        ppp = float(game[0])
        teamid = game[1]
        oppid = game[2]
        raw_oe_dict[teamid]['oe'] = np.append(raw_oe_dict[teamid]['oe'], float(game[0]))
        raw_oe_dict[teamid]['ind'] = np.append(raw_oe_dict[teamid]['ind'], teams[oppid])
        
        r, c = raw_oe_mat.shape
        team_idx = team_indices[team]
        last_entry = raw_oe_mat[r][team_idx]
        if last_entry != np.nan:
            # add a row
            new_row = np.empty(c)
            new_row.fill(np.nan)
            np.concatenate((raw_oe_mat, new_row), axis=0)
            raw_oe_mat[r+1][team_idx] = ppp
        else:
            non_nan = np.count_nonzero(~np.isnan(raw_oe_mat[:,team_idx]))
            raw_oe_mat[non_nan + 1][team_idx] = ppp





        # do the thing

    for team in teams:
        teamid = team
        raw_oe_dict[teamid]['ind'] =  np.reshape(raw_oe_dict[teamid]['ind'],(raw_oe_dict[teamid]['ind'].shape[0],1))
        raw_oe_dict[teamid]['oe'] =  np.reshape(raw_oe_dict[teamid]['oe'],(raw_oe_dict[teamid]['oe'].shape[0],1))


    avg_oe_vec = np.zeros(shape=(0,0))
    for team in raw_oe_dict:
        avg_oe_vec = np.append(avg_oe_vec, raw_oe_dict[team]['oe'])
    avg_oe_all =  np.mean(avg_oe_vec)

    q = """SELECT dppp, teamid, oppid FROM advanced_stats"""
    cur.execute(q)
    result = cur.fetchall()

    raw_de_dict = {team: {'de': np.zeros(shape=(0,1)),
                          'ind': np.zeros(shape=(0,1),dtype=int)} for team in teams}

    for game in result:
        teamid = game[1]
        oppid = game[2]
        raw_de_dict[teamid]['de'] = np.append(raw_de_dict[teamid]['de'], float(game[0]))
        raw_de_dict[teamid]['ind'] = np.append(raw_de_dict[teamid]['ind'], teams[oppid])
    for team in teams:
        teamid = team
        raw_de_dict[teamid]['ind'] =  np.reshape(raw_de_dict[teamid]['ind'],(raw_de_dict[teamid]['ind'].shape[0],1))
        raw_de_dict[teamid]['de'] =  np.reshape(raw_de_dict[teamid]['de'],(raw_de_dict[teamid]['de'].shape[0],1))
        #print raw_de_dict[teamid]['ind'].shape
    


    avg_de_vec = np.zeros(shape=(0,0))
    for team in raw_de_dict:
        avg_de_vec = np.append(avg_de_vec, raw_de_dict[team]['de'])
    avg_de_all =  np.mean(avg_oe_vec)

    # initialize adjusted vectors
    adj_oe = np.zeros(shape=(12,1))
    for team in teams:
        adj_oe[teams[team]][0] = np.mean(raw_oe_dict[team]['oe'])

    adj_de = np.zeros(shape=(12,1))
    for team in teams:
        adj_de[teams[team]][0] = np.mean(raw_de_dict[team]['de'])

    cnt = 0
    r_off_arr = []
    r_def_arr = []
    r_off = 1
    r_def = 1
    #print raw_de_dict
    while cnt < 3 and not(r_off < 0.0001 and r_def < 0.0001):
        adj_de_prev = adj_de*1
        adj_oe_prev = adj_oe*1

        for team in teams:
            ind_oe = np.ravel(raw_oe_dict[team]['ind'])
            new_oe = np.mean((raw_oe_dict[team]['oe'] / adj_de[ind_oe]) * avg_oe_all)
            adj_oe[teams[team]] = new_oe

            #print raw_oe_dict[team]['oe'], adj_de
            #print avg_oe_all
            #print '--------------'
        for team in teams:
            ind_de = np.ravel(raw_de_dict[team]['ind'])
            new_de = np.mean((raw_de_dict[team]['de'] / adj_de[ind_de]) * avg_de_all)
            adj_de[teams[team]] = new_de


        r_off = np.linalg.norm(adj_oe_prev - adj_oe)
        r_def = np.linalg.norm(adj_de_prev - adj_de)
        print adj_de_prev, adj_de
        print '**********'

        r_off_arr.append(r_off)
        r_def_arr.append(r_def)

        cnt += 1

    print r_def_arr
    print r_off_arr

    total_eff = adj_oe - adj_de
    l = ['']*nteams
    for team in teams:
        idx = teams[team]
        l[idx] = team
    
    l = [db_tools.get_team_id(team,col1='ncaaid', col2='ncaa') for team in l]
    
    rank_arr = adj_de
    ranks = [(l[k], rank_arr[k]) for k in xrange(len(l))]
    ranks = sorted(ranks, key=lambda tup: tup[1])

    for j in xrange(len(ranks)):
        print "#%d %s: %s" % (len(ranks) - j, ranks[j][0], ranks[j][1])






    conn.close()

def main():
    team_rank('')

if __name__ == '__main__':
    main()