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
    teams = {result[k][0]: k for k in xrange(len(result))}

    q = """SELECT ppp, teamid, oppid FROM advanced_stats"""
    cur.execute(q)
    result = cur.fetchall()
    #teams = {result[k][1]: k for k in xrange(len(result))}
    nteams = len(teams)
    print teams
    raw_oe_dict = {team: {'oe': np.zeros(shape=(0,1)),
                          'ind': np.zeros(shape=(0,1),dtype=int)} for team in teams}


    for game in result:
        teamid = game[1]
        oppid = game[2]
        raw_oe_dict[teamid]['oe'] = np.append(raw_oe_dict[teamid]['oe'], float(game[0]))
        raw_oe_dict[teamid]['ind'] = np.append(raw_oe_dict[teamid]['ind'], teams[oppid])
    for team in teams:
        teamid = team
        raw_oe_dict[teamid]['ind'] =  np.reshape(raw_oe_dict[teamid]['ind'],(raw_oe_dict[teamid]['ind'].shape[0],1))
        raw_oe_dict[teamid]['oe'] =  np.reshape(raw_oe_dict[teamid]['oe'],(raw_oe_dict[teamid]['oe'].shape[0],1))


    avg_oe_vec = np.zeros(shape=(0,0))
    for team in raw_oe_dict:
        avg_oe_vec = np.append(avg_oe_vec, raw_oe_dict[team]['oe'])
    avg_oe_all =  np.mean(avg_oe_vec)

    q = """SELECT dppp, teamid FROM advanced_stats"""
    cur.execute(q)
    result = cur.fetchall()

    raw_de_dict = {team: {'de': np.zeros(shape=(0,1)),
                          'ind': np.zeros(shape=(0,1),dtype=int)} for team in teams}

    for game in result:
        teamid = game[1]
        raw_de_dict[teamid]['de'] = np.append(raw_de_dict[teamid]['de'], float(game[0]))
        raw_de_dict[teamid]['ind'] = np.append(raw_de_dict[teamid]['ind'], int(teams[teamid]))
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
    while cnt < 100 and not(r_off < 0.0001 and r_def < 0.0001):
        adj_de_prev = adj_de
        adj_oe_prev = adj_oe

        for team in teams:
            
            #adj_de[raw_oe_dict[team]['ind']] = np.ravel(adj_de[raw_oe_dict[team]['ind']])
            ind_oe = np.ravel(raw_oe_dict[team]['ind'])
            adj_oe[teams[team]] = np.sum((raw_oe_dict[team]['oe'] / adj_de[ind_oe]) * avg_oe_all) / len(raw_oe_dict[team]['oe'])
        for team in teams:
            ind_de = np.ravel(raw_de_dict[team]['ind'])
            adj_de[teams[team]] = np.sum((raw_de_dict[team]['de'] / adj_de[ind_de]) * avg_de_all) / len(raw_de_dict[team]['de'])

        r_off = np.linalg.norm(adj_oe_prev - adj_oe)
        r_def = np.linalg.norm(adj_oe_prev - adj_oe)

        r_off_arr.append(r_off)
        r_def_arr.append(r_def)

        cnt += 1

    #print r_def_arr
    #print r_off_arr

    total_eff = adj_oe - adj_de
    l = ['']*nteams
    for team in teams:
        idx = teams[team]
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