import numpy as np
import psycopg2
import psycopg2.extras
import db_tools

def team_rank(date):
    cur, conn = db_tools.get_cursor()
    q = """SELECT AVG(ppp), teamid FROM advanced_stats GROUP BY teamid"""
    cur.execute(q)
    result = cur.fetchall()
    teams = {result[k][1]: k for k in xrange(len(result))}
    raw_oe = np.array([float(team[0]) for team in result])

    q = """SELECT AVG(dppp), teamid FROM advanced_stats GROUP BY teamid"""
    cur.execute(q)
    result = cur.fetchall()
    raw_de = np.array([float(team[0]) for team in result])
    
    nteams = len(teams)
    gp = np.zeros(shape=(nteams,nteams))
    nopp = np.zeros(shape=(nteams,1))
    ngp = np.zeros(shape=(nteams,1))
    all_games_q = """SELECT away_team, home_team FROM big_ten"""
    cur.execute(all_games_q)
    all_games = cur.fetchall()
    for game in all_games:
        away_idx = teams[game[0]]
        home_idx = teams[game[1]]
        ngp[away_idx] += 1
        ngp[home_idx] += 1
        gp[home_idx][away_idx] = 1
        gp[away_idx][home_idx] = 1
    
    nopp = np.sum(gp,0)
    opp_adj_oe = np.dot(gp, raw_oe) / nopp
    opp_adj_de = np.dot(gp, raw_de) / nopp
    adj_oe = raw_oe
    adj_de = raw_de

    avg_all_oe = np.mean(raw_oe)
    avg_all_de = np.mean(raw_de)

    cnt = 0
    r_off_arr = []
    r_def_arr = []
    r_off = 1
    r_def = 1
    while cnt < 100 and not(r_off < 0.0001 and r_def < 0.0001):
        adj_de_prev = adj_de
        adj_oe_prev = adj_oe

        adj_oe = (raw_oe / opp_adj_de) * avg_all_oe
        adj_de = (raw_de / opp_adj_oe) * avg_all_de

        opp_adj_de = np.dot(gp, adj_de) / nopp
        opp_adj_oe = np.dot(gp, adj_oe) / nopp

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