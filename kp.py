import psycopg2
import psycopg2.extras
from datetime import datetime, date
import db_tools
import matplotlib.pyplot as plt
import pandas as pd
import csv

def write_kp():
    cur, conn = db_tools.get_cursor()
    f = open('kp_name_map.csv', 'r')
    r = csv.reader(f)
    for line in f:
        words = line.split(',')
        kp = words[0].strip()
        ncaa = words[1].strip()
        #kp = kp.replace("'", "&apos;")
        #ncaa = ncaa.replace("'", "&apos;")

        update = """UPDATE raw_teams
                    SET kenpom = '%s'
                    WHERE ncaa = '%s'
                 """ % (kp.replace("'", "''"), ncaa.replace("'", "''"))
        cur.execute(update)

    conn.commit()
    conn.close()

def kp_to_sql(year):
    cur, conn = db_tools.get_cursor()
    df = pd.read_csv('kenpom/summary%s_pt.csv' % year[-2:])
    df = df[['AdjOE', 'AdjDE', 'TeamName']]

    teamsdf = pd.read_sql("""SELECT kenpom, ncaaid FROM raw_teams""", conn)

    df = pd.merge(teamsdf, df, left_on=['kenpom'], right_on=['TeamName'])
    df['year'] = int(year)
    df = df[['kenpom','ncaaid','AdjOE','AdjDE','year']]
    
    for idx, row in df.iterrows():
        cols = df.columns
        d = {col: row[col] for col in cols}
        #print d
        db_tools.insert_values('kenpom', d, cur=cur)

    conn.commit()
    conn.close()


def main():
    # years = range(2007,2014)
    # for year in years:
    #     kp_to_sql(str(year))
    return None


if __name__ == '__main__':
    main()