from bs4 import BeautifulSoup
import scrape_tools as scrape
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras



def main():
    start_date = datetime(2013, 11, 9).date()
    end_date = datetime(2014, 3, 12).date()
    day_count = (end_date - start_date).days + 1

    no_links = []
    for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
        teams, box_link_list, missing_links = missing_data(single_date, test_postgres())
        no_links.extend(missing_links)
        print len(no_links)


def test_postgres():
    conn = psycopg2.connect(database="seth", user="seth", password="abc123",
                             host="localhost", port="5432")
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = """SELECT * FROM raw_teams"""
    cur.execute(query)
    rows = cur.fetchall()
    
    return {row['ncaa'] for row in rows}

def missing_data(date, ncaa_names):
    fmt = '%m/%d/%Y'
    scoreboard_url = scrape.scoreboard_url(date)

    soup = scrape.get_soup(scoreboard_url)
    largest_table = scrape.get_largest_table(soup)
    if largest_table is not None:
        game_tables = largest_table.findAll('table')
    else:
        return [], [], []

    team_list, box_link_list, missing_links = [], [], []
    for game in game_tables:
        # skip tables that contain tables
        if len(game.findAll('table')) != 0:
            continue
        box_link = None
        teams = []
        rows = game.findAll('tr')
        tds = [row.findAll('td')[0] for row in rows]
        
        # find all the links in the game table
        game_links = game.findAll('a')

        idx = 0
        for td in tds:
            # get the url in the a tag
            try:
                link = td.findAll('a')[0]
                url = link['href']
            except:
                url = ''

            if 'team/index' in url:
                # handle a team link
                team_string = link.get_text().strip()
                teamID = url[url.index('=')+1:len(url)]
                teams.append(team_string)

            elif url == '' and idx != 2:
                # if there is no link for the cell, and isn't the (game score)
                team_string = td.get_text().strip()
                teams.append(team_string)
            elif 'game/index' in url:
                box_link = scrape.url_to_game_link(url)

            idx += 1

        if box_link is not None and len(teams) == 2:
            team_list.append(teams)
            box_link_list.append(box_link)
        elif len(teams) == 2:
            # there was no link, but the game is there
            if str(teams[0]) in ncaa_names and str(teams[1]) in ncaa_names:
                missing_links.append(teams)
    return teams, box_link_list, missing_links

if __name__ == '__main__':
    main()