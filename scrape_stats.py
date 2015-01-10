from bs4 import BeautifulSoup
import scrape_tools as scrape
from datetime import datetime, timedelta
import db_tools

def get_url(link_type):
    if link_type == 'box':
        l = 'http://stats.ncaa.org/game/index/3518684?org_id=6'

    return l


def get_game_data(soup, game_dict):
    hdrs = {'Game Date', 'Location', 'Attendance', 'Officials'}
    rows = soup.findAll('tr')
    for row in rows:
        bold_cells = row.findAll('td', {'class': 'boldtext'})
        if len(bold_cells) == 0:
            continue

        tds = row.findAll('td')
        hdr = bold_cells[0].get_text().replace(':', '').strip()
        if hdr in hdrs:
            text = tds[1].get_text().strip()
            if hdr == 'Game Date':
                date_args = text.split()
                date = date_args[0]
                date = datetime.strptime(date, '%m/%d/%Y').date()
                game_dict['dt'] = date
            elif hdr == 'Location':
                game_dict['venue'] = text
            elif hdr == 'Attendance':
                game_dict['attendance'] = int(text.replace(',', ''))
            elif hdr == 'Officials':
                officials = text.split(', ')
                game_dict['officials'] = ','.join(officials)
    return game_dict


def get_teams_and_score(box_soup, game_dict):
    cur, conn = db_tools.get_cursor()

    # grab the game summary table
    summary_table = box_soup.findAll('table')[0]
    trs = summary_table.findAll('tr')

    # store away team data
    away_tds = trs[1].findAll('td')
    away_team = away_tds[0].get_text().strip()
    away_team_id = db_tools.get_team_id(cur, str(away_team))
    game_dict['away_team'] = away_team_id
    game_dict['away_first'] = int(away_tds[1].get_text().strip())
    game_dict['away_score'] = int(away_tds[3].get_text().strip())
    
    # store home team data
    home_tds = trs[2].findAll('td')
    home_team = home_tds[0].get_text().strip()
    home_team_id = db_tools.get_team_id(cur, str(home_team))
    game_dict['home_team'] = home_team_id
    game_dict['home_first'] = int(home_tds[1].get_text().strip())
    game_dict['home_score'] = int(home_tds[3].get_text().strip())

    # determine the winner
    if game_dict['home_score'] > game_dict['away_score']:
        game_dict['home_outcome'] = 'W'
    else:
        game_dict['home_outcome'] = 'L'

    return game_dict

def store_game(link):
    this_game = {'home_team': '', 'away_team': '', 'home_outcome': '',
                'home_score': 0, 'away_score': 0, 'neutral_site': False,
                'officials': '', 'attendance': 0, 'venue': '',
                'dt': None}

    # grab the cursor for the database
    cur, conn = db_tools.get_cursor()
    soup = scrape.get_soup(link)

    # fill game dict with data from the link
    this_game = get_game_data(soup, this_game)
    this_game = get_teams_and_score(soup, this_game)

    # store the game in the database
    try:
        db_tools.insert_game(cur, this_game)
        conn.commit()
    except:
        print "Error storing game for %s, %s, on %s" \
              % (this_game['home_team'], this_game['away_team'], this_game['dt'])
        conn.rollback()
    conn.close()


def get_box_rows(soup):

    box_rows = []

    tables = soup.findAll('table', {'class' : 'mytable'})
    for table in tables:
        headers = table.findAll('tr', {'class' : 'heading'})
        if len(headers) == 0:
            continue

        header = headers[0]
        team = header.findAll('td')[0].get_text().strip()

        row = table.findAll('tr', {'class' : 'grey_heading'})[1]
        box_rows.append((str(row),team))

    return box_rows


def raw_box_to_stats(rows):

    # these headers are the headers used by stats.ncaa's box scores
    hdrs = ['Player', 'Pos','MP','FGM','FGA','3FG','3FGA', \
            'FT','FTA','PTS','ORebs','DRebs','Tot Reb', \
            'AST','TO','STL','BLK','Fouls']

    box_data = []
    d = {hdrs[k] : 0 for k in xrange(len(hdrs))}
    # max_score variable will be used to determine who won the game
    j = 0
    for row in rows:
        box_data.append(d.copy())
        team = row[1]
        row_string = row[0]
        box_data[j]['Player'] = team

        # convert row to bs object for parsing
        row = BeautifulSoup(row_string,"html.parser")
        tds = row.findAll('td')

        # column index for each row
        k = 0
        for td in tds:
            string = td.get_text().strip()
            if string == '':
                # ignore empty cells
                k += 1
                continue

            if hdrs[k] == 'Player':
                k += 1
                continue
            elif hdrs[k] == 'Pos':
                k += 1
                continue
            elif hdrs[k] == 'MP':
                if ':' in string:
                    val = int(string[0:string.find(':')])
                    box_data[j][hdrs[k]] = val
            else:
                #some of the values have a '/' appended, so remove it
                val = int(string.replace('/',''))
                box_data[j][hdrs[k]] = val

            k += 1
        j += 1
    return box_data


def main():
    url = 'http://stats.ncaa.org/game/index/3617104?org_id=306'
    store_game(url)
    return None

    cur, conn = db_tools.get_cursor()
    print db_tools.team_exists(cur, home_team)
    print db_tools.team_exists(cur, away_team)


if __name__ == '__main__':
    main()