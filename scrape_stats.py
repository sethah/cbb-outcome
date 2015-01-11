from bs4 import BeautifulSoup
import scrape_tools as scrape
from datetime import datetime, timedelta, date
import db_tools
import traceback
import csv


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

    # grab the game summary table
    summary_table = box_soup.findAll('table')[0]
    trs = summary_table.findAll('tr')

    # store away team data
    away_tds = trs[1].findAll('td')
    away_team = away_tds[0].get_text().strip()
    away_team_id = db_tools.get_team_id(str(away_team))
    game_dict['away_team'] = away_team_id
    game_dict['away_first'] = int(away_tds[1].get_text().strip())
    game_dict['away_score'] = int(away_tds[3].get_text().strip())
    
    # store home team data
    home_tds = trs[2].findAll('td')
    home_team = home_tds[0].get_text().strip()
    home_team_id = db_tools.get_team_id(str(home_team))
    game_dict['home_team'] = home_team_id
    game_dict['home_first'] = int(home_tds[1].get_text().strip())
    game_dict['home_score'] = int(home_tds[3].get_text().strip())

    # determine the winner
    if game_dict['home_score'] > game_dict['away_score']:
        game_dict['home_outcome'] = 'W'
    else:
        game_dict['home_outcome'] = 'L'

    return game_dict


def store_games(start_date, end_date):
    start_date = datetime(2013, 11, 9).date()
    end_date = datetime(2013, 11, 9).date()
    day_count = (end_date - start_date).days + 1

    for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
        teams, box_link_list, missing_games = get_box_links(single_date)
        for link in box_link_list:
            store_game(link)


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
        db_tools.insert_values('games', this_game, cur=cur)
        conn.commit()
        
    except Exception, e:
        print traceback.format_exc()
        print "Error storing game for %s, %s, on %s" \
              % (this_game['home_team'], this_game['away_team'], this_game['dt'])
        conn.rollback()
        return None

    cur.execute("""SELECT id
                       FROM games
                       WHERE home_team = '%s' 
                       AND away_team = '%s'
                       AND dt = '%s'""" 
                       % (this_game['home_team'], 
                        this_game['away_team'], this_game['dt']))
    
    gameid = cur.fetchone()

    # store the box stats for the game
    rows = get_box_rows(soup)
    box_data = raw_box_to_stats(rows, gameid)
    for box_dict in box_data:
        db_tools.insert_values('box_stats', box_dict, cur=cur)
    conn.commit()
    conn.close()


def get_box_links(date):
    fmt = '%m/%d/%Y'
    scoreboard_url = scrape.scoreboard_url(date)

    soup = scrape.get_soup(scoreboard_url)

    # find the largest table in the soup
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
            # box link was found and two teams were found
            team_list.append(teams)
            box_link_list.append(box_link)
        elif len(teams) == 2:
            # there was no link, but the game is there
            if db_tools.team_exists(str(teams[0])) \
            and db_tools.team_exists(str(teams[1])):
                teams.append(datetime.strftime(date, fmt))
                missing_links.append(teams)

    return teams, box_link_list, missing_links


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


def raw_box_to_stats(rows, gameid):

    # these headers are the headers used by stats.ncaa's box scores
    hdrs = ['Player', 'Pos','MP','FGM','FGA','3FG','3FGA',
            'FT','FTA','PTS','ORebs','DRebs','Tot Reb',
            'AST','TO','STL','BLK','Fouls']

    header_dict = {'Player': 'teamid', 'MP': 'mp', 'FGM': 'fgm',
                   'FGA': 'fga', '3FG': 'tpm', '3FGA': 'tpa', 'FT': 'ftm',
                   'FTA': 'fta', 'PTS': 'pts', 'ORebs': 'oreb',
                   'DRebs': 'dreb', 'Tot Reb': 'reb', 'AST': 'ast',
                   'TO': 'turnover', 'STL': 'stl', 'BLK': 'blk',
                   'Fouls': 'pf'}

    box_data = []
    d = {header_dict[key] : 0 for key in header_dict}
    # max_score variable will be used to determine who won the game
    j = 0
    for row in rows:
        box_data.append(d.copy())
        team = row[1]
        row_string = row[0]

        # assign the gameid to this stat
        box_data[j]['gameid'] = gameid

        # get the teamid from team name
        teamid = db_tools.get_team_id(team, col='ncaa')
        box_data[j]['teamid'] = teamid

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
                    box_data[j][header_dict[hdrs[k]]] = val
            else:
                #some of the values have a '/' appended, so remove it
                try:
                    val = int(string.replace('/',''))
                except:
                    print val, hdrs[k]
                    val = 0
                box_data[j][header_dict[hdrs[k]]] = val

            k += 1
        j += 1
    return box_data


def store_box_stats(cur, soup, gameid):

    rows = get_box_rows(soup)
    box_data = raw_box_to_stats(rows, gameid)
    for box_dict in box_data:
        db_tools.insert_values('box_stats', box_dict)


def get_missing_links(missing_teams, year):
    for team_pair in missing_teams:
        for team in team_pair:
            teamid = db_tools.get_team_id(team)
            team_link = scrape.get_url('team', team=teamid, year=year)
            print team_link


def write_missing_links(missing_games):
    b = open('missing_games.csv', 'a')
    a = csv.writer(b)
    a.writerows(missing_games)
    b.close()


def main():
    store_games('start_date', 'end_date')
    return None
    the_date = date(2014, 12, 19)
    year = scrape.year_from_date(the_date)
    teams, box_link_list, missing_games = get_box_links(the_date)
    write_missing_links(missing_games)
    print missing_games
    #get_missing_links(missing_teams, year)
    return None

    cur, conn = db_tools.get_cursor()
    print db_tools.team_exists(cur, home_team)
    print db_tools.team_exists(cur, away_team)


if __name__ == '__main__':
    main()