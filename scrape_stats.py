from bs4 import BeautifulSoup
import scrape_tools as scrape
import pandas as pd
from datetime import datetime, timedelta, date
import db_tools
import traceback
import csv
import re
from queries import Query
from scrape_tools import Scraper
import re
import traceback
from itertools import izip


class DataPipeline(object):

    def __init__(self, site):
        self.query = Query()
        self.scraper = Scraper()
        self.site = site

        #load teams for easier access
        self.teams()

    def teams(self):
        self.teams = pd.read_sql("""SELECT * FROM raw_teams""", self.query.conn)

    def date_string(self, dt):
        return datetime.strftime(dt, '%Y-%m-%d')

    def date_seq(self, dt):
        return datetime.strftime(dt, '%Y%m%d')

    def get_team(self, team, from_col, to_col):
        try:
            return self.teams[self.teams[from_col] == team][to_col].iloc[0]
        except:
            print "Couldn't find %s" % team

    def get_slug(self, link):
        return link.split('/')[-1]

    def game_link(self, link):
        if self.site == 'statsheet':
            return 'http://statsheet.com' + link

    def update_spread(self, game_data):
        q = """UPDATE games
               SET home_spread
               WHERE dt = '%s'
               AND home_team = %s
               AND away_team = %s""" \
               % (self.date_string(game_data['date']), 
                  game_data['home_team'],
                  game_data['away_team'])

        self.query.query = q
        self.query.execute(commit=True, fetch=False)

    def scoreboard_soup(self, dt):
        if self.site == 'statsheet':
            base = 'http://statsheet.com/mcb/games/scoreboard/'
            url = base + self.date_string(dt)

        elif self.site == 'ncaa':
            year = self.scraper.year_from_date(date)
            date_string = datetime.strftime(date, '%m/%d/%Y')
            prefix = 'http://stats.ncaa.org/team/schedule_list?academic_year='
            suffix = '&division=1.0&sport_code=MBB&schedule_date='
            url = prefix+str(year)+suffix+date_string

        return self.scraper.get_soup(url)

    def game_tables(self, soup):
        if self.site == 'statsheet':
            # get all the tables that are 4x4
            tables = soup.findAll('table')
            game_tables = []
            for table in tables:
                rows = table.findAll('tr')
                if len(rows) == 4 and len(rows[0].findAll('th')) == 4:
                    game_tables.append(table)
            return game_tables

    def update_odds(self, start_date, end_date):
        day_count = (end_date - start_date).days + 1

        for dt in (start_date + timedelta(n) for n in xrange(day_count)):
            print 'asdfasdfasdf'
            soup = self.scoreboard_soup(dt)
            game_tables = self.game_tables(soup)

            games = []
            stored = 0
            total = 0
            for gtable in game_tables:
                game_data = self.process_scoreboard_game(gtable)
                if game_data is None:
                    total += 1
                    print total
                    continue
                if 'home_spread' in game_data:
                    q = """UPDATE games
                           SET home_spread = %s
                           WHERE dt = '%s'
                           AND home_team = %s
                           AND away_team = %s
                        """ % (game_data['dt'], game_data['home_team'], game_data['away_team'])
                    self.query.query = q
                    self.query.execute(commit=True, fetch=False)
                    print 'Updated %s, %s, %s' % (game_data['dt'], game_data['home_team'], game_data['away_team'])


    def scrape_scoreboard(self, start_date, end_date):
        day_count = (end_date - start_date).days + 1

        for dt in (start_date + timedelta(n) for n in xrange(day_count)):
            soup = self.scoreboard_soup(dt)
            game_tables = self.game_tables(soup)

            games = []
            stored = 0
            total = 0
            for gtable in game_tables:
                game_data = self.process_scoreboard_game(gtable)
                if game_data is None:
                    total += 1
                    continue

                game_data['dt'] = dt
                games.append(game_data)

                stored = self.query.insert_values('games', game_data)
                if not stored:
                    total += 1

                # print success message
                print 'Stored game for %s vs %s on %s' \
                % (game_data['home_team'], game_data['away_team'], game_data['dt'])
                
                # track successes
                stored += 1
                total += 1

            # print summary
            msg = '%d out %d games stored on %s' % (stored, total, dt)
            self.scraper.print_msg(msg, '-')

        # close connection
        self.query.conn.close()

    def process_scoreboard_game(self, game_table):
        d = {}
        rows = game_table.findAll('tr')

        # load rows
        if len(rows) < 4:
            return None
        hdr = rows[0]
        away_row = rows[1]
        home_row = rows[2]
        summary_row = rows[3]

        summary_links = summary_row.findAll('th')[0].findAll('a')
        d['box_link'] = summary_links[0]['href']
        if len(summary_links) == 2:
            d['pbp_link'] = summary_links[1]['href']

        # get the teams
        home_tds = home_row.findAll('td')
        home_th = home_row.findAll('th')[0]
        away_tds = away_row.findAll('td')
        away_th = away_row.findAll('th')[0]
        home_slug =  self.get_slug(home_tds[0].find('a')['href'])
        away_slug = self.get_slug(away_tds[0].find('a')['href'])
        d['home_team'] = self.get_team(home_slug, 'statsheet', 'ncaaid')
        d['away_team'] = self.get_team(away_slug, 'statsheet', 'ncaaid')
        d['neutral_site'] = False

        if d['home_team'] is None or d['away_team'] is None:
            return None

        # get scores
        d['home_first'], d['home_second'] = [int(cell.get_text() or 0)
                                             for cell in home_tds[1:3]]
        d['away_first'], d['away_second'] = [int(cell.get_text() or 0)
                                             for cell in away_tds[1:3]]
        d['home_score'] = int(home_th.get_text().strip() or 0)
        d['away_score'] = int(away_th.get_text().strip() or 0)
        if d['home_score'] > d['away_score']:
            d['home_outcome'] = 'W'
        else:
            d['home_outcome'] = 'L'

        if home_tds[-1].get_text().strip() != '':
            # there is a spread
            d['home_spread'] = float(home_tds[-1].get_text().replace('+',''))

        return d

    def get_teams_and_score(box_soup, game_dict):

        # grab the game summary table
        summary_table = box_soup.findAll('table')[0]
        trs = summary_table.findAll('tr')

        # store away team data
        away_tds = trs[1].findAll('td')
        away_scores = get_score_by_half(away_tds)
        away_team = away_tds[0].get_text().strip()
        away_team_id = db_tools.get_team_id(str(away_team), col1='ncaa', col2='ncaaid')
        game_dict['away_team'] = away_team_id
        game_dict['away_first'] = away_scores[0]
        game_dict['away_second'] = away_scores[1]
        game_dict['away_score'] = away_scores[-1]

        # store home team data
        home_tds = trs[2].findAll('td')
        home_scores = get_score_by_half(home_tds)
        home_team = home_tds[0].get_text().strip()
        home_team_id = db_tools.get_team_id(str(home_team), col1='ncaa', col2='ncaaid')
        game_dict['home_team'] = home_team_id
        game_dict['home_first'] = home_scores[0]
        game_dict['home_second'] = home_scores[1]
        game_dict['home_score'] = home_scores[-1]

        # determine the winner
        if game_dict['home_score'] > game_dict['away_score']:
            game_dict['home_outcome'] = 'W'
        else:
            game_dict['home_outcome'] = 'L'

        return game_dict, home_team, away_team

    def get_game_data_ss(self, soup):

        table = soup.findAll('table')[0]
        #first row in table
        first_row = table.findAll('tr')[0]
        first_table = first_row.findAll('table')[0]
        first_team_row = first_table.findAll('tr')[0]
        fifth_td = first_team_row.findAll('td')[4]
        fifth_td_text = fifth_td.get_text().strip()
        if 'OT' in fifth_td_text:
            if fifth_td_text[0:2] == 'OT':
                num_OT = 1
            else:
                num_OT = int(fifth_td_text[0])
        else:
            num_OT = 0

        mp = 200 + num_OT*25

        p = table.findAll('p')[-2:]
        if 'TV:' in p[-1].get_text():
            p = p[-2]
        else:
            p = p[-1]
        p = BeautifulSoup(str(p).replace('<br/>', '/'), 'html.parser')
        game_data = p.get_text().replace('\n','').split('/')
        print game_data
        if len(game_data) == 1:
            venue = game_data[0]
            city, attendance = ('', 0)
        elif len(game_data) == 2:
            venue, city = game_data
            attendance = 0
        elif len(game_data) == 3:
            venue, city, attendance = game_data
            attendance = int(attendance.split()[-1].replace(',',''))
        else:
            venue, city, attendance = ('', '', 0)

        return venue, city, attendance, mp

    def get_game_data(soup, game_dict):
        if soup is None:
            return game_dict

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

    def process_games(self, max_iter=10):
        # get all the games from database that don't have box stats
        q = """SELECT games.id, games.box_link, games.dt 
               FROM games 
               WHERE not exists 
                    (SELECT box_stats.gameid
                     FROM box_stats 
                     WHERE games.id = box_stats.gameid)
               AND games.box_link IS NOT NULL;
            """
        self.query.query = q
        self.query.execute()
        games = self.query.results

        for k, game in enumerate(games):
            link = self.game_link(game[1])
            gameid = game[0]
            dt = game[2]
            soup = self.scraper.get_soup(link)
            venue, city, attendance, mp = self.get_game_data_ss(soup)
            
            # insert these values to the game object
            q = """UPDATE games
                   SET venue = '%s', city = '%s', attendance = %s
                   WHERE id = %s""" % (venue, city, attendance, gameid)
            self.query.query = q
            self.query.execute(commit=True)

            #get the box data in list of dictionaries
            hdr_row, rows = self.get_box_rows(soup)
            box_data = self.raw_box_to_stats_ss(hdr_row, rows, mp, gameid)

            #insert the box data
            for row in box_data:
                stored = self.query.insert_values('box_stats', row)
                if not stored:
                    print "Failed storing game %s on %s" %(gameid, dt)
                    break

            if k >= max_iter:
                break

    def raw_box_to_stats_ss(self, hdr_row, rows, mp, gameid):
        box_data = []
        for row in rows:
            data = self.process_row(row, hdr_row)
            data['gameid'] = gameid
            data['mp'] = mp
            box_data.append(data)

        return box_data

    def process_row(self, row, hdr_row):
        header_dict = self.box_header_map()
        
        d = {}
        for cell, hdr in izip(row.findAll('td'), hdr_row.findAll('th')):
            hdr = hdr.get_text().strip().lower()
            if hdr in header_dict:
                if hdr == 'team':
                    d[header_dict[hdr]] = cell.get_text().strip()
                    a = cell.findAll('a')[0]
                    slug = self.get_slug(a['href'])
                    d[header_dict[hdr]] = self.get_team(slug,
                                                        'statsheet', 'ncaaid')
                else:
                    d[header_dict[hdr]] = int(cell.get_text().strip())

        return d

    def get_score_by_half(tds):
        scores = []
        for td in tds:
            try:
                val = int(td.get_text().strip())
            except:
                continue
            scores.append(val)

        return scores

    def store_games(self, start_date, end_date):
        big_ten = {'301', '306', '312', '418', '416', '428',
                   '463', '509', '539', '518', '559', '796'}
        day_count = (end_date - start_date).days + 1

        for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
            date_string = datetime.strftime(single_date, '%m/%d/%Y')
            teams, box_link_list, missing_games = get_box_links(single_date)

            # write the missing games into a csv
            write_csv(missing_games, 'missing_games.csv')

            msg = 'Storing games for %s' % date_string
            scrape.print_msg(msg)

            stored_count = 0
            not_stored_count = 0
            for link in box_link_list:
                stored = store_game(link)
                if stored:
                    stored_count += 1
                else:
                    not_stored_count += 1

            msg = '%d out of %d games were stored' % \
                  (stored_count, stored_count + not_stored_count)
            scrape.print_msg(msg, '*')

    def store_game(self, link):
        this_game = {'home_team': '', 'away_team': '', 'home_outcome': '',
                     'home_score': 0, 'away_score': 0, 'home_first': 0,
                     'away_first': 0, 'home_second': 0, 'away_second': 0,
                     'neutral_site': False, 'officials': '', 'attendance': 0,
                     'venue': '', 'dt': None}

        soup = scrape.get_soup(link)
        if soup is None:
            return False

        # fill game dict with data from the link
        if self.site == 'ncaa':
            this_game = get_game_data(soup, this_game)
            this_game, home_team, away_team = get_teams_and_score(soup, this_game)
        else:
            hdr_row, team_rows = self.get_game_data_ss(soup, this_game) 
            box_data = self.raw_box_to_stats_ss(hdr_row, team_rows, 12)
        return None
        # store the game in the database
        try:
            # remove this game from the 'error_games.csv'

            db_tools.insert_values('games', this_game, cur=cur)
            conn.commit()

        except Exception, e:
            print "Error storing game for %s, %s, on %s" \
                  % (home_team, away_team, this_game['dt'])

            if this_game['home_team'] is None or this_game['away_team'] is None:
                # error storing game due to null condition
                print "Home team is: %s, and away team is: %s \n" \
                       % (this_game['home_team'], this_game['away_team'])
            elif type(e).__name__ == 'IntegrityError':
                # the game already exists
                print 'This game already exists!\n'
            else:
                error_game = [this_game['home_team'], this_game['away_team'],
                              datetime.strftime(this_game['dt'], '%m/%d/%Y'),
                              type(e).__name__]
                write_csv([error_game], 'error_games.csv')
                print traceback.format_exc()
                print "Generic error, home team is: %s, and away team is: %s \n" \
                    % (this_game['home_team'], this_game['away_team'])
            conn.rollback()
            return False

        # get the gameid from the games table and use as foreign key
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

        return True


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
                team1_exists = db_tools.team_exists(str(teams[0]))
                team2_exists = db_tools.team_exists(str(teams[1]))
                if team1_exists and team2_exists:
                    teams.append(datetime.strftime(date, fmt))
                    missing_links.append(teams)

        return team_list, box_link_list, missing_links


    def get_box_rows(self, soup):

        if self.site == 'statsheet':
            div = soup.findAll('div', {'id': 'basicteamstats'})[0]
            box_table = div.findAll('table')[0]
            rows = box_table.findAll('tr')[0:3]
            hdr_row = rows[0]
            team_rows = rows[1:3]

            return hdr_row, team_rows
        elif self.site == 'ncaa':
            box_rows = []

            tables = soup.findAll('table', {'class': 'mytable'})
            for table in tables:
                headers = table.findAll('tr', {'class': 'heading'})
                if len(headers) == 0:
                    continue

                header = headers[0]
                team = header.findAll('td')[0].get_text().strip()

                row = table.findAll('tr', {'class': 'grey_heading'})[1]
                box_rows.append((str(row), team))

            return box_rows

    def box_header_map(self):
        if self.site == 'statsheet':
            header_dict = {'team': 'teamid', 'fgm': 'fgm',
                       'fga': 'fga', '3pm': 'tpm', '3pa': 'tpa', 'ftm': 'ftm',
                       'fta': 'fta', 'pts': 'pts', 'oreb': 'oreb',
                       'dreb': 'dreb', 'reb': 'reb', 'ast': 'ast',
                       'to': 'turnover', 'stl': 'stl', 'blk': 'blk',
                       'pf': 'pf'}
        elif self.site == 'ncaa':
            header_dict = {'Player': 'teamid', 'MP': 'mp', 'FGM': 'fgm',
                       'FGA': 'fga', '3FG': 'tpm', '3FGA': 'tpa', 'FT': 'ftm',
                       'FTA': 'fta', 'PTS': 'pts', 'ORebs': 'oreb',
                       'DRebs': 'dreb', 'Tot Reb': 'reb', 'AST': 'ast',
                       'TO': 'turnover', 'STL': 'stl', 'BLK': 'blk',
                       'Fouls': 'pf'}
        else:
            header_dict = {}

        return header_dict

    def raw_box_to_stats(self, rows, gameid):

        # these headers are the headers used by stats.ncaa's box scores
        hdrs = ['Player', 'Pos', 'MP', 'FGM', 'FGA', '3FG', '3FGA',
                'FT', 'FTA', 'PTS', 'ORebs', 'DRebs', 'Tot Reb',
                'AST', 'TO', 'STL', 'BLK', 'Fouls']

        header_dict = {'Player': 'teamid', 'MP': 'mp', 'FGM': 'fgm',
                       'FGA': 'fga', '3FG': 'tpm', '3FGA': 'tpa', 'FT': 'ftm',
                       'FTA': 'fta', 'PTS': 'pts', 'ORebs': 'oreb',
                       'DRebs': 'dreb', 'Tot Reb': 'reb', 'AST': 'ast',
                       'TO': 'turnover', 'STL': 'stl', 'BLK': 'blk',
                       'Fouls': 'pf'}

        box_data = []
        d = {header_dict[key]: 0 for key in header_dict}
        for j, row in enumerate(rows):
            box_data.append(d.copy())
            team = row[1]
            row_string = row[0]

            # assign the gameid to this stat
            box_data[j]['gameid'] = gameid

            # get the teamid from team name
            teamid = db_tools.get_team_id(team, col1='ncaa', col2='ncaaid')
            box_data[j]['teamid'] = teamid

            # convert row to bs object for parsing
            row = BeautifulSoup(row_string, "html.parser")
            tds = row.findAll('td')

            # column index for each row
            for k, td in enumerate(tds):
                string = td.get_text().strip()

                # remove non ASCII
                string = re.sub(r'[^\x00-\x7f]', r'', string)

                if string == '':
                    # ignore empty cells
                    continue

                if hdrs[k] == 'Player':
                    continue
                elif hdrs[k] == 'Pos':
                    continue
                elif hdrs[k] == 'MP':
                    if ':' in string:
                        val = int(string[0:string.find(':')])
                        box_data[j][header_dict[hdrs[k]]] = val
                else:
                    # some of the values have a '/' appended, so remove it
                    val = int(string.replace('/', ''))
                    box_data[j][header_dict[hdrs[k]]] = val

        return box_data

    def store_box_stats(self, cur, soup, gameid):

        rows = get_box_rows(soup)
        box_data = raw_box_to_stats(rows, gameid)
        for box_dict in box_data:
            db_tools.insert_values('box_stats', box_dict)

    def get_missing_links(self, missing_teams, year):
        for team_pair in missing_teams:
            for team in team_pair:
                teamid = db_tools.get_team_id(team)
                team_link = scrape.get_url('team', team=teamid, year=year)
                print team_link


    def write_csv(self, rows, fname):
        b = open(fname, 'a')
        a = csv.writer(b)
        a.writerows(rows)
        b.close()


    def drop_duplicate_missing_games(self, fname):
        df = pd.read_csv(fname)
        newdf = df.drop_duplicates()
        newdf.to_csv(fname, index=False)

    def scrape_odds(self, start_date, end_date):
        q = Query()
        day_count = (end_date - start_date).days + 1

        for single_date in (start_date + timedelta(n) for n in xrange(day_count)):
            date_seq = datetime.strftime(single_date, '%Y%m%d')
            date_string = datetime.strftime(single_date, '%Y-%m-%d')
            url = 'http://espn.go.com/mens-college-basketball/lines?date=%s' % date_seq

            soup = scrape.get_soup(url)
            team_rows = soup.findAll('tr', {'class': 'stathead'})

            largest_table = scrape.get_largest_table(soup)
            rows = largest_table.findAll('tr')

            team_flag = False
            d = {}
            cnt = 0
            for row in rows:
                try:
                    if row.get('class') is None:
                        continue
                    if row.get('class')[0] == 'stathead':
                        team_row = row.get_text().split(',')[0]
                        teams = team_row.split(' at ')
                        team1 = re.sub("\d+", "", teams[0]).replace('#', '').strip()
                        team2 = re.sub("\d+","", teams[1]).replace('#', '').strip()
                        # team1 = team1.replace("'", "''")
                        # team2 = team2.replace("'", "''")
                        print team1, team2
                        team_flag = True
                        home_team = q.cursor.execute("""SELECT ncaaid, espn_name FROM raw_teams WHERE espn_name = '%s' OR espn_name = '%s'""" % (team2.replace("'", "''"), team1.replace("'", "''")))
                        for result in q.cursor.fetchall():
                            if result[1] == team1:
                                away_team = result[0]
                                print 'ateam', away_team, team1
                            elif result[1] == team2:
                                home_team = result[0]
                                print 'hteam', home_team, team2

                    elif (row.get('class')[0] == 'evenrow' or row.get('class')[0] == 'oddrow') and team_flag:
                        if len(row.findAll('table')) == 0:
                            continue
                        # table = row.findAll('table')[0]
                        # row = table.findAll('tr')[0]
                        # td = row.findAll('td')[0]
                        tds = row.findAll('td')
                        td = tds[1]
                        if td.get_text().lower().strip() == 'even':
                            home_spread = 0
                            away_spread = 0
                        else:
                            td = td.findAll('td')[0]

                            spreads = BeautifulSoup(str(td).replace('<br/>', ','), 'html.parser')
                            spreads = spreads.get_text().split(',')
                            away_spread = float(spreads[0].replace('+',''))
                            home_spread = float(spreads[1].replace('+',''))
                        print away_spread, home_spread
                        
                        qry = """UPDATE games SET home_spread = %s WHERE dt = '%s' AND (home_team = %s) AND (away_team = %s)""" % (home_spread, date_string, home_team, away_team)
                        q.query = qry
                        q.execute(commit=True)
                        cnt += 1
                        team_flag = False

                except:
                    print traceback.format_exc()

        print cnt

        q.conn.close()


def main():
    d = DataPipeline(site='statsheet')
    start_date = datetime(2013, 11, 9).date()
    end_date = datetime(2013, 11, 9).date()
    # d.scrape_scoreboard(start_date, end_date)
    # d.process_games(max_iter=1)
    d.update_odds(start_date, end_date)
    # print 'asdf'
    # d.store_game('http://statsheet.com/mcb/games/2014/12/17/langston-65-north-texas-78')
    return None


    start_date = datetime(2014, 2, 21).date()
    end_date = datetime(2014, 3, 10).date()
    store_games(start_date, end_date)
    l = 'http://stats.ncaa.org/game/box_score/2694293'
    #store_game(l)
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
