from bs4 import BeautifulSoup
import scrape_tools as scrape
from datetime import datetime


def main():
	print missing_data(datetime(2015, 1, 6))


def missing_data(date):
	fmt = '%m/%d/%Y'
	this_year = date.year

	scoreboard_url = scrape.scoreboard_url(date)
	soup = scrape.get_soup(scoreboard_url)
	largest_table = scrape.get_largest_table(soup)

	game_tables = largest_table.findAll('table')
	print len(game_tables)
	team_list, box_link_list = [], []

	for game in game_tables:
    	# skip tables that contain tables
		if len(game.findAll('table')) != 0:
			continue
		

        print 'asdf'
        print 1, 2,3
        print '3'



    	print '1.5'
    	box_link = None
    	print '2'
    	teams = []
    	rows = game.findAll('tr')
    	print '4'
        tds = [row.findAll('td')[0] for row in rows]
        print 'hey3*****'
        # find all the links in the game table
        game_links = game.findAll('a')
        print 'asdasdff'
        # handle all the links
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
                team_string =  link.get_text().strip()
                teamID = url[url.index('=')+1:len(url)]
                teams.append(team_string)

            elif url == '' and idx != 2:
                #if there is no link for the cell, and it isn't the (game score)
                team_string =  td.get_text().strip()
                teams.append(team_string)
            elif 'game/index' in url:
                box_link = scrape.url_to_game_link(url)

            idx += 1

        if box_link is not None and len(teams) == 2:
        	team_list.append(teams)
        	box_link_list.append(box_link)
        elif len(teams) == 2:
        	# there was no link, but the game is there
        	print 'No link for team %s' % teams
	return teams, box_link_list

if __name__ == '__main__':
	main()