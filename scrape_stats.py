from bs4 import BeautifulSoup
import scrape_tools as scrape
from datetime import datetime, timedelta


def get_box_rows(soup):

    rows = soup.findAll('tr', {'class' : 'smtext'})
    trows = soup.findAll('tr')
    trows = [str(row) for row in trows if \
            row.findAll('td')[0].get_text().strip() == 'Totals']

    rows = [str(row) for row in rows]
    rows = rows+trows
    bs_rows = [BeautifulSoup(row, "html.parser") for row in rows]
    td_lens = [len(row.findAll('td')) for row in bs_rows]
    if len(set(td_lens)) > 1:
        print 'rows not all the same length'
        assert False

    return rows

def team_exists(team, type='ncaa'):
    
def home_and_away_teams(box_soup):

    summary_table = box_soup.findAll('table')[0]
    trs = table.findAll('tr')
    away_team = trs[1].findAll('td')[0].get_text().strip()
    home_team = trs[2].findAll('td')[0].get_text().strip()

    return home_team, away_team

def main():
    pass


if __name__ == '__main__':
    main()