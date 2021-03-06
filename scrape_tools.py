from bs4 import BeautifulSoup
from datetime import datetime
import urllib2
import cookielib
import httplib



class Page_Opener:

    def __init__(self):
        self.cookiejar = cookielib.LWPCookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(
            self.cookiejar))
        self.agent = 'Mozilla/4.0 (compatible; MSIE 6.0; \
            Windows NT 5.1; SV1; NeosBrowser; .NET CLR 1.1.4322; \
            .NET CLR 2.0.50727)'
        self.headers = {'User-Agent': self.agent}

    def open_and_soup(self, url, data=None):
        req = urllib2.Request(url, data=None, headers=self.headers)
        try:
            response = self.opener.open(req)
        except httplib.BadStatusLine as e:
            print e, e.line
        else:
            pass

        the_page = response.read()
        soup = BeautifulSoup(the_page)
        return soup

class Scraper(object):

    def __init__(self):
        pass

    def get_soup(self, link):
        po = Page_Opener()
        try:
            soup = po.open_and_soup(link)
        except:
            #error connecting to link
            soup = None

        return soup

    def get_ncaa_yearid(self, the_year):
        year_dict = {2015: 12020, 2014: 11540, 2013: 11220}

        return year_dict[the_year]

    def year_from_date(self, date):
        month = date.month
        if month > 9:
            year = date.year + 1
        else:
            year = date.year

        return year

    def get_largest_table(self, soup):
        if soup is None:
            return None

        tables = soup.findAll('table')
        if len(tables) == 0:
            return None

        largest_table = None
        max_rows = 0
        for table in soup.findAll('table'):
            number_of_rows = len(table.findAll('tr'))
            if number_of_rows > max_rows:
                largest_table = table
                max_rows = number_of_rows

        return largest_table

    def url_to_game_link(self, url):
        if 'game/index' in url:
            index = url[url.index('/index')+len('index/')+1:url.index('?')]
            return 'http://stats.ncaa.org/game/box_score/'+index
        else:
            return None

    def print_msg(self, msg, char='-'):
        l = len(msg)
        print char*l
        print msg
        print char*l+'\n'

    def get_url(self, link_type, **kwargs):
        if link_type == 'box':
            link = 'http://stats.ncaa.org/game/index/3518684?org_id=6'
        elif link_type == 'team':
            year = kwargs['year']
            year = str(get_ncaa_yearid(year))
            teamid = kwargs['team']
            link = 'http://stats.ncaa.org/team/index/'+year+'?org_id='+teamid

        return link








