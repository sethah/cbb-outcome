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
        print url
        req = urllib2.Request(url, data=None, headers=self.headers)
        try:
            response = self.opener.open(req)
        except httplib.BadStatusLine as e:
            print e, e.line
        else:
            print 'Success'

        the_page = response.read()
        soup = BeautifulSoup(the_page)
        return soup


def get_soup(link):
    po = Page_Opener()
    try:
        soup = po.open_and_soup(link)
    except:
        #error connecting to link
        soup = None

    return soup


def scoreboard_url(date):
    date_string = datetime.strftime(date, '%m/%d/%Y')
    prefix = 'http://stats.ncaa.org/team/schedule_list?academic_year='
    suffix = '&division=1.0&sport_code=MBB&schedule_date='

    return prefix+str(date.year)+suffix+date_string


def get_largest_table(soup):
    largest_table = None
    max_rows = 0
    for table in soup.findAll('table'):
        number_of_rows = len(table.findAll('tr'))
        if number_of_rows > max_rows:
            largest_table = table
            max_rows = number_of_rows

    return largest_table


def url_to_game_link(url):
    if 'game/index' in url:
        index = url[url.index('/index')+len('index/')+1:url.index('?')]
        return 'http://stats.ncaa.org/game/box_score/'+index
    else:
        return None
