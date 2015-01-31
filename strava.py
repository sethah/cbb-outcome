from stravalib.client import Client

import requests as r

if __name__ == '__main__':
    access_token = '535848c783c374e8f33549d22f089c1ce0d56cd6'
    auth_val = values={'access_token':access_token}

    # ath_data = urllib.urlencode (ath_val)

    # ath_req = urllib2.Request(ath_url, ath_data)

    # ath_response = urllib2.urlopen(ath_req)

    # the_page = ath_response.read()
    url = 'https://www.strava.com/api/v3/segments//502495'
    header = {'Authorization': 'Bearer %s' % access_token}
    data = r.get(url, headers=header).json()
    for key in data:
        print key, data[key]