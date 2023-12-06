import time
from datetime import datetime

import bs4
import pandas as pd
import requests
from lxml import etree

import procycling.functions as f
from procycling.utils import (
    XPATH_HIST_GEN
)

FIRSTCYCLING_URL = "https://firstcycling.com/"


class RaceScraper(object):

    def __init__(self,
                 start_year: int = 1876,
                 end_year: int = datetime.now().year,
                 gender: str = 'M'):
        self.start_year = start_year
        self.end_year = end_year
        self.gender = gender
        self.url = "https://firstcycling.com/race.php?y={}&t={}&m={}"

    def scrape_races(self):
        t = 2 if self.gender == 'M' else 6
        races = []
        for year in range(self.start_year, self.end_year+1):
            print(f'Year {year} start in {datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")}')
            for month in range(1, 13):
                time.sleep(1)
                s = bs4.BeautifulSoup(requests.get(self.url.format(str(year), str(t), str(month))).content,
                                      'lxml')
                dom = etree.HTML(str(s.find('body')))
                tbl = s.find('tbody')
                if tbl is None:
                    continue
                text = [x.strip('\t').strip('\r') for x in tbl.text.split('\n') if x not in ['', ' ', '\r']]
                text = [[x1, x2, x3, x4, x5] for x1, x2, x3, x4, x5 in zip(text[::5],
                                                                           text[1::5],
                                                                           text[2::5],
                                                                           text[3::5],
                                                                           text[4::5])]
                xpath = [[
                    *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'span'),
                    *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'a', expected_length=2),
                    *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 4, 'span'),
                    *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 4, 'a', expected_length=2)
                ] for tr in range(1, len(tbl.find_all('tr')) + 1)]
                flag_race = [f.re_country_flag(flag[0]) for flag in xpath]
                res_lnk = [FIRSTCYCLING_URL + lnk[1] for lnk in xpath]
                winner_flag = [f.re_country_flag(flag[3]) for flag in xpath]
                rider_id = [f.re_racer_id(rider_id[4]) for rider_id in xpath]
                rider_lnk = [FIRSTCYCLING_URL + rider_id[4] if isinstance(rider_id[4], str) else None for rider_id in xpath]
                races_month = [[res[0], res[1], flag_race, res[2], res_lnk,
                                winner_flag, rider_id, res[3], rider_lnk] for (flag_race,
                                                                               res_lnk,
                                                                               winner_flag,
                                                                               rider_id,
                                                                               rider_lnk,
                                                                               res) in zip(flag_race,
                                                                                           res_lnk,
                                                                                           winner_flag,
                                                                                           rider_id,
                                                                                           rider_lnk,
                                                                                           text)]
                races.extend(races_month)
            print(f'Year {year} finish in {datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")}')
        races = pd.DataFrame(races).drop_duplicates().values.tolist()
        return races


if __name__ == '__main__':
    test = RaceScraper(1876, 2023)
    list_race = test.scrape_races()
    pd.DataFrame(list_race).to_csv('static_race.csv', index=False)
