from typing import Optional, Union, List
from pathlib import Path
import os
import re
from datetime import datetime

import bs4
import pandas as pd
import requests
from lxml import etree

from utils import ISO_COUNTRY_CODE, HISTORY_CODE, XPATH_HIST_GEN, XPATH_HIST_YBY, XPATH_HIST_YO

BASE_DIR = Path(os.environ.get("CYCLING_DIR", Path.home() / "procycling"))
DATA_DIR = Path(BASE_DIR, "data")

FIRSTCYCLING_DATADIR = DATA_DIR / "Firstcycling"
FIRSTCYCLING_URL = "https://firstcycling.com/"

NO_CACHE = False
NO_STORE = False

RE_FLAG = re.compile(r"(?<=flag flag-)\w+")
RE_ID = re.compile(r"(?<=r=)\d+")
RE_DATE_TOUR = re.compile(r"\d{2}\.\d{2}-\d{2}\.\d{2}")
RE_DATE_RACE = re.compile(r"\d{2}\.\d{2}")


class FirstCycling(object):
    """

    """

    def __init__(self,
                 season: int,
                 no_cache: bool = NO_CACHE,
                 no_store: bool = NO_STORE,
                 data_dir: Path = FIRSTCYCLING_DATADIR
                 ):
        """

        Args:
            season:
            no_cache:
            no_store:
            data_dir:
        """

        self.season = season
        self.no_cache = no_cache
        self.no_store = no_store
        self.data_dir = data_dir
        if not self.no_store:
            self.data_dir.joinpath("seasons").mkdir(parents=True, exist_ok=True)
            self.data_dir.joinpath("races").mkdir(parents=True, exist_ok=True)
            self.data_dir.mkdir(parents=True, exist_ok=True)

    def read_schedule(self,
                      gender: str = 'M',
                      force_cache: bool = True
                      ) -> pd.DataFrame:
        """

        Args:
            gender:
            force_cache:

        Returns:

        """
        filemask = "seasons/schedule{}_{}.csv"
        filepath = self.data_dir / filemask.format(gender, self.season)

        if not force_cache or self.no_cache:
            all_months = ['0' + str(x) if x < 10 else str(x) for x in range(1, 13)]

            month_now = '0' + str(datetime.now().month) if datetime.now().month < 10 else str(datetime.now().month)

            races_year = pd.DataFrame()
            for month in all_months:
                t = 2 if gender == 'M' else 6
                month_url = FIRSTCYCLING_URL + f"race.php?y={str(self.season)}&t={t}&m={month}"
                month_page = bs4.BeautifulSoup(requests.get(month_url).content, 'lxml')
                body = month_page.find('body')
                tbl_races = body.find('tbody')
                if tbl_races is None:
                    if month == month_now:
                        break
                    else:
                        continue
                races_info = [x.strip('\t').strip('\r') for x in tbl_races.text.split('\n') if x not in ['', ' ', '\r']]
                list_ids = [RE_ID.search(ids['href']).group() for ids in tbl_races.find_all('a', href=True)]
                dom = etree.HTML(str(body))

                if month == month_now:
                    end_races = self._finish_race_in_current_month(races_info)
                    races_info = races_info[:end_races * 5]
                    list_ids = list_ids[: end_races * 3]
                    countries = [
                        [dom.xpath(f'//*[@id="wrapper"]/div[3]/table/tbody/tr[{row}]/td[{td}]/span')[0].values()[0] \
                         for td in range(3, 5)] for row in range(1, end_races + 1)]
                else:
                    countries = [
                        [dom.xpath(f'//*[@id="wrapper"]/div[3]/table/tbody/tr[{row}]/td[{td}]/span')[0].values()[0] \
                         for td in range(3, 5)] for row in range(1, len(tbl_races.find_all('tr')) + 1)]

                list_ids_2d = [[x1, x2] for x1, x2 in zip(list_ids[0::3], list_ids[1::3])]
                list_races = [[dates, cat, rname, win,
                               self._re_country_flag(country[0]),
                               self._re_country_flag(country[1]),
                               ids[0], ids[1]] \
                              for (dates, cat, rname, win, country, ids) in zip(races_info[0::5],
                                                                                races_info[1::5],
                                                                                races_info[2::5],
                                                                                races_info[3::5],
                                                                                countries,
                                                                                list_ids_2d)]

                races_month = (
                    pd.DataFrame(list_races, columns=['Date', 'Category', 'Race_Name', 'Winner',
                                                      'Country_Race', 'Country_Winner',
                                                      'RaceID', 'WinnerID'])
                    .assign(
                        Season_RaceID=lambda df_: ["_".join([str(self.season), race_id]) for race_id in df_.RaceID],
                        Season=self.season,
                        Gender=gender,
                        Profile_Type=lambda df_: ['Stage Race' if x == '2' else 'One Day' for x in
                                                  df_.Category.str[:1]],
                        Start_End=lambda df_: [self._parse_race_dates(dt) for dt in df_.Date],
                    )
                    .assign(
                        Date_Start=lambda df_: [dt[0] for dt in df_.Start_End],
                        Date_End=lambda df_: [dt[1] for dt in df_.Start_End]
                    )
                    .drop(columns='Start_End')
                    .loc[:, ['Season_RaceID', 'Season', 'Date', 'Date_Start', 'Date_End', 'RaceID', 'Race_Name',
                             'Category', 'Gender', 'Profile_Type', 'Country_Race', 'WinnerID', 'Winner',
                             'Country_Winner']]
                )
                races_year = pd.concat([races_year, races_month], axis=0, ignore_index=True)
                if month == month_now:
                    break
            races_year = races_year.drop_duplicates().reset_index(drop=True)
            if not self.no_store:
                races_year.to_csv(filepath, index=False)
        else:
            races_year = pd.read_csv(filepath)
        return races_year

    def read_race_hist_general(self, race_id: Union[int, List[int]]) -> pd.DataFrame:
        info_url = FIRSTCYCLING_URL + "race.php?r={}&k={}"
        overall_tbl = pd.DataFrame()
        for k in range(1, 5):
            page = bs4.BeautifulSoup(requests.get(info_url.format(race_id, k)).content, 'lxml')
            body = page.find("body")
            tbl_history = body.find("tbody")
            dom = etree.HTML(str(body))

            hist_df = (
                pd.DataFrame([[
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 1, 'a'),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 2, return_text=True),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'a', expected_length=3, check_information=True),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 4, 'span'),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 4, 'a', expected_length=2),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 5, 'span'),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 5, 'a', expected_length=2),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 6, 'span'),
                    *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 6, 'a', expected_length=2),
                ] for tr in range(1, len(tbl_history.find_all('tr')) + 1)],
                    columns=['Year', 'Category', 'Information', 'RaceLink', 'Results',
                             'WinnerCountry', 'WinnerID', 'Winner', 'SecondCountry', 'SecondID',
                             'Second', 'ThirdCountry', 'ThirdID', 'Third'])
                .pipe(lambda df_: df_.loc[df_.Information != 'Information']).reset_index(drop=True)
                .drop(columns=['Information', 'Results'])
                .assign(
                    Year=lambda df_: [int(re.search(r"(?<=y=)\d{4}", year).group()) for year in df_.Year],
                    RaceLink=lambda df_: FIRSTCYCLING_URL + df_.RaceLink,
                    WinnerCountry=lambda df_: [self._re_country_flag(flag) for flag in df_.WinnerCountry],
                    WinnerLink=lambda df_: FIRSTCYCLING_URL + df_.WinnerID,
                    WinnerID=lambda df_: [self._re_racer_id(racer_id) for racer_id in df_.WinnerID],
                    SecondCountry=lambda df_: [self._re_country_flag(flag) for flag in df_.SecondCountry],
                    SecondLink=lambda df_: FIRSTCYCLING_URL + df_.SecondID,
                    SecondID=lambda df_: [self._re_racer_id(racer_id) for racer_id in df_.SecondID],
                    ThirdCountry=lambda df_: [self._re_country_flag(flag) for flag in df_.ThirdCountry],
                    ThirdLink=lambda df_: FIRSTCYCLING_URL + df_.ThirdID,
                    ThirdID=lambda df_: [self._re_racer_id(racer_id) for racer_id in df_.ThirdID],
                    Classification=HISTORY_CODE[k]
                )
                .loc[:, ['Classification', 'Year', 'Category', 'RaceLink', 'WinnerCountry', 'WinnerID', 'Winner',
                         'WinnerLink', 'SecondCountry', 'SecondID', 'Second', 'SecondLink', 'ThirdCountry',
                         'ThirdID', 'Third', 'ThirdLink']]
            )
            overall_tbl = pd.concat([overall_tbl, hist_df], axis=0, ignore_index=True)
        return overall_tbl

    def read_race_hist_yby(self, race_id: int, convert_to_sec: bool = False) -> pd.DataFrame:
        yby_url = FIRSTCYCLING_URL + "race.php?r={}&k=X"
        page = bs4.BeautifulSoup(requests.get(yby_url.format(race_id)).content, 'lxml')
        body = page.find("body")
        years = [year.text.strip('\n') for year in body.find_all('thead')]
        tbl_yby = body.find_all("tbody")
        dom = etree.HTML(str(body))
        overall_tbl = pd.DataFrame()
        for i, tbl_year in enumerate(tbl_yby):
            text = [x.strip('\t') for x in tbl_year.text.split('\n') if x not in ['', ' ', '\r']]
            position = [int(pos) for pos in text[::2]]
            results = [[re.sub(r"(\d{1,3}:\d{2}:\d{2})|(\+.+)", "", x),
                        re.search(r"(\d{1,3}:\d{2}:\d{2})|((?<=\+ ).+)|(0)", x)] for x in text[1::2]]
            results = [[rider, None] if time is None else [rider, time.group()] for rider, time in results]
            xpath_year = [[
                *self._xpath_element(dom, XPATH_HIST_YBY, i+2, tr, 2, 'span'),
                *self._xpath_element(dom, XPATH_HIST_YBY, i+2, tr, 3, 'a')
            ] for tr in range(1, len(tbl_year.find_all('td')) + 1)]
            countries = [self._re_country_flag(flag[0]) for flag in xpath_year]
            riders_id = [self._re_racer_id(rider_id[1]) for rider_id in xpath_year]
            riders_link = [rider_id[1] for rider_id in xpath_year]
            year = (
                pd.DataFrame([[pos, country, r_id, r_link, *res] for pos, country, r_id, r_link, res in zip(
                    position, countries, riders_id, riders_link, results
                )],
                             columns=['Position', 'RiderCountry', 'RiderID', 'RiderLink', 'Rider', 'Time'])
                .assign(
                    Year=years[i],
                    RiderLink=lambda df_: FIRSTCYCLING_URL + df_.RiderLink
                )
            )
            overall_tbl = pd.concat([overall_tbl, year], axis=0, ignore_index=True)
        if convert_to_sec:
            overall_tbl = (
                overall_tbl
                .merge(
                    (
                        overall_tbl
                        .pipe(lambda df_: df_.loc[df_.Position == 1])
                        .drop(columns=['Position', 'Rider', 'RiderLink', 'RiderCountry', 'RiderID'])
                        .reset_index(drop=True)
                    ),
                    how='inner',
                    on=['Year']
                )
                .assign(Time_Winner=lambda df_: df_.Time_x == df_.Time_y)
                .assign(
                    Time_x=lambda df_: [self._convert_to_seconds(race_time) for race_time in df_.Time_x],
                    Time_y=lambda df_: [self._convert_to_seconds(race_time) for race_time in df_.Time_y]
                )
                .assign(
                    Time_x=lambda df_: [x + y if z is False else x for (x, y, z) in zip(
                        df_.Time_x,
                        df_.Time_y,
                        df_.Time_Winner
                    )]
                )
                .drop(columns=['Time_y', 'Time_Winner'])
                .rename(columns={'Time_x': 'Time'})
            )
        else:
            overall_tbl = (
                overall_tbl
                .assign(Time=lambda df_: ['+' + t if pos != 1 and t is not None else t for t, pos in zip(
                    df_.Time, df_.Position
                )])
            )
        return overall_tbl

    def read_race_hist_victories(self, race_id: int) -> pd.DataFrame:
        victory_url = FIRSTCYCLING_URL + "race.php?r={}&k=W"
        page = bs4.BeautifulSoup(requests.get(victory_url.format(race_id)).content, 'lxml')
        body = page.find("body")
        tbl_victory = body.find("tbody")
        dom = etree.HTML(str(body))

        text = [x.strip('\t').strip('\r') for x in tbl_victory.text.split('\n') if x not in ['', ' ', '\r']]
        text = [x.strip(' ').strip('\t') for x in text if re.search(r'(\w)|(\d)', x) is not None]
        results = [[pos, rider, country_name, first, second, third] \
                   for pos, rider, country_name, first, second, third in zip(
            text[::6],
            text[1::6],
            text[2::6],
            text[3::6],
            text[4::6],
            text[5::6]
        )]

        xpath_winner = [[
            *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'span'),
            *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 2, 'a')
        ] for tr in range(1, len(tbl_victory.find_all('tr')) + 1)]
        countries = [self._re_country_flag(flag[0]) for flag in xpath_winner]
        riders_id = [self._re_racer_id(rider_id[1]) for rider_id in xpath_winner]
        riders_link = [rider_id[1] for rider_id in xpath_winner]
        winner = (
            pd.DataFrame(
                [[country, r_id, r_link, *res] for country, r_id, r_link, res in zip(
                    countries, riders_id, riders_link, results
                )],
                columns=['RiderCountry', 'RiderID', 'RiderLink', 'Position', 'Rider', 'CountryName',
                         'FirstPlace', 'SecondPlace', 'ThirdPlace'])
            .assign(RiderLink=lambda df_: FIRSTCYCLING_URL + df_.RiderLink)
            .loc[:, ['Position', 'RiderCountry', 'RiderID', 'RiderLink', 'Rider', 'CountryName',
                     'FirstPlace', 'SecondPlace', 'ThirdPlace']]
        )
        return winner

    def read_race_hist_stages(self, race_id: int) -> pd.DataFrame:
        stages_url = FIRSTCYCLING_URL + "race.php?r={}&k=Z"
        page = bs4.BeautifulSoup(requests.get(stages_url.format(race_id)).content, 'lxml')
        body = page.find("body")
        tbl_stages = body.find("tbody")
        dom = etree.HTML(str(body))

        text = [x.strip('\t').strip('\r') for x in tbl_stages.text.split('\n') if x not in ['', ' ', '\r']]
        text = [x.strip(' ').strip('\t') for x in text if re.search(r'(\w)|(\d)', x) is not None]
        results = [[pos, rider, country_name, win] for pos, rider, country_name, win in zip(
            text[::4],
            text[1::4],
            text[2::4],
            text[3::4]
        )]
        xpath_stage = [[
            *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'span'),
            *self._xpath_element(dom, XPATH_HIST_GEN, 3, tr, 2, 'a')
        ] for tr in range(1, len(tbl_stages.find_all('tr')) + 1)]
        countries = [self._re_country_flag(flag[0]) for flag in xpath_stage]
        riders_id = [self._re_racer_id(rider_id[1]) for rider_id in xpath_stage]
        riders_link = [rider_id[1] for rider_id in xpath_stage]
        win_stages = (
            pd.DataFrame(
                [[country, r_id, r_link, *res] for country, r_id, r_link, res in zip(
                    countries, riders_id, riders_link, results
                )],
                columns=['RiderCountry', 'RiderID', 'RiderLink', 'Position', 'Rider', 'CountryName', 'WinStages'])
            .assign(RiderLink=lambda df_: FIRSTCYCLING_URL + df_.RiderLink)
            .loc[:, ['Position', 'RiderCountry', 'RiderID', 'RiderLink', 'Rider', 'CountryName', 'WinStages']]
        )
        return win_stages

    def read_race_hist_young_old_win(self, race_id: int) -> pd.DataFrame:
        yby_url = FIRSTCYCLING_URL + "race.php?r={}&k=Y"
        page = bs4.BeautifulSoup(requests.get(yby_url.format(race_id)).content, 'lxml')
        body = page.find("body")
        tbl_yo = body.find_all("tbody")
        dom = etree.HTML(str(body))
        overall_tbl = pd.DataFrame()
        for i, yo in enumerate(tbl_yo):
            text = [x.strip('\t').strip(' ') for x in yo.text.split('\n') if x not in ['', ' ', '\r']]
            if self._is_blank(text):
                return None
            tbl = [[year, rider, country_name, age] for year, rider, country_name, age in zip(text[::4],
                                                                                              text[1::4],
                                                                                              text[2::4],
                                                                                              text[3::4])]
            xpath_year = [[
                *self._xpath_element(dom, XPATH_HIST_YO, 3, tr, 3, 'span', table=i+1),
                *self._xpath_element(dom, XPATH_HIST_YO, 3, tr, 2, 'a', table=i+1)
            ] for tr in range(1, len(yo.find_all('td')) + 1)]
            countries = [self._re_country_flag(flag[0]) for flag in xpath_year]
            riders_id = [self._re_racer_id(rider_id[1]) for rider_id in xpath_year]
            riders_link = [rider_id[1] for rider_id in xpath_year]
            age_winner = (
                pd.DataFrame([[country, r_id, r_link, *info] for country, r_id, r_link, info in zip(
                    countries, riders_id, riders_link, tbl
                )],
                             columns=['RiderCountry', 'RiderID', 'RiderLink', 'Year', 'Rider', 'CountryName', 'Age'])
                .assign(
                    AgeType='Youngest' if i == 0 else 'Oldest',
                    RiderLink=lambda df_: FIRSTCYCLING_URL + df_.RiderLink
                )
                .loc[:, ['Year', 'AgeType', 'RiderCountry', 'RiderID', 'RiderLink', 'Rider', 'CountryName', 'Age']]
            )
            overall_tbl = pd.concat([overall_tbl, age_winner], axis=0, ignore_index=True)
        return overall_tbl

    def read_race(self,
                  race_id: Optional[Union[int, List[int]]] = None,
                  force_cache: bool = True,
                  live: bool = True
                  ):
        """

        Args:
            race_id:
            force_cache:
            live:

        Returns:

        """
        pass

    def _parse_race_dates(self, date: str):
        d = date.split('-')
        if len(d) == 1:
            return [
                datetime.strptime('.'.join([d[0], str(self.season)]), '%d.%M.%Y').strftime('%Y/%M/%d'),
                datetime.strptime('.'.join([d[0], str(self.season)]), '%d.%M.%Y').strftime('%Y/%M/%d')
            ]
        else:
            return [
                datetime.strptime('.'.join([d[0], str(self.season)]), '%d.%M.%Y').strftime('%Y/%M/%d'),
                datetime.strptime('.'.join([d[1], str(self.season)]), '%d.%M.%Y').strftime('%Y/%M/%d')
            ]

    @staticmethod
    def _re_country_flag(flag: str):
        if flag is None:
            return None
        try:
            iso_code = RE_FLAG.search(flag).group()
            country = ISO_COUNTRY_CODE[iso_code.upper()]
        except AttributeError:
            country = 'UCI'
        except KeyError:
            country = 'UNK'
        return country

    @staticmethod
    def _re_racer_id(racer_id: str):
        if racer_id is None:
            return None
        return int(RE_ID.search(racer_id).group())

    def _finish_race_in_current_month(self, races_info):
        dt = [x for x in races_info if (RE_DATE_RACE.search(x) is not None) or (RE_DATE_RACE.search(x) is not None)]
        return len([1 for date in dt if datetime.strptime(self._parse_race_dates(date)[1],
                                                          "%Y/%M/%d").day < datetime.now().day])

    @staticmethod
    def _xpath_element(
            dom: etree._Element,
            base_xpath: str,
            div: int,
            tr: int,
            td: int,
            tag: str = None,
            table: int = None,
            expected_length: int = 1,
            check_information: bool = False,
            return_text: bool = False
    ):
        if table is not None:
            base_xpath = base_xpath.format(str(div), str(table), str(tr), str(td))
        else:
            base_xpath = base_xpath.format(str(div), str(tr), str(td))
        xpath = base_xpath + '/' + tag if tag is not None else base_xpath
        try:
            res = dom.xpath(xpath)[0].values()
        except IndexError:
            res = [None] * expected_length
        if check_information:
            if dom.xpath(xpath)[0].text == 'Information':
                return ['Information'] * expected_length
        if return_text:
            res = [dom.xpath(xpath)[0].text]
        return res

    @staticmethod
    def _convert_to_seconds(race_time: str) -> int:
        race_time = race_time.split(':')
        if len(race_time) == 1:
            return int(race_time[0])
        elif len(race_time) == 2:
            return int(race_time[0]) * 60 + int(race_time[1])
        elif len(race_time) == 3:
            return int(race_time[0]) * 3600 + int(race_time[0]) * 60 + int(race_time[1])

    @staticmethod
    def _is_blank(data: List[str]) -> bool:
        info = set([x for x in data[2::3]])
        return info == {'Information---'}


if __name__ == '__main__':
    cycling = FirstCycling(season=2023)
    # df = cycling.read_schedule(force_cache=True)
    hist1 = cycling.read_race_hist_general(667)
    hist2 = cycling.read_race_hist_young_old_win(667)
    hist3 = cycling.read_race_hist_yby(667, convert_to_sec=False)
    hist4 = cycling.read_race_hist_victories(667)
    hist5 = cycling.read_race_hist_stages(667)
    a = 1
