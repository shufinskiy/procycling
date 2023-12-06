import json
from typing import Optional, Union, List, Dict
from pathlib import Path
import os
import re
from datetime import datetime

import bs4
import pandas as pd
import requests
from lxml import etree

import functions as f
from utils import (
    RE_ID,
    HISTORY_CODE,
    XPATH_HIST_GEN,
    XPATH_HIST_YBY,
    XPATH_HIST_YO
)

BASE_DIR = Path(os.environ.get("CYCLING_DIR", Path.home() / "procycling"))
DATA_DIR = Path(BASE_DIR, "data")

FIRSTCYCLING_DATADIR = DATA_DIR / "Firstcycling"
FIRSTCYCLING_URL = "https://firstcycling.com/"

NO_CACHE = False
NO_STORE = False


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
                    end_races = f.finish_race_in_current_month(self.season, races_info)
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
                               f.re_country_flag(country[0]),
                               f.re_country_flag(country[1]),
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
                        Start_End=lambda df_: [f.parse_race_dates(self.season, dt) for dt in df_.Date],
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

    def read_race_history(self,
                          race_id: int,
                          force_cache: bool = True
                          ) -> Dict:
        foldermask = "races/race_{}"
        folderpath = self.data_dir / foldermask.format(race_id)
        hist_race = {}
        if not force_cache or self.no_cache:
            general = self.read_race_hist_general(race_id, force_cache=False)
            yby = self.read_race_hist_yby(race_id, force_cache=False)
            victory = self.read_race_hist_victories(race_id, force_cache=False)
            stage = self.read_race_hist_stages(race_id, force_cache=False)
            age_winner = self.read_race_hist_young_old_win(race_id, force_cache=False)
            if not self.no_store:
                folderpath.mkdir(parents=True, exist_ok=True)
                hist_race['general'] = {
                    'headers': f.convert_dataframe_to_json(general, True),
                    'data': f.convert_dataframe_to_json(general)
                }
                hist_race['yby'] = {
                    'headers': f.convert_dataframe_to_json(yby, True),
                    'data': f.convert_dataframe_to_json(yby)
                }
                hist_race['victory'] = {
                    'headers': f.convert_dataframe_to_json(victory, True),
                    'data': f.convert_dataframe_to_json(victory)
                }
                hist_race['stage'] = {
                    'headers': f.convert_dataframe_to_json(stage, True),
                    'data': f.convert_dataframe_to_json(stage)
                }
                hist_race['age_winner'] = {
                    'headers': f.convert_dataframe_to_json(age_winner, True),
                    'data': f.convert_dataframe_to_json(age_winner)
                }
                with Path(folderpath.joinpath('history_race.json')).open('w') as file:
                    json.dump(hist_race, file)
        else:
            with Path(folderpath.joinpath('history_race.json')).open('r') as file:
                hist_race = json.load(file)
        return hist_race

    def read_race_hist_general(self,
                               race_id: Union[int, List[int]],
                               force_cache: bool = True) -> pd.DataFrame:
        overall_tbl = pd.DataFrame()
        if not force_cache or self.no_cache:
            info_url = FIRSTCYCLING_URL + "race.php?r={}&k={}"
            for k in range(1, 5):
                page = bs4.BeautifulSoup(requests.get(info_url.format(race_id, k)).content, 'lxml')
                body = page.find("body")
                tbl_history = body.find("tbody")
                dom = etree.HTML(str(body))

                hist_df = (
                    pd.DataFrame([[
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 1, 'a'),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 2, return_text=True),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'a', expected_length=3, check_information=True),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 4, 'span'),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 4, 'a', expected_length=2),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 5, 'span'),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 5, 'a', expected_length=2),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 6, 'span'),
                        *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 6, 'a', expected_length=2),
                    ] for tr in range(1, len(tbl_history.find_all('tr')) + 1)],
                        columns=['Year', 'Category', 'Information', 'RaceLink', 'Results',
                                 'WinnerCountry', 'WinnerID', 'Winner', 'SecondCountry', 'SecondID',
                                 'Second', 'ThirdCountry', 'ThirdID', 'Third'])
                    .pipe(lambda df_: df_.loc[df_.Information != 'Information']).reset_index(drop=True)
                    .drop(columns=['Information', 'Results'])
                    .assign(
                        Year=lambda df_: [int(re.search(r"(?<=y=)\d{4}", year).group()) for year in df_.Year],
                        RaceLink=lambda df_: FIRSTCYCLING_URL + df_.RaceLink,
                        WinnerCountry=lambda df_: [f.re_country_flag(flag) for flag in df_.WinnerCountry],
                        WinnerLink=lambda df_: FIRSTCYCLING_URL + df_.WinnerID,
                        WinnerID=lambda df_: [f.re_racer_id(racer_id) for racer_id in df_.WinnerID],
                        SecondCountry=lambda df_: [f.re_country_flag(flag) for flag in df_.SecondCountry],
                        SecondLink=lambda df_: FIRSTCYCLING_URL + df_.SecondID,
                        SecondID=lambda df_: [f.re_racer_id(racer_id) for racer_id in df_.SecondID],
                        ThirdCountry=lambda df_: [f.re_country_flag(flag) for flag in df_.ThirdCountry],
                        ThirdLink=lambda df_: FIRSTCYCLING_URL + df_.ThirdID,
                        ThirdID=lambda df_: [f.re_racer_id(racer_id) for racer_id in df_.ThirdID],
                        Classification=HISTORY_CODE[k]
                    )
                    .loc[:, ['Classification', 'Year', 'Category', 'RaceLink', 'WinnerCountry', 'WinnerID', 'Winner',
                             'WinnerLink', 'SecondCountry', 'SecondID', 'Second', 'SecondLink', 'ThirdCountry',
                             'ThirdID', 'Third', 'ThirdLink']]
                )
                overall_tbl = pd.concat([overall_tbl, hist_df], axis=0, ignore_index=True)
        else:
            foldermask = "races/race_{}"
            folderpath = self.data_dir / foldermask.format(race_id)
            with Path(folderpath.joinpath('history_race.json')).open('r') as file:
                hist_race = json.load(file)
            data = hist_race['general']['data']
            headers = hist_race['general']['headers']
            overall_tbl = pd.DataFrame(data, columns=headers)
        return overall_tbl

    def read_race_hist_yby(self,
                           race_id: int,
                           convert_to_sec: bool = False,
                           force_cache: bool = True) -> pd.DataFrame:
        overall_tbl = pd.DataFrame()
        if not force_cache or self.no_cache:
            yby_url = FIRSTCYCLING_URL + "race.php?r={}&k=X"
            page = bs4.BeautifulSoup(requests.get(yby_url.format(race_id)).content, 'lxml')
            body = page.find("body")
            years = [year.text.strip('\n') for year in body.find_all('thead')]
            tbl_yby = body.find_all("tbody")
            dom = etree.HTML(str(body))

            for i, tbl_year in enumerate(tbl_yby):
                text = [x.strip('\t') for x in tbl_year.text.split('\n') if x not in ['', ' ', '\r']]
                position = [int(pos) for pos in text[::2]]
                results = [[re.sub(r"(\d{1,3}:\d{2}:\d{2})|(\+.+)", "", x),
                            re.search(r"(\d{1,3}:\d{2}:\d{2})|((?<=\+ ).+)|(0)", x)] for x in text[1::2]]
                results = [[rider, None] if time is None else [rider, time.group()] for rider, time in results]
                xpath_year = [[
                    *f.xpath_element(dom, XPATH_HIST_YBY, i+2, tr, 2, 'span'),
                    *f.xpath_element(dom, XPATH_HIST_YBY, i+2, tr, 3, 'a')
                ] for tr in range(1, len(tbl_year.find_all('td')) + 1)]
                countries = [f.re_country_flag(flag[0]) for flag in xpath_year]
                riders_id = [f.re_racer_id(rider_id[1]) for rider_id in xpath_year]
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
                        Time_x=lambda df_: [f.convert_to_seconds(race_time) for race_time in df_.Time_x],
                        Time_y=lambda df_: [f.convert_to_seconds(race_time) for race_time in df_.Time_y]
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
        else:
            foldermask = "races/race_{}"
            folderpath = self.data_dir / foldermask.format(race_id)
            with Path(folderpath.joinpath('history_race.json')).open('r') as file:
                hist_race = json.load(file)
            data = hist_race['yby']['data']
            headers = hist_race['yby']['headers']
            overall_tbl = pd.DataFrame(data, columns=headers)
            if convert_to_sec:
                overall_tbl = (
                    overall_tbl
                    .assign(
                        TimeWinner=lambda df_: [f.convert_to_seconds(time) for time in
                                                df_.groupby('Year')['Time'].transform(lambda x: x.head(1)).ffill()],
                        Time=lambda df_: [f.convert_to_seconds(time)
                                          if f.convert_to_seconds(time) == winner
                                          else f.convert_to_seconds(time) + winner
                                          if f.convert_to_seconds(time) is not None
                                          else None for time, winner in
                                          zip(df_.Time, df_.TimeWinner)]
                    )
                    .drop(columns='TimeWinner')
                )
        return overall_tbl

    def read_race_hist_victories(self,
                                 race_id: int,
                                 force_cache: bool = True) -> Optional[pd.DataFrame]:

        if not force_cache or self.no_cache:
            victory_url = FIRSTCYCLING_URL + "race.php?r={}&k=W"
            page = bs4.BeautifulSoup(requests.get(victory_url.format(race_id)).content, 'lxml')
            body = page.find("body")
            tbl_victory = body.find("tbody")
            dom = etree.HTML(str(body))

            text = [x.strip('\t').strip('\r') for x in tbl_victory.text.split('\n') if x not in ['', ' ', '\r']]
            if len(text) == 0:
                return None
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
                *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'span'),
                *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 2, 'a')
            ] for tr in range(1, len(tbl_victory.find_all('tr')) + 1)]
            countries = [f.re_country_flag(flag[0]) for flag in xpath_winner]
            riders_id = [f.re_racer_id(rider_id[1]) for rider_id in xpath_winner]
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
        else:
            foldermask = "races/race_{}"
            folderpath = self.data_dir / foldermask.format(race_id)
            with Path(folderpath.joinpath('history_race.json')).open('r') as file:
                hist_race = json.load(file)
            data = hist_race['victory']['data']
            headers = hist_race['victory']['headers']
            winner = pd.DataFrame(data, columns=headers)
            if winner.shape[0] == 0:
                winner = None
        return winner

    def read_race_hist_stages(self,
                              race_id: int,
                              force_cache: bool = True
                              ) -> Optional[pd.DataFrame]:
        if not force_cache or self.no_cache:
            stages_url = FIRSTCYCLING_URL + "race.php?r={}&k=Z"
            page = bs4.BeautifulSoup(requests.get(stages_url.format(race_id)).content, 'lxml')
            body = page.find("body")
            tbl_stages = body.find("tbody")
            dom = etree.HTML(str(body))

            text = [x.strip('\t').strip('\r') for x in tbl_stages.text.split('\n') if x not in ['', ' ', '\r']]
            if len(text) == 0:
                return None
            text = [x.strip(' ').strip('\t') for x in text if re.search(r'(\w)|(\d)', x) is not None]
            results = [[pos, rider, country_name, win] for pos, rider, country_name, win in zip(
                text[::4],
                text[1::4],
                text[2::4],
                text[3::4]
            )]
            xpath_stage = [[
                *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 3, 'span'),
                *f.xpath_element(dom, XPATH_HIST_GEN, 3, tr, 2, 'a')
            ] for tr in range(1, len(tbl_stages.find_all('tr')) + 1)]
            countries = [f.re_country_flag(flag[0]) for flag in xpath_stage]
            riders_id = [f.re_racer_id(rider_id[1]) for rider_id in xpath_stage]
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
        else:
            foldermask = "races/race_{}"
            folderpath = self.data_dir / foldermask.format(race_id)
            with Path(folderpath.joinpath('history_race.json')).open('r') as file:
                hist_race = json.load(file)
            data = hist_race['stage']['data']
            headers = hist_race['stage']['headers']
            win_stages = pd.DataFrame(data, columns=headers)
            if win_stages.shape[0] == 0:
                win_stages = None
        return win_stages

    def read_race_hist_young_old_win(self,
                                     race_id: int,
                                     force_cache: bool = True
                                     ) -> Optional[pd.DataFrame]:
        overall_tbl = pd.DataFrame()
        if not force_cache or self.no_cache:
            yby_url = FIRSTCYCLING_URL + "race.php?r={}&k=Y"
            page = bs4.BeautifulSoup(requests.get(yby_url.format(race_id)).content, 'lxml')
            body = page.find("body")
            tbl_yo = body.find_all("tbody")
            dom = etree.HTML(str(body))

            for i, yo in enumerate(tbl_yo):
                text = [x.strip('\t').strip(' ') for x in yo.text.split('\n') if x not in ['', ' ', '\r']]
                if f.is_blank(text):
                    return None
                tbl = [[year, rider, country_name, age] for year, rider, country_name, age in zip(text[::4],
                                                                                                  text[1::4],
                                                                                                  text[2::4],
                                                                                                  text[3::4])]
                xpath_year = [[
                    *f.xpath_element(dom, XPATH_HIST_YO, 3, tr, 3, 'span', table=i+1),
                    *f.xpath_element(dom, XPATH_HIST_YO, 3, tr, 2, 'a', table=i+1)
                ] for tr in range(1, len(yo.find_all('td')) + 1)]
                countries = [f.re_country_flag(flag[0]) for flag in xpath_year]
                riders_id = [f.re_racer_id(rider_id[1]) for rider_id in xpath_year]
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
        else:
            foldermask = "races/race_{}"
            folderpath = self.data_dir / foldermask.format(race_id)
            with Path(folderpath.joinpath('history_race.json')).open('r') as file:
                hist_race = json.load(file)
            data = hist_race['age_winner']['data']
            headers = hist_race['age_winner']['headers']
            overall_tbl = pd.DataFrame(data, columns=headers)
            if overall_tbl.shape[0] == 0:
                overall_tbl = None
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


if __name__ == '__main__':
    cycling = FirstCycling(season=2023)
    df = cycling.read_schedule(force_cache=True)
    # hist = cycling.read_race_history(667, force_cache=False)
    hist1 = cycling.read_race_hist_general(667, force_cache=True)
    hist2 = cycling.read_race_hist_young_old_win(667, force_cache=True)
    hist3 = cycling.read_race_hist_yby(667, convert_to_sec=True, force_cache=True)
    hist4 = cycling.read_race_hist_victories(667, force_cache=True)
    hist5 = cycling.read_race_hist_stages(667, force_cache=True)
    a = 1
