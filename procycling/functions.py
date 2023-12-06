from typing import List, Union, Optional
from datetime import datetime

import pandas as pd
from lxml import etree

from procycling.utils import ISO_COUNTRY_CODE, RE_FLAG, RE_ID, RE_DATE_RACE


def parse_race_dates(season: int, date: str):
    d = date.split('-')
    if len(d) == 1:
        return [
            datetime.strptime('.'.join([d[0], str(season)]), '%d.%M.%Y').strftime('%Y/%M/%d'),
            datetime.strptime('.'.join([d[0], str(season)]), '%d.%M.%Y').strftime('%Y/%M/%d')
        ]
    else:
        return [
            datetime.strptime('.'.join([d[0], str(season)]), '%d.%M.%Y').strftime('%Y/%M/%d'),
            datetime.strptime('.'.join([d[1], str(season)]), '%d.%M.%Y').strftime('%Y/%M/%d')
        ]


def re_country_flag(flag: str):
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


def re_racer_id(racer_id: str):
    if racer_id is None:
        return None
    return int(RE_ID.search(racer_id).group())


def finish_race_in_current_month(season: int, races_info):
    dt = [x for x in races_info if (RE_DATE_RACE.search(x) is not None) or (RE_DATE_RACE.search(x) is not None)]
    return len([1 for date in dt if datetime.strptime(parse_race_dates(season, date)[1],
                                                      "%Y/%M/%d").day < datetime.now().day])


def is_blank(data: List[str]) -> bool:
    info = set([x for x in data[2::3]])
    return info == {'Information---'}


def convert_to_seconds(race_time: str) -> int:
    if race_time is None:
        return None
    race_time = race_time.split(':')
    if len(race_time) == 1:
        return int(race_time[0])
    elif len(race_time) == 2:
        return int(race_time[0]) * 60 + int(race_time[1])
    elif len(race_time) == 3:
        return int(race_time[0]) * 3600 + int(race_time[0]) * 60 + int(race_time[1])


def xpath_element(
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
) -> List[Union[str, None]]:
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


def convert_dataframe_to_json(
        df: Optional[pd.DataFrame],
        columns: bool = False
) -> Optional[List]:
    if isinstance(df, pd.DataFrame):
        if columns:
            return df.columns.to_list()
        else:
            return df.values.tolist()
    else:
        return None
