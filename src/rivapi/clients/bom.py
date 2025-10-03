#!/usr/bin/env python3

import time
import pandas as pd

from rivapi.decorators import rate_limited, retry_on_failure
from rivapi.clients.base import BaseClient 

from rich.progress import Progress
from pathlib import Path 
from typing import List, Union, Optional

from .bom_helpers import get_station_list, get_daily


class BOMWater_Client(BaseClient):
    
    SOURCE = 'BOM'
    SITE_COLUMN_NAME = 'station_no'
    VARIABLE_MAP = {
        'discharge': 'Water Course Discharge',
        'stage': 'Water Course Stage'
    }
    FREQUENCY_MAP = {}
    STATISTIC_MAP = {}

    @rate_limited()
    @retry_on_failure()
    def get_metadata(self, 
                     variable: str,
                     **kwargs): 

        parameter_type = self._parse_mapped_argument(variable, 'variable', self.VARIABLE_MAP)
        metadata = get_station_list(parameter_type=parameter_type, **kwargs)
        self.metadata = metadata

    @rate_limited()
    @retry_on_failure()
    def get_data_single_site(self, 
                             site: str, 
                             variable: str,
                             frequency: str, 
                             statistic: str, 
                             start: str,
                             end: str) -> pd.DataFrame:
        df = get_daily(
            parameter_type=variable,
            station_number=site,
            start_date=start,
            end_date=end,
            var=None, # max,min,Mean... - defaults to Mean
            aggregation='24HR'
        )
        return df


# parameters()
# x = get_station_list(parameter_type='Water Course Discharge')
# z = get_station_list(parameter_type='Water Course Level')
# get_parameter_list(station_number = ("410730", "570946"))
# df = get_daily(
#   parameter_type = "Water Course Discharge",
#   station_number = "410730",
#   start_date = "2020-01-01",
#   end_date = "2020-01-31",
#   var = "max",
#   aggregation = "24HR"
# )