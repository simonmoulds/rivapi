
import pandas as pd
import dataretrieval.nwis as nwis

from ..decorators import rate_limited, retry_on_failure
from .base import BaseClient 

from rich.progress import Progress
from dataretrieval.codes.states import state_codes
from typing import Union, List

# NWIS_SERVICES = [
#     'iv', # instantaneous data
#     'dv', # daily mean data
#     'site', # site description
#     'measurements', # discharge measurements
#     'peaks', # discharge peaks
#     'gwlevels', # groundwater levels
#     'pmcodes', # get parameter codes
#     'water_use', # get water use data
#     'ratings', # get rating table
#     'stat' # get statistics
# ]

class NWIS_Client(BaseClient): 

    SOURCE = 'USGS_NWIS'
    SITE_COLUMN_NAME = 'site_no'
    VARIABLE_MAP = {
        'discharge': '00060',
        'stage': '00065',
    }
    FREQUENCY_MAP = { # Rename SERVICE_MAP??? - would the semantics hold for other clients?
        'daily': 'dv',
        'instantaneous': 'iv'
    }
    STATISTIC_MAP = None

    def parse_state_code(self, state_code: Union[str, List[str]]): 
        if state_code is None: 
            return [code for _, code in state_codes.items()]
        elif isinstance(state_code, str): 
            if state_code not in state_codes.values():
                raise ValueError(f"State code '{state_code}' not recognised!")
            return [state_code]
        elif isinstance(state_code, list):
            unrecognised_list = []
            for cd in state_code:
                if cd not in state_codes.values():
                    unrecognised_list.append(cd)
            if len(unrecognised_list) > 0:
                unrecognised_str = ', '.join(unrecognised_list)
                raise ValueError(f'State code(s) {unrecognised_str} not recognised!')
            return state_code 
        else:
            raise TypeError(f"Expected str or list[str], got {type(state_code)}")

    def _parse_start_and_end_times(self, start, end):
        if start:
            start = start.strftime('%Y-%m-%d')
        if end:
            end = end.strftime('%Y-%m-%d')
        return start, end

    @rate_limited()
    @retry_on_failure()
    def get_state_metadata(self, state_code: str, variable: str = None):
        sites, _ = nwis.get_info(stateCd=state_code, parameterCd=variable)
        return sites

    def get_metadata(self, 
                     variable: str, 
                     state_code: Union[str, List[str]] = None):

        variable = self._parse_mapped_argument(variable, 'variable', self.VARIABLE_MAP)
        state_code_list = self.parse_state_code(state_code)
        n_states = len(state_code_list)
        site_list = []
        with Progress() as progress:
            task = progress.add_task("Downloading site metadata…", total=n_states)
            for code in state_code_list: 
                st_meta = self.get_state_metadata(code, variable)
                site_list.append(st_meta)
                progress.update(task, advance=1)
        self.metadata = pd.concat(site_list)

    @rate_limited()
    @retry_on_failure()
    def get_data_single_site(self, 
                             site: str, 
                             variable: str,
                             frequency: str, 
                             statistic: str, 
                             start: str,
                             end: str) -> pd.DataFrame:

        df = nwis.get_record(
            sites=site,
            start=start,
            end=end,
            service=frequency, 
            parameterCd=variable
        )
        return df 


