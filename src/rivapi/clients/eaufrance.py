#!/usr/bin/env python3

import pandas as pd

from pandas._libs.tslibs.parsing import DateParseError

from ..decorators import rate_limited, retry_on_failure
from .base import BaseClient 
from .eaufrance_helpers import get_hydrometrie_stations, parse_time, get_hydrometrie_obs_elab, get_hydrometrie_observations_tr

MAX_RECORDS = 20000

class Eaufrance_Client(BaseClient): 

    SOURCE = 'Eaufrance'
    SITE_COLUMN_NAME = 'code_station'
    VARIABLE_MAP = {
        'discharge': 'Q',
        'stage': 'H'
    }
    FREQUENCY_MAP = {
        'daily': 'J',
        'monthly': 'M'
    }
    STATISTIC_MAP = {
        'mean': 'mn',
        'maximum': 'IX',
        'minimum': 'IN'
    }

    def _parse_start_and_end_times(self, start, end):

        def _parse_time(tm): 
            if isinstance(tm, pd.Timestamp): 
                if not tm.tzinfo: 
                    tm = tm.tz_localize('UTC')
                else:
                    tm = tm.tz_convert('UTC')
            else: 
                try:
                    tm = pd.Timestamp(tm, tz='UTC')
                except DateParseError: 
                    raise ValueError(f'Time {tm} cannot be coerced to pandas.Timestamp')
            return tm

        start = _parse_time(start)
        end = _parse_time(end) 
        return super()._parse_start_and_end_times(start, end)

    def _parse_arguments(self, variable = None, frequency = None, statistic = None, start = None, end = None):
        args = super()._parse_arguments(variable, frequency, statistic, start, end)
        variable, frequency, statistic = args['variable'], args['frequency'], args['statistic']

        if (variable == 'H') & (statistic in ['mn', 'IN']): 
            raise ValueError('Stage is only available as a maximum instantaneous value from the Eaufrance API')

        # variables = [
        #     f{a}{b}{c}
        #     for a, b, c in itertools.product(variable, frequency, statistic)
        # ]
        variable = f'{variable}{statistic}{frequency}'
        args['variable'] = variable 
        args['frequency'] = None 
        args['statistic'] = None 
        return args

    def get_metadata(self, **kwargs): 
        # According to the CAMELS-FR dataset: 
        # The CAMELS-FR dataset daily streamflow time series were retrieved from the 
        # Hydroportail website using the hydroportail R package (Delaigue, 2022). 
        # Streamflow data were retrieved at the station level to get all the data, 
        # even for sites where calendar information is missing or incomplete. For 
        # stations with sub-daily streamflow data, mean daily streamflows are 
        # calculated using a trapezoidal method, 
        result  = get_hydrometrie_stations(code_sandre_reseau_station=False)
        self.metadata = pd.DataFrame(result['data'])

    @rate_limited()
    @retry_on_failure()
    def get_data_single_site(self, 
                             site: str, 
                             variable: str,
                             frequency: str, 
                             statistic: str, 
                             start: str,
                             end: str) -> pd.DataFrame:


        if self.metadata is not None: 
            # If metadata is available then we can get the start and end of the record
            station_open = self.metadata[self.metadata['code_station']==site].iloc[0]['date_ouverture_station']
            station_closed = self.metadata[self.metadata['code_station']==site].iloc[0]['date_fermeture_station']
            if station_open: 
                station_open = parse_time(station_open)
                start = max(start, station_open)
            
            if station_closed: 
                station_closed = parse_time(station_closed)
                end = min(end, station_closed)

            if start > end: 
                # This suggests that the station was closed before the provided start time 
                # and therefore no data is available
                # FIXME - add a warning?
                return None

        if variable.endswith('J'): 
            # Then daily 
            n_days = (end.date() - start.date()).days
            chunk_days = min(MAX_RECORDS, n_days)
            chunk_start = start
            all_data = []
            while chunk_start <= end:
                chunk_end = min(chunk_start + pd.Timedelta(days=chunk_days), end)
                result = get_hydrometrie_obs_elab(
                    code_entite = site,
                    date_debut_obs_elab=chunk_start, 
                    date_fin_obs_elab=chunk_end,
                    grandeur_hydro_elab = variable,
                )
                if len(result['data']) > 0:
                    df = pd.DataFrame(result['data'])
                    all_data.append(df)
                chunk_start = chunk_end + pd.Timedelta(days=1) # Increment start
            
            if len(all_data) > 0:
                df = pd.concat(all_data)
            else:
                return None

        else:
            # Then monthly - assume all data will fit into limit
            result = get_hydrometrie_obs_elab(
                code_entite = site,
                date_debut_obs_elab=start, 
                date_fin_obs_elab=end,
                grandeur_hydro_elab = variable
            )
            if len(all_data) > 0:
                df = pd.DataFrame(result['data'])
            else:
                return None

        return df 

    # TODO this is the near realtime data
    # @rate_limited()
    # @retry_on_failure() 
    # def get_data_single_site_near_realtime(self, site, start, end, variable, **kwargs): 
    #     if isinstance(variable, str): 
    #         if variable not in OBSERVATIONS_TR_VARIABLES:
    #             raise ValueError(
    #                 f'Variable not supported. Must be one or both of: ' + ', '.join(OBSERVATIONS_TR_VARIABLES)
    #             )
    #     elif isinstance(variable, (list, tuple)): 
    #         if not all([v in OBSERVATIONS_TR_VARIABLES for v in variable]): 
    #             raise ValueError(
    #                 f'Variable not supported. Must be one or both of: ' + ', '.join(OBSERVATIONS_TR_VARIABLES)
    #             )
    #         else: 
    #             variable=','.join(variable)
    #     start = parse_time(start) 
    #     end = parse_time(end)
    #     if self.metadata: 
    #         # Only return data for stations currently in service
    #         in_service = bool(self.metadata[self.metadata['code_station']==site].iloc[0]['en_service'])
    #         if not in_service: 
    #             return None
    #     latest_end = pd.Timestamp.now(tz='UTC')
    #     earliest_start = latest_end - pd.DateOffset(months=1)
    #     start = max(start, earliest_start)
    #     end = max(min(end, latest_end), earliest_start)
    #     result = get_hydrometrie_observations_tr(
    #         code_entite = site, 
    #         date_debut_obs=start, 
    #         date_fin_obs=end,
    #         grandeur_hydro = variable,
    #     )
    #     if len(result['data']) > 0: 
    #         df = pd.DataFrame(result['data'])
    #     else:
    #         return None
    #     return df

    # def get_data(self, 
    #              sites: Union[str, List[str]] = None, 
    #              sites_from_metadata: bool = False,
    #              variable: Union[str, List[str]] = None,
    #              frequency: Optional[str] = None,
    #              statistic: Optional[str] = None,
    #              start: pd.Timestamp = None, 
    #              end: pd.Timestamp = None,
    #              output_dir: Optional[Path] = None,
    #              overwrite: bool = False,
    #              append: bool = False):

    #     sites = self.get_sites(sites, sites_from_metadata)
    #     n_sites = len(sites)
    #     variable = self.parse_parameter_code_or_name(variable, frequency, statistic)
    #     with Progress() as progress:
    #         task = progress.add_task(f'Downloading data for {n_sites} sites…', total=n_sites)
    #         for site in sites:
    #             time.sleep(0.5)
    #             df = self.get_data_single_site(
    #                 site, 
    #                 start, 
    #                 end, 
    #                 variable
    #             )
    #             if df is not None:
    #                 self.write_data(output_dir, site, df, overwrite, append)
    #             progress.update(task, advance=1)

# # # TESTING 
# result  = get_hydrometrie_sites(unique_site=False)#
# df = pd.DataFrame(result['data'])
# result = get_hydrometrie_stations(code_sandre_reseau_station=False)
# metadata = pd.DataFrame(result['data'])
# result = get_hydrometrie_obs_elab(code_entite = "H0203020", grandeur_hydro_elab = "QmM")
# df = pd.DataFrame(result['data'])
# # result = get_hydrometrie_obs_tr(code_entite = "H0203020", grandeur_hydro='Q')
# # df = pd.DataFrame(result['data'])
# result = get_hydrometrie_obs_elab(code_entite = "1011000101", grandeur_hydro_elab = "QmM")