
import time
import pandas as pd

from rich.progress import Progress
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, List, Optional


class BaseClient(ABC):

    SOURCE = None
    SITE_COLUMN_NAME = None 
    VARIABLE_MAP = {}
    FREQUENCY_MAP = {}
    STATISTIC_MAP = {}

    def __init__(self, 
                 metadata: Optional[Union[Path, pd.DataFrame]] = None):

        if metadata is None:
            self.metadata = None

        elif isinstance(metadata, pd.DataFrame):
            self.metadata = metadata.copy()
        else:
            try:
                self.metadata = pd.read_csv(metadata)
            except Exception as e:
                raise ValueError(
                    f"Provided metadata could not be interpreted as a pandas.DataFrame or a valid file path: {e}"
                )

        if self.metadata is not None:
            required_cols = [self.SITE_COLUMN_NAME]
            if not all(col in self.metadata.columns for col in required_cols):
                raise ValueError(f"Metadata is missing required columns: {required_cols}")

    def _parse_mapped_argument(self, 
                               arg: Optional[Union[str, List[str]]] = None,
                               argname: str = "", 
                               mapping: Optional[dict] = None) -> Union[str, List[str]]:

        if arg is None or not mapping:
            return arg

        if isinstance(arg, str):
            if arg not in mapping:
                raise ValueError(f"{argname.title()} '{arg}' not recognised!")
            return mapping[arg]

        elif isinstance(arg, list):
            mapped_args = []
            for v in arg:
                if v not in mapping:
                    raise ValueError(f"{argname.title()} '{v}' not recognised! Valid values include: {', '.join(mapping.values())}")
                mapped_args.append(mapping[v])
            return mapped_args
        else:
            raise TypeError(f"Expected str or list[str], got {type(arg)}")

    def _parse_start_and_end_times(self, start: pd.Timestamp, end: pd.Timestamp): 
        if start > end: 
            raise ValueError(f'End date must be after start date')
        return start, end

    def _parse_arguments(self,
                         variable: Union[str, List[str]] = None,
                         frequency: Optional[str] = None,
                         statistic: Optional[str] = None,
                         start: pd.Timestamp = None, 
                         end: pd.Timestamp = None): 

        variable = self._parse_mapped_argument(variable, 'variable', self.VARIABLE_MAP)
        frequency = self._parse_mapped_argument(frequency, 'frequency', self.FREQUENCY_MAP)
        statistic = self._parse_mapped_argument(statistic, 'statistic', self.STATISTIC_MAP)
        start, end = self._parse_start_and_end_times(start, end)
        return {'variable': variable, 'frequency': frequency, 'statistic': statistic, 'start': start, 'end': end}

    def _write_metadata(self, output_path):
        self.metadata.to_csv(output_path, index=False)

    def _write_data(self, output_dir, site, df, overwrite=False, append=False):
        output_path = Path(output_dir) / f'{site}.csv'
        if output_path.exists():
            if overwrite:
                mode, header = "w", True
            elif append:
                mode, header = "a", False
            else:
                raise FileExistsError(f"{output_path} already exists. Set overwrite=True or append=True.")
        else:
            mode, header = "w", True
        df.to_csv(output_path, mode=mode, index=False, header=header)

    @abstractmethod
    def get_metadata(self): 
        pass

    @abstractmethod
    def get_data_single_site(self, 
                             site: str, 
                             variable: str,
                             frequency: str, 
                             statistic: str, 
                             start: str,
                             end: str) -> pd.DataFrame:
        """Get data for a single site (implemented by subclass)."""
        pass

    def get_sites(self,
                  sites: Union[str, List[str]],
                  sites_from_metadata: bool = False): 

        if sites is None: 
            sites = [] 
        if isinstance(sites, str):
            sites = [sites]
        sites = list(sites) 

        if sites_from_metadata:
            if not self.metadata:
                raise ValueError(f'No metadata is available!') 
            if self.SITE_COLUMN_NAME not in self.metadata: 
                raise ValueError(f'Metadata does not contain the site column name: {self.SITE_COLUMN_NAME}')

            sites += self.metadata[self.SITE_COLUMN_NAME].tolist()
        sites = list(dict.fromkeys(sites))

        # FIXME use default sites 
        if not sites:
            raise ValueError("No sites provided. Pass `sites` or set `sites_from_metadata=True`.")
        return sites 

    def get_data(self, 
                 sites: Union[str, List[str]] = None, 
                 sites_from_metadata: bool = False,
                 variable: Union[str, List[str]] = None,
                 frequency: Optional[str] = None,
                 statistic: Optional[str] = None,
                 start: pd.Timestamp = None, 
                 end: pd.Timestamp = None,
                 write: bool = False,
                 output_dir: Optional[Path] = None,
                 overwrite: bool = False,
                 append: bool = False,
                 return_data: bool = False):

        sites = self.get_sites(sites, sites_from_metadata)
        n_sites = len(sites)
        args = self._parse_arguments(variable=variable, frequency=frequency, statistic=statistic, start=start, end=end)
        data = {}
        with Progress() as progress:
            task = progress.add_task(f'Downloading data for {n_sites} sites…', total=n_sites)
            for site in sites:
                time.sleep(0.5)
                df = self.get_data_single_site(site, **args)
                if write:
                    self.write_data(output_dir, site, df, overwrite, append)
                data[site] = df
                progress.update(task, advance=1)

        return data if return_data else None