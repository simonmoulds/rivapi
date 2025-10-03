#!/usr/bin/env python3
 
import os
import pandas as pd

from pathlib import Path
from typing import List, Optional, Union

from rivapi.config import settings
from rivapi.clients import usgs, eaufrance
from rivapi.cache import install_cache


def _initialize_cache(no_cache): 
    use_cache = not no_cache
    if use_cache:
        install_cache()
    return None


def get_client(source): 
    source = source.lower()
    if source == 'usgs': 
        return usgs.NWIS_Client 
    elif source == 'eaufrance': 
        return eaufrance.Eaufrance_Client
    else: 
        raise NotImplementedError


def get_sites(site: Optional[Union[str, List[str]]] = None, 
              site_file: Optional[Path] = None) -> list: 

    site_list_cli = []
    if isinstance(site, str):
        site_list_cli += [x.strip() for x in site.split(',')]
    elif isinstance(site, list):
        site_list_cli += site

    site_list_file = [] 
    if site_file: 
        with open(site_file, "r") as f:
            site_list_file += [line.strip() for line in f if line.strip()]
    site_list = list(dict.fromkeys(site_list_cli + site_list_file))
    return site_list


def get_metadata(
    source: str, 
    variable: Optional[str] = None,
    save: bool = False,
    output_path: Path = None,
    no_cache: bool = False
):
    _initialize_cache(no_cache)
    client = get_client(source)()
    client.get_metadata(variable=variable)
    if save:
        if output_path is None: 
            output_path = os.getcwd()
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        client.write_metadata(output_path)

    return client.metadata 


def get_data(
    source: str,
    site: Optional[Union[str, List[str]]] = None,
    site_file: Optional[Path] = None, 
    sites_from_metadata: bool = False, 
    metadata: Optional[Union[Path, pd.DataFrame]] = None, 
    variable: Union[str, List[str]] = None, 
    frequency: str = 'daily', 
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
    write: Optional[bool] = False,
    output_dir: Optional[Path] = None,
    overwrite: bool = False,
    append: bool = False,
    return_data: bool = False,
    no_cache: bool = True
):
    _initialize_cache(no_cache)
    sites = get_sites(site, site_file)
    client = get_client(source)(metadata)
    client.get_data(sites, sites_from_metadata, variable, frequency, start, end, write, output_dir, overwrite, append, return_data)
