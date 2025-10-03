"""Console script for rivapi."""

import typer

from pathlib import Path 
from rich.console import Console
from typing import Optional

from .config import settings
from .rivapi import get_metadata, get_data
from .clients import usgs, eaufrance, bom
from .cache import remove_cache

app = typer.Typer()
console = Console()


@app.callback()
def main(
    rate_limit: int = typer.Option(5, help="Global max calls per second"),
    retries: int = typer.Option(5, help="Global max retries"),
    backoff: float = typer.Option(0.5, help="Global backoff factor"),
):
    settings.rate_limit = rate_limit
    settings.retries = retries
    settings.backoff = backoff


def get_client(source): 
    if source == 'usgs': 
        return usgs.NWIS_Client 
    elif source == 'eaufrance': 
        return eaufrance.Eaufrance_Client
    elif source == 'bom': 
        return bom.BOMWater_Client
    else: 
        raise NotImplementedError


@app.command()
def get_metadata_cli(
    source: str, 
    variable: str = typer.Option(None, help='Variable to retrieve'),
    output: str = typer.Option('.', help='Output file path'),
    no_cache: bool = typer.Option(False, help='Disable caching', is_flag=True)
):
    _ = get_metadata(source, variable, save=True, output_path=Path(output), no_cache=no_cache)


@app.command()
def get_data_cli(
    source: str,
    site: str = typer.Option(None, help='Comma-separated list of site IDs', is_flag=False),
    site_file: Optional[Path] = typer.Option(None, exists=True, readable=True, help='Path to a text file containing one site ID per line'),
    sites_from_metadata: bool = typer.Option(False, help='Whether to retrieve list of sites from metadata file'),
    metadata_file: Optional[Path] = typer.Option(None, exists=True, readable=True, help='Path to metadata file'),
    variable: str = typer.Option(None, help='Variable to retrieve'),
    frequency: str = typer.Option('daily', help='Variable frequency'),
    start: str = typer.Option(None, help='Start time'),
    end: str = typer.Option(None, help='End time'),
    output_dir: Optional[Path] = typer.Option(None, file_okay=True, dir_okay=True, writable=True, resolve_path=True, help='Output location. Created if it does not exist'),
    overwrite: bool = typer.Option(False, help='Whether to overwrite existing files'),
    append: bool = typer.Option(False, help='Whether to append to existing files'),
    no_cache: bool = typer.Option(False, help='Disable caching', is_flag=True)
):
    write=True
    return_data=False
    get_data(
        source, site, site_file, sites_from_metadata, metadata_file, variable, 
        frequency, start, end, write, output_dir, overwrite, append, return_data, no_cache
    )


@app.command()
def clear_cache(): 
    remove_cache()


if __name__ == "__main__":
    app()
