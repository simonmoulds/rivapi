#!/usr/bin/env python3

import requests
import pandas as pd
import warnings 

from pandas._libs.tslibs.parsing import DateParseError

from urllib.parse import urlencode, urlparse, parse_qs, urlunparse, urljoin

# Available elaborated hydrometric quantities: 
# - average daily flows (QmnJ)
# - average monthly flows (QmM)
# - maximum monthly instantaneous height (HIXM)
# - maximum daily instantaneous height (HIXnJ)
# - minimum monthly instantaneous flow (QINM)
# - minimum daily instantaneous flow (QINnJ)
# - maximum monthly instantaneous flow (QixM)
# - maximum daily instantaneous flow (QIXnJ)
OBS_ELAB_VARIABLES = ['QmnJ', 'QmM', 'HIXM', 'HIXnJ', 'QINM', 'QINnJ', 'QIXM', 'QIXnJ']
OBSERVATIONS_TR_VARIABLES = ['Q', 'H']
MAX_RECORDS = 20000
HYDROMETRIE_CONFIG = {
    'api_url': 'https://hubeau.eaufrance.fr',
    'apis': {
        'hydrometrie' : {
            'endpoints': {
                'sites': {
                    'path': 'api/v2/hydrometrie/referentiel/sites',
                    'fields': [
                        'bbox', 'code_commune_site', 'code_cours_eau', 
                        'code_departement', 'code_region', 'code_site', 
                        'code_troncon_hydro_site', 'code_zone_hydro_site', 'distance', 
                        'fields', 'format', 'latitude',
                        'libelle_cours_eau', 'libelle_site', 'longitude', 
                        'page', 'size' 
                    ]
                }, 
                'stations': { 
                    'path': 'api/v2/hydrometrie/referentiel/stations', 
                    'fields': [ 
                        'bbox', 'code_commune_station', 'code_cours_eau', 
                        'code_departement', 'code_region', 'code_sandre_reseau_station'
                        'code_site', 'code_station', 'date_fermeture_station', 
                        'date_ouverture_station', 'distance', 'en_service', 
                        'fields', 'format', 'latitude', 
                        'libelle_cours_eau', 'libelle_site', 'libelle_station', 
                        'longitude', 'page', 'size', 
                    ]
                }, 
                'obs_elab': {
                    'path': "api/v2/hydrometrie/obs_elab",
                    'fields': [
                        'bbox', 'code_entite', 'cursor', 
                        'date_debut_obs_elab', 'date_fin_obs_elab', 'distance', 
                        'fields', 'grandeur_hydro_elab', 'latitude', 
                        'longitude', 'resultat_max', 'resultat_min', 
                        'size', 
                    ]
                },
                'observations_tr': {
                    'path': 'api/v2/hydrometrie/observations_tr',
                    'fields': [
                        'bbox', 'code_entite', 'code_statut', 
                        'cursor', 'date_debut_obs', 'date_fin_obs', 
                        'distance', 'fields', 'grandeur_hydro', 
                        'latitude', 'longitude', 'size', 
                        'sort', 'timestep', 
                    ]
                }
            }
        }
    }
}

def do_api_query(api, endpoint, cfg=HYDROMETRIE_CONFIG, *args, **kwargs):
    """
    Query an API endpoint with parameters, handling pagination and errors.

    Parameters:
    - api: str, API name (must exist in config)
    - endpoint: str, endpoint name (must exist in config)
    - *args / **kwargs: additional query parameters
    - params: dict (deprecated, use kwargs)
    """
    params = kwargs 

    # --- Build base query URL ---
    query = urljoin(cfg["api_url"], cfg["apis"][api]["endpoints"][endpoint]["path"])
    allowed_fields = cfg["apis"][api]["endpoints"][endpoint]["fields"]

    # --- Validate and encode query parameters ---
    encoded_params = {}
    for k, v in params.items():
        if k not in allowed_fields:
            raise ValueError(
                f"The parameter '{k}' is not available for this query. "
                f"Run list_params('{api}', '{endpoint}') to see available parameters."
            )
        if v is not None:
            # If list, join with commas
            if isinstance(v, (list, tuple)):
                v = ",".join(map(str, v))
            encoded_params[k] = v

    # Add query parameters to URL
    if encoded_params:
        url_parts = list(urlparse(query))
        # Merge existing query params if any
        qs = parse_qs(url_parts[4])
        qs.update(encoded_params)
        url_parts[4] = urlencode(qs, doseq=True)
        query = urlunparse(url_parts)

    # --- User agent ---
    user_agent = cfg.get("user_agent", "python-requests/1.0")
    headers = {"User-Agent": user_agent}

    # --- Loop for pagination / retries ---
    data = []
    while True:
        resp = requests.get(query, headers=headers)
        if resp.status_code >= 400:
            try:
                content = resp.json()
            except Exception:
                raise RuntimeError(f"Error {resp.status_code} on query: {query}")
            field_errors = content.get("field_errors")
            if field_errors:
                msgs = [f"{fe['field']}: {fe['message']}" for fe in field_errors]
                raise RuntimeError(
                    f"Error {resp.status_code} on query: {query}\nError on parameters:\n"
                    + "\n".join(msgs)
                )
            else:
                raise RuntimeError(f"Error {resp.status_code} on query: {query}")
        else:
            content = resp.json()
            count = int(content.get("count", 0))
            if count > 20000:
                raise ValueError(
                    "Request exceeds API limit of 20000 records. "
                    "Use filters to reduce the number of records."
                )
            elif count == 0:
                data = []
                break

            data.extend(content.get("data", []))
            if resp.status_code == 206:
                # Partial content, get next page
                query = content.get("next")
                if query is None:
                    break

            elif resp.status_code == 200:
                break

    # Optional: attach query as attribute (Python convention: return as tuple)
    return {"data": data, "query": query}


def get_hydrometrie_sites(unique_site, **kwargs): 
    result = do_api_query(api='hydrometrie', endpoint='sites', **kwargs)
    data = result['data']
    query = result['query']

    fields = [
        "code_commune_site",
        "libelle_commune",
        "code_departement",
        "code_region",
        "libelle_region",
        "libelle_departement",
    ]

    cleaned = []
    for site in data:
        first_warning = True
        for field in fields:
            if field in site and site[field] is not None:
                values = site[field]
                # Flatten list-like values
                if isinstance(values, list):
                    unique_values = list(dict.fromkeys(values))  # preserves order
                else:
                    unique_values = [values]

                if unique_site and len(unique_values) > 1:
                    if first_warning:
                        warnings.warn(
                            f"The site '{site.get('code_site')}' has "
                            f"{len(unique_values)} different locations; "
                            "only the first one is returned",
                            stacklevel=2
                        )
                        first_warning = False
                    site[field] = unique_values[0]
                else:
                    site[field] = unique_values[0]
        cleaned.append(site)

    return {"data": cleaned, "query": query} 


def get_hydrometrie_stations(code_sandre_reseau_station=False, **kwargs): 
    result = do_api_query(api='hydrometrie', endpoint='stations')
    data = result['data']
    query = result['query']
    
    cleaned = []
    for station in data: 
        if not code_sandre_reseau_station: 
            station.pop('code_sandre_reseau_station', None)
        cleaned.append(station)

    return {"data": cleaned, "query": query}


def get_hydrometrie_obs_elab(**kwargs): 
    if 'grandeur_hydro_elab' in kwargs: 
        if kwargs['grandeur_hydro_elab'] == 'QmJ': 
            warnings.warn(
                f"The parameter `grandeur_hydro_elab = 'QmJ'` is "
                f"deprecated, use `grandeur_hydro_elab = 'QmnJ'` instead"
            )
            kwargs['grandeur_hydro_elab'] = 'QmnJ'

    result = do_api_query(api='hydrometrie', endpoint='obs_elab', **kwargs)
    return result


def get_hydrometrie_observations_tr(entities='station', **kwargs): 
    if not entities in ['station', 'site', 'both']: 
        raise ValueError(f"Argument 'entities' must be one of 'station', 'site', 'both'")

    result = do_api_query(api='hydrometrie', endpoint='observations_tr', **kwargs)
    data = result['data']
    query = result['query']
    cleaned = []
    for entity in data: 
        if entities == 'station': 
            if entity['code_station']:
                cleaned.append(entity)

        elif entities == 'site': 
            if not entity['code_station']:
                cleaned.append(entity)

    return {'data': cleaned, 'query': query}


def list_params(api, endpoint): 
    return HYDROMETRIE_CONFIG['apis'][api]['endpoints'][endpoint]['fields']


def parse_time(tm): 
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