#!/usr/bin/env python3

import requests
import pandas as pd
import pytz 
import datetime

from time import sleep
from typing import List, Optional, Union


def make_bom_request(params, retries=5, backoff=1.0):
    """
    Make a request to the BoM waterdata service.

    Parameters
    ----------
    params : dict
        Query parameters for the request.
    retries : int
        Number of retries in case of failure.
    backoff : float
        Delay multiplier between retries.

    Returns
    -------
    pd.DataFrame
        Resulting data as a pandas DataFrame.
    """
    bom_url = "http://www.bom.gov.au/waterdata/services"

    base_params = {
        "service": "kisters",
        "type": "QueryServices",
        "format": "json"
    }

    query_params = {**base_params, **params}

    for attempt in range(retries):
        try:
            r = requests.get(bom_url, params=query_params, timeout=30)
            r.raise_for_status()
            json_data = r.json()
            break
        except requests.HTTPError as e:
            print(f"HTTP error on attempt {attempt+1}: {e}")
        except requests.RequestException as e:
            print(f"Request error on attempt {attempt+1}: {e}")
        sleep(backoff * (2 ** attempt))  # exponential backoff
    else:
        raise RuntimeError(
            "Request for water data failed. Check your request and make sure "
            "http://www.bom.gov.au/waterdata/ is online."
        )

    request_type = params.get("request")
    # Handling list-type requests
    if request_type in ["getParameterList", "getSiteList", "getStationList", "getTimeseriesList"]:
        if json_data[0] == "No matches.":
            raise ValueError("No parameter type and station number match found")
        df = pd.DataFrame(json_data[1:], columns=json_data[0])

    elif request_type == "getTimeseriesValues":
        column_names = json_data[0]["columns"].split(",")
        if len(json_data[0]["data"]) == 0:
            df = pd.DataFrame({
                "Timestamp": pd.to_datetime([]),
                "Value": pd.Series(dtype=float),
                "Quality Code": pd.Series(dtype=int)
            })
        else:
            df = pd.DataFrame(json_data[0]["data"], columns=column_names)

    else:
        df = pd.DataFrame(json_data)

    return df


def get_station_list(
    parameter_type: str = None,
    station_number: list[str] | str = None,
    bbox: list[float] | str = None,
    return_fields: list[str] | str = None,
) -> pd.DataFrame:
    """
    Retrieve a list of stations from the BoM Water Data API.

    Parameters
    ----------
    parameter_type : str, optional
        Type of parameter to filter on (default "Water Course Discharge").
    station_number : str or list of str, optional
        One or more station numbers.
    bbox : list of float or str, optional
        Bounding box [minLon, minLat, maxLon, maxLat].
    return_fields : list of str or str, optional
        Fields to return in the response.

    Returns
    -------
    pd.DataFrame
        A DataFrame of station metadata.
    """
    params = {"request": "getStationList"}

    # Default: all water course discharge stations
    if parameter_type is None:
        parameter_type = "Water Course Discharge"
    params["parameterType_name"] = parameter_type

    # Handle station numbers
    if station_number is not None:
        if isinstance(station_number, (list, tuple)):
            station_number = ",".join(map(str, station_number))
        params["station_no"] = station_number

    # Handle bounding box
    if bbox is not None:
        if isinstance(bbox, (list, tuple)):
            bbox = ",".join(map(str, bbox))
        params["bbox"] = bbox

    # Handle return fields
    if return_fields is None:
        params["returnfields"] = ",".join(
            [
                "station_name",
                "station_no",
                "station_id",
                "station_latitude",
                "station_longitude",
            ]
        )
    else:
        if isinstance(return_fields, (list, tuple)):
            return_fields = ",".join(return_fields)
        params["returnfields"] = return_fields

    # Call the request function
    station_list = make_bom_request(params)

    # # Type conversion (like utils::type.convert in R)
    # station_list = station_list.apply(
    #     lambda col: pd.to_numeric(col, errors="ignore")
    # )

    return station_list

def get_timeseries_id(parameter_type, station_number, ts_name):
    params = {
        "request": "getTimeseriesList",
        "parametertype_name": parameter_type,
        "ts_name": ts_name,
        "station_no": station_number
    }
    get_bom_request = make_bom_request(params)
    return get_bom_request

def get_parameter_list(station_number: str, 
                       return_fields: Optional[List[str]] = None):

    params = {'request': 'getParameterList'}
    if isinstance(station_number, (list, tuple)): 
        station_number = ','.join(station_number)

    params['station_no'] = station_number 

    # Set the default return fields
    if return_fields is None:
        return_fields = [
            "station_no",
            "station_id",
            "station_name",
            "parametertype_id",
            "parametertype_name",
            "parametertype_unitname",
            "parametertype_shortunitname"
        ]
        params["returnfields"] = ','.join(return_fields)

    get_bom_request = make_bom_request(params)

    # # Convert types
    # parameter_list <- dplyr::mutate_all(
    #     get_bom_request,
    #     utils::type.convert,
    #     as.is = TRUE
    # )
    return get_bom_request


def get_timeseries_values(ts_id, start_date, end_date, return_fields):
    params = {
        'request': 'getTimeseriesValues',
        'ts_id': ts_id,
        'from': start_date,
        'to': end_date,
        'returnfields': ','.join(return_fields)
    }
    get_bom_request = make_bom_request(params)
    return get_bom_request


def get_timeseries(
    parameter_type: str,
    station_number: Union[str, List[str]],
    start_date: Union[str, pd.Timestamp],
    end_date: Union[str, pd.Timestamp],
    tz: Optional[str] = None,
    return_fields: Optional[List[str]] = None,
    ts_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Retrieve BoM timeseries data for a station.

    Parameters
    ----------
    parameter_type : str
        The parameter type, e.g. "Water Course Discharge".
    station_number : str or list
        A single station number (list not yet supported).
    start_date : str or datetime
        Start date (YYYY-MM-DD or datetime).
    end_date : str or datetime
        End date (YYYY-MM-DD or datetime).
    tz : str, optional
        Timezone to apply. If None, inferred from jurisdiction.
    return_fields : list, optional
        Which fields to return.
    ts_name : str, optional
        Timeseries name.

    Returns
    -------
    pd.DataFrame
    """

    # Ensure only one station
    if isinstance(station_number, (list, tuple)) and len(station_number) > 1:
        raise ValueError("Only a single station can be requested at a time")

    # Default tz inference
    if tz is None:
        station_list = get_station_list(parameter_type, station_number, return_fields="custom_attributes")
        if station_list.empty:
            raise ValueError(f"Station number {station_number} is invalid")

        jurisdiction = str(station_list["DATA_OWNER_NAME"].iloc[0]).split(" -")[0]
        if jurisdiction in ["ACT", "ACTNSW", "NSW", "QLD", "TAS", "VIC"]:
            tz = "Australia/Queensland"   # AEST (no DST)
        elif jurisdiction in ["SA", "NT"]:
            tz = "Australia/Darwin"       # ACST
        elif jurisdiction == "WA":
            tz = "Australia/Perth"        # AWST
        else:
            print("Jurisdiction not found, returning datetimes in UTC")
            tz = "UTC"
    else:
        # Validate timezone string
        if tz not in pytz.all_timezones:
            raise ValueError("Invalid tz argument. Check pytz.all_timezones.")
        station_list = get_station_list(parameter_type, station_number)
        if station_list.empty:
            raise ValueError(f"Station number {station_number} is invalid")

    # Handle dates
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date, format="%Y-%m-%d", errors="coerce")
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date, format="%Y-%m-%d", errors="coerce")

    if pd.isna(start_date) or pd.isna(end_date):
        raise ValueError("Dates must be formatted as %Y-%m-%d (e.g. 2000-01-01)")
    if start_date > end_date:
        raise ValueError("start_date must be less than end_date")

    # Coerce to tz-aware datetime
    if isinstance(start_date, pd.Timestamp) and start_date.tz is None:
        start_date = start_date.tz_localize(tz)
        end_date = end_date.tz_localize(tz)
    elif not isinstance(start_date, pd.Timestamp):
        raise ValueError("Provide dates as string, date, or datetime object")

    # Format to ISO8601 with offset hh:mm (BoM requires colon in offset)
    def format_bom_time(dt: pd.Timestamp) -> str:
        iso_str = dt.isoformat()
        # Ensure offset has colon (Python isoformat already does: +10:00)
        return iso_str

    start_date_str = format_bom_time(start_date)
    end_date_str = format_bom_time(end_date)

    # Default return fields
    if return_fields is None:
        return_fields = ["Timestamp", "Value", "Quality Code"]

    # Get timeseries ID
    timeseries_id = get_timeseries_id(parameter_type, station_number, ts_name)
    ts_id = timeseries_id.loc[0, "ts_id"]

    # Get values
    timeseries_values = get_timeseries_values(ts_id, start_date_str, end_date_str, return_fields)

    if not timeseries_values.empty:
        if "Timestamp" in timeseries_values.columns:
            timeseries_values["Timestamp"] = pd.to_datetime(
                timeseries_values["Timestamp"], utc=True
            ).dt.tz_convert(tz)

        #     # Convert other fields to numeric if possible
        #     for col in timeseries_values.columns:
        #         if col != "Timestamp":
        #             timeseries_values[col] = pd.to_numeric(timeseries_values[col], errors="ignore")
        # else:
        #     timeseries_values = timeseries_values.apply(pd.to_numeric, errors="ignore")

    return timeseries_values

def parameters(pars: Optional[str] = None):
    continuous = [
        "Dry Air Temperature",
        "Relative Humidity",
        "Wind Speed",
        "Electrical Conductivity At 25C",
        "Turbidity",
        "pH",
        "Water Temperature",
        "Ground Water Level",
        "Water Course Level",
        "Water Course Discharge",
        "Storage Level",
        "Storage Volume"
    ]
    discrete = ["Rainfall", "Evaporation"]
    if pars is None: 
        return discrete + continuous 
    else: 
        try:
            return {'continuous': continuous, 'discrete': discrete}[pars]
        except KeyError: 
            raise ValueError(f'Invalid parameter category: must be one of continuous, discrete')


def get_daily(parameter_type,
              station_number,
              start_date,
              end_date,
              var=None,
              aggregation=None,
              tz=None,
              return_fields=None):

    # Match parameter_type ignoring case
    param_candidates = [p for p in parameters() if p.lower() == parameter_type.lower()]
    if len(param_candidates) == 0:
        raise ValueError("Invalid parameter requested")
    parameter_type = param_candidates[0]

    # Handle var input
    if var is None:
        if parameter_type in parameters("discrete"):
            var = "Total"
        else:
            var = "Mean"
    else:
        var = var.title()

    # Handle aggregation input
    if aggregation is None:
        aggregation = "24HR"
    else:
        aggregation = aggregation.upper()

    ts_name = f"DMQaQc.Merged.Daily{var}.{aggregation}"

    # Define valid time series depending on parameter type
    if parameter_type in parameters("continuous"):
        valid_daily_ts = [
            "DMQaQc.Merged.DailyMean.24HR",
            "DMQaQc.Merged.DailyMax.24HR",
            "DMQaQc.Merged.DailyMin.24HR"
        ]
        if parameter_type == "Water Course Discharge":
            valid_daily_ts.append("DMQaQc.Merged.DailyMean.09HR")

    elif parameter_type in parameters("discrete"):
        valid_daily_ts = [
            "DMQaQc.Merged.DailyTotal.09HR",
            "DMQaQc.Merged.DailyTotal.24HR"
        ]
    else:
        valid_daily_ts = []

    if ts_name not in valid_daily_ts:
        raise ValueError("Invalid combination of parameter_type, var and aggregation")

    if tz is None:
        tz = None

    if return_fields is None:
        return_fields = ["Timestamp", "Value", "Quality Code"]

    # Call the translated get_timeseries function
    timeseries_values = get_timeseries(
        parameter_type=parameter_type,
        station_number=station_number,
        start_date=start_date,
        end_date=end_date,
        tz=tz,
        return_fields=return_fields,
        ts_name=ts_name
    )

    return timeseries_values