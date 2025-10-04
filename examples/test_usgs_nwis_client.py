
import pandas as pd

from rivapi.rivapi import get_metadata, get_data, get_client

metadata = get_metadata(source='usgs', variable='discharge', save=False)
stations = metadata['site_no'].tolist()[slice(0, 10)]
data = get_data( # This will fall back on the default variable/frequency/statistic
    source='usgs', 
    site=stations, 
    start=pd.Timestamp('2020-01-01'), 
    end=pd.Timestamp('2020-01-31')
)

client = get_client('usgs')(metadata)
stations = client.get_sites_from_metadata()
data = client.get_data(
    sites=stations[0:10], 
    start=pd.Timestamp('2020-01-01'), 
    end=pd.Timestamp('2020-01-31'), 
    return_data=True
)