
import pandas as pd
import rivapi.rivapi
import rivapi.clients 

import importlib 
importlib.reload(rivapi.rivapi)
importlib.reload(rivapi.clients)

from rivapi.rivapi import get_metadata, get_data
from rivapi.clients import eaufrance

# Use the `get_data` wrapper, which implements caching
metadata = get_metadata(source='eaufrance', variable='discharge', save=False)
stations = metadata['code_station'].tolist()[slice(0, 10)]
data = get_data( # This will fall back on the default variable/frequency/statistic
    source='eaufrance', 
    site=stations, 
    start=pd.Timestamp('2020-01-01'), 
    end=pd.Timestamp('2020-01-31')
)

# We can also use the client directly
client = eaufrance.Eaufrance_Client()
client.get_metadata()
data = client.get_data(
    sites=stations, 
    variable='discharge', frequency='daily', statistic='mean', 
    start=pd.Timestamp('2020-01-01'), 
    end=pd.Timestamp('2020-01-31'), 
    write=False, 
    return_data=True
)