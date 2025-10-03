
import pandas as pd

from rivapi.clients import bom

client = bom.BOMWater_Client()
client.get_metadata(variable='discharge')
stations = ['403213']
data = client.get_data(sites=stations, variable='discharge', frequency='daily', statistic='mean', start=pd.Timestamp('2020-01-01'), end=pd.Timestamp('2020-01-31'), write=False, return_data=True)