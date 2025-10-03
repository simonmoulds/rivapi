
import pandas as pd

from rivapi.clients import usgs, eaufrance, bom

client = usgs.NWIS_Client()
client.get_metadata()
stations = ['02339495']
data = client.get_data(sites=stations, variable='discharge', frequency='daily', start=pd.Timestamp('2020-01-01'), end=pd.Timestamp('2020-01-31'), write=False, return_data=True)
data = client.get_data(sites=stations, variable='discharge', frequency='daily', write=False, return_data=True)

client = eaufrance.Eaufrance_Client()
client.get_metadata()
stations = ['1011000101']
data = client.get_data(sites=stations, variable='discharge', frequency='daily', statistic='mean', start=pd.Timestamp('2020-01-01'), end=pd.Timestamp('2020-01-31'), write=False, return_data=True)

client = bom.BOMWater_Client()
client.get_metadata(variable='discharge')
stations = ['403213']
data = client.get_data(sites=stations, variable='discharge', frequency='daily', statistic='mean', start=pd.Timestamp('2020-01-01'), end=pd.Timestamp('2020-01-31'), write=False, return_data=True)