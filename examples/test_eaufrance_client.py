
import pandas as pd

from rivapi.clients import eaufrance

client = eaufrance.Eaufrance_Client()
client.get_metadata()
stations = ['1011000101']
data = client.get_data(sites=stations, variable='discharge', frequency='daily', statistic='mean', start=pd.Timestamp('2020-01-01'), end=pd.Timestamp('2020-01-31'), write=False, return_data=True)
