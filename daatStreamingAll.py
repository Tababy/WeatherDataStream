import requests
from time import sleep
import json
from datetime import datetime
import pandas as pd
import os

all_cities_df=pd.read_csv('/home/vboxuser/Documents/030223/zone_city.csv')
# Drop cities not in api data
all_cities_df.drop(55,inplace=True)


dict = {}
dict_df = {}
for i in range(all_cities_df.shape[0]):
    dict[all_cities_df['city'][i]] =  {'temp':[] , 'min_temp':[] , 'max_temp' : [] , 'pressure' : [] , 'humidity' : [] , 'wind_speed' : [] , 'weather' : [] , 'sunrise' : [] , 'sunset' : []}

for i in range(all_cities_df.shape[0]):
    dict_df[all_cities_df['city'][i]] =  pd.DataFrame(columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])

cities = list(all_cities_df['city'])

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
API = 'd7f0a7befa7a96d3566133170d078f06'
city = 'Atlanta'
request_url = f"{BASE_URL}?appid={API}&q={city}"

try:
    for i in range(1440):
        for city in cities:
            request_url = f"{BASE_URL}?appid={API}&q={city}"
            response = requests.get(request_url)
            data = json.loads(response.content)
            dt = datetime.fromtimestamp(data['dt'])
            temp = round(data['main']['temp'] - 273.15, 2)
            min_temp = round(data['main']['temp_min'] - 273.15, 2)
            max_temp = round(data['main']['temp_max'] - 273.15, 2)
            pressure = round(data['main']['pressure'] , 2)
            humidity = round(data['main']['humidity'] , 2)
            wind_speed = round(data['wind']['speed'], 2)
            weather = data['weather'][0]['main']
            sunrise = datetime.fromtimestamp(data['sys']['sunrise'])
            sunset = datetime.fromtimestamp(data['sys']['sunset'])
            print('-'*50)
            print(f"At the present moment {dt} in {city.upper()}:")
            print(f"Current Temperature is {temp}, with minimum of {min_temp} and max as in the {max_temp}")
            print(f"Current Pressure is {pressure}")
            print(f"Current Humidity is {humidity}")
            print(f"Current Weather is {weather}")
            print(f"Current Wind speed is {wind_speed}")
            print(f"The sunrise is at {sunrise} and the sunddawn is at {sunset} ")
            print('-'*50)

            # Store data in dict
            dict[city]['temp'].append(temp)
            dict[city]['min_temp'].append(min_temp)
            dict[city]['max_temp'].append(max_temp)
            dict[city]['pressure'].append(pressure)
            dict[city]['humidity'].append(humidity)
            dict[city]['wind_speed'].append(wind_speed)
            dict[city]['weather'].append(weather)
            dict[city]['sunrise'].append(sunrise)
            dict[city]['sunset'].append(sunset)
            # Store data in df

            if i==0 :
                temp_df = pd.DataFrame({'Datetime':[dt],'temp':[temp],'min_temp':[min_temp],'max_temp':[max_temp],'pressure':[pressure],'humidity':[humidity],'wind_speed':[wind_speed],'weather':[weather],'sunrise':[sunrise],'sunset':[sunset]})#,columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])
                dict_df[city] = pd.concat([dict_df[city] ,temp_df])
                dict_df[city].to_csv(f'/home/vboxuser/Documents/030223/data_stream/{city}/CheckedIn_{datetime.now().strftime("%m_%d_%Y_T_%H:%M:%S")}_DataUpTo_{dt}.csv')

            elif i>=1 and dict_df[city]['Datetime'].iloc[dict_df[city].shape[0]-1] != dt:
                temp_df = pd.DataFrame({'Datetime':[dt],'temp':[temp],'min_temp':[min_temp],'max_temp':[max_temp],'pressure':[pressure],'humidity':[humidity],'wind_speed':[wind_speed],'weather':[weather],'sunrise':[sunrise],'sunset':[sunset]})#,columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])
                dict_df[city] = pd.concat([dict_df[city] ,temp_df])
                dict_df[city].to_csv(f'/home/vboxuser/Documents/030223/data_stream/{city}/CheckedIn_{datetime.now().strftime("%m_%d_%Y_T_%H:%M:%S")}_DataUpTo_{dt}.csv')
            else:
                print(f'No register made, Repeated value for {city} at {dt}')
                
                
            for filename in sorted(os.listdir(f"/home/vboxuser/Documents/030223/data_stream/{city}/"))[:-10]:
                filename_relPath = os.path.join(f"/home/vboxuser/Documents/030223/data_stream/{city}/",filename)
                os.remove(filename_relPath)
        sleep(60)
except KeyError:
    print(f'The city {city} is not found in the API data')