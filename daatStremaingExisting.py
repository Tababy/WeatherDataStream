import requests
from time import sleep
import json
from datetime import datetime
import pandas as pd
import os

def fetch_weather_data(ind,countries, zones,cities):    
    #For new data of X country
         # If folders are empty, create
    
        countries = list(all_cities_df['country'])
        cities = list(all_cities_df['city'])
        zones = list(all_cities_df['zone'])
        for i in range(1440):
            for k in range(len(cities)):
                city = cities[k]
                zone = zones[k]
                country = countries[k]
                if len(sorted(os.listdir(f"./data_stream/{country}/{zone}/{city}/"))) == 0:    # for new files

                    request_url = f"{BASE_URL}?appid={API}&q={cities[k]}"
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
                    print(f"At the present moment {dt} in {country.upper()}, {city.upper()}:")
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
                    dict[city]['country'].append(country)
                    dict[city]['state'].append(zone)
                    dict[city]['city'].append(city)
                    # Store data in df

                    if i==0 :
                        temp_df = pd.DataFrame({'Datetime':[dt],'temp':[temp],'min_temp':[min_temp],'max_temp':[max_temp],'pressure':[pressure],'humidity':[humidity],'wind_speed':[wind_speed],'weather':[weather],'sunrise':[sunrise],'sunset':[sunset],'country':[country],'state':[zone],'city':[city]})#,columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])
                        dict_df[city] = pd.concat([dict_df[city] ,temp_df])
                        dict_df[city].to_csv(f'./data_stream/{country}/{zone}/{city}/CheckedIn_{datetime.now().strftime("%m_%d_%Y_T_%H:%M:%S")}_DataUpTo_{dt}.csv')

                    elif i>=1 and dict_df[city]['Datetime'].iloc[dict_df[city].shape[0]-1] != dt:
                        temp_df = pd.DataFrame({'Datetime':[dt],'temp':[temp],'min_temp':[min_temp],'max_temp':[max_temp],'pressure':[pressure],'humidity':[humidity],'wind_speed':[wind_speed],'weather':[weather],'sunrise':[sunrise],'sunset':[sunset],'country':[country],'state':[zone],'city':[city]})#,columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])
                        dict_df[city] = pd.concat([dict_df[city] ,temp_df])
                        dict_df[city].to_csv(f'./data_stream/{country}/{zone}/{city}/CheckedIn_{datetime.now().strftime("%m_%d_%Y_T_%H:%M:%S")}_DataUpTo_{dt}.csv')
                    else:
                        print(f'No register made, Repeated value for {city} at {dt}')
                        
                        
                    for filename in sorted(os.listdir(f"./data_stream/{country}/{zone}/{city}/"))[:-5]:
                        filename_relPath = os.path.join(f"./data_stream/{country}/{zone}/{city}/",filename)
                        os.remove(filename_relPath)
                    

                elif len(sorted(os.listdir(f"./data_stream/{country}/{zone}/{city}/"))) != 0:       # If something exists already
                # Connectin to the API
                    request_url = f"{BASE_URL}?appid={API}&q={cities[k]}"
                    response = requests.get(request_url)
                    data = json.loads(response.content)
                    # Data we want to retrieve
                    dt = datetime.fromtimestamp(data['dt'])
                    '''city = cities[k]
                    zone = zones[k]
                    country = countries[k]'''
                    temp = round(data['main']['temp'] - 273.15, 2)
                    min_temp = round(data['main']['temp_min'] - 273.15, 2)
                    max_temp = round(data['main']['temp_max'] - 273.15, 2)
                    pressure = round(data['main']['pressure'] , 2)
                    humidity = round(data['main']['humidity'] , 2)
                    wind_speed = round(data['wind']['speed'], 2)
                    weather = data['weather'][0]['main']
                    sunrise = datetime.fromtimestamp(data['sys']['sunrise'])
                    sunset = datetime.fromtimestamp(data['sys']['sunset'])

                    # Output to display in the terminal
                    print('-'*50)
                    print(f"At the present moment {dt} in {country.upper()}, {city.upper()}:")
                    print(f"Current Temperature is {temp}, with minimum of {min_temp} and max as in the {max_temp}")
                    print(f"Current Pressure is {pressure}")
                    print(f"Current Humidity is {humidity}")
                    print(f"Current Weather is {weather}")
                    print(f"Current Wind speed is {wind_speed}")
                    print(f"The sunrise is at {sunrise} and the sunddawn is at {sunset} ")
                    print('-'*50)

                    # Store data in dict of lists, which we will use to structure DFs later
                    dict[city]['temp'].append(temp)
                    dict[city]['min_temp'].append(min_temp)
                    dict[city]['max_temp'].append(max_temp)
                    dict[city]['pressure'].append(pressure)
                    dict[city]['humidity'].append(humidity)
                    dict[city]['wind_speed'].append(wind_speed)
                    dict[city]['weather'].append(weather)
                    dict[city]['sunrise'].append(sunrise)
                    dict[city]['sunset'].append(sunset)
                    dict[city]['country'].append(country)
                    dict[city]['state'].append(zone)
                    dict[city]['city'].append(city)


                    # Retrieve data from Dfs aready created in dict_df
                    name = sorted(os.listdir(f"./data_stream/{country}/{zone}/{city}/"))[0]
                    dict_df[city] = pd.read_csv(f'/Users/zoso/Documents/Courses_and_Certifications/weatherStreamData/WeatherDataStream/data_stream/{country}/{zone}/{city}/{name}')

                    # Store data in dict of DFs

                    if i==0 :
                        temp_df = pd.DataFrame({'Datetime':[dt],'temp':[temp],'min_temp':[min_temp],'max_temp':[max_temp],'pressure':[pressure],'humidity':[humidity],'wind_speed':[wind_speed],'weather':[weather],'sunrise':[sunrise],'sunset':[sunset],'country':[country],'state':[zone],'city':[city]})#,columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])
                        dict_df[city] = pd.concat([dict_df[city] ,temp_df])
                        dict_df[city].to_csv(f'./data_stream/{country}/{zone}/{city}/CheckedIn_{datetime.now().strftime("%m_%d_%Y_T_%H:%M:%S")}_DataUpTo_{dt}.csv')

                    elif i>=1 and dict_df[city]['Datetime'].iloc[dict_df[city].shape[0]-1] != dt:
                        temp_df = pd.DataFrame({'Datetime':[dt],'temp':[temp],'min_temp':[min_temp],'max_temp':[max_temp],'pressure':[pressure],'humidity':[humidity],'wind_speed':[wind_speed],'weather':[weather],'sunrise':[sunrise],'sunset':[sunset],'country':[country],'state':[zone],'city':[city]})#,columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset'])
                        dict_df[city] = pd.concat([dict_df[city] ,temp_df])
                        dict_df[city].to_csv(f'./data_stream/{country}/{zone}/{city}/CheckedIn_{datetime.now().strftime("%m_%d_%Y_T_%H:%M:%S")}_DataUpTo_{dt}.csv')
                    else:
                        print(f'No register made, Repeated value for {city} at {dt}')
                        
                        
                    for filename in sorted(os.listdir(f"./data_stream/{country}/{zone}/{city}/"))[:-5]:
                        filename_relPath = os.path.join(f"./data_stream/{country}/{zone}/{city}/",filename)
                        os.remove(filename_relPath)
            print(f'Going to sleep..., time elapsed sleeping is {round(i*33/60,2)} minutes')
            sleep(1)






all_cities_df = pd.read_csv('./clean_api_full_data.csv')

#zone_mex = list(all_cities_df.loc[(all_cities_df['country'] == 'Mexico')].groupby('zone').count().index)
#zone_usa = list(all_cities_df.loc[(all_cities_df['country'] == 'United States')].groupby('zone').count().index)
all_cities_df = all_cities_df[all_cities_df['country'].isin(['United States' , 'Mexico'])]
#all_cities_df = all_cities_df[all_cities_df['zone'].isin(['California'])]
#all_cities_df = all_cities_df[all_cities_df['city'].isin(['Adelanto'])]
dict = {}
dict_df = {}
for i in range(all_cities_df.shape[0]):
    dict[all_cities_df['city'].iloc[i]] =  {'temp':[] , 'min_temp':[] , 'max_temp' : [] , 'pressure' : [] , 'humidity' : [] , 'wind_speed' : [] , 'weather' : [] , 'sunrise' : [] , 'sunset' : [],'country' : [] , 'state' : [] , 'city' : []}

for i in range(all_cities_df.shape[0]):
    dict_df[all_cities_df['city'].iloc[i]] =  pd.DataFrame(columns=['Datetime', 'temp' , 'min_temp' , 'max_temp' , 'pressure' , 'humidity' , 'wind_speed' , 'weather' , 'sunrise','sunset','country' , 'state' , 'city' ])

all_cities_df['country'].replace({'/':'_'}, regex=True,inplace=True)
all_cities_df['zone'].replace({'/':'_'}, regex=True,inplace=True)
all_cities_df['city'].replace({'/':'_'}, regex=True,inplace=True)

countries = list(all_cities_df['country'])
cities = list(all_cities_df['city'])
zones = list(all_cities_df['zone'])

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
API = 'd7f0a7befa7a96d3566133170d078f06'



    
fetch_weather_data(0,countries,zones,cities)
    
