import os
import time
import requests
from prometheus_client import start_http_server, Gauge, Counter

# Metrics
temperature = Gauge('weather_temperature_celsius', 'Current temperature in Celsius')
humidity = Gauge('weather_humidity_percent', 'Current humidity percentage')
pressure = Gauge('weather_pressure_hpa', 'Current atmospheric pressure in hPa')
wind_speed = Gauge('weather_wind_speed_ms', 'Current wind speed in m/s')
wind_direction = Gauge('weather_wind_direction_degrees', 'Current wind direction in degrees')
visibility = Gauge('weather_visibility_meters', 'Current visibility in meters')
clouds = Gauge('weather_clouds_percent', 'Current cloud coverage percentage')
rain_1h = Gauge('weather_rain_1h_mm', 'Rain volume for the last 1 hour in mm')
snow_1h = Gauge('weather_snow_1h_mm', 'Snow volume for the last 1 hour in mm')
api_calls = Counter('weather_api_calls_total', 'Total number of API calls made')

# OpenWeather API details
API_KEY = os.getenv('OPENWEATHER_API_KEY', 'your_openweather_api_key_here')  # Read from env
CITY = 'Astana'  # Change to desired city
URL = f'http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'

def fetch_weather_data():
    try:
        response = requests.get(URL)
        data = response.json()
        api_calls.inc()

        if response.status_code == 200:
            main = data['main']
            wind = data.get('wind', {})
            weather = data['weather'][0] if data['weather'] else {}
            rain = data.get('rain', {})
            snow = data.get('snow', {})

            temperature.set(main.get('temp', 0))
            humidity.set(main.get('humidity', 0))
            pressure.set(main.get('pressure', 0))
            wind_speed.set(wind.get('speed', 0))
            wind_direction.set(wind.get('deg', 0))
            visibility.set(data.get('visibility', 0))
            clouds.set(data.get('clouds', {}).get('all', 0))
            rain_1h.set(rain.get('1h', 0))
            snow_1h.set(snow.get('1h', 0))
        else:
            print(f"API Error: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data: {e}")

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)
    print("Custom Exporter started on port 8000")

    while True:
        fetch_weather_data()
        time.sleep(20)  # Update every 20 seconds