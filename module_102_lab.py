import httpx
from prefect import flow, task


@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    print(f"Most recent temp C: {most_recent_temp} degrees")
    return most_recent_temp


@flow(log_prints=True)
def fetch_weather_flow():
    for lat, lon in [(38.9, -77.0), (41.9, 12.49), (48.85, 2.35)]:
        most_recent_temp = fetch_weather(lat, lon)
        print(f"Most recent temp: {lat}, {lon}")


if __name__ == "__main__":
    fetch_weather_flow()
