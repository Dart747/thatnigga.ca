def fetch_eu_storage_detail():
    api_key = '1a6f6ff1f20fb007aa7f7df073f9151b'
    # existing code for fetching EU storage details...


def fetch_brent():
    # Assume api call here returns data
    data = call_api()
    if not data:
        # Logic to handle market closure
        latest_data = pull_latest_available_data()
        return latest_data
    # Logic to process and return Brent price data...
