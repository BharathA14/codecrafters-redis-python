from math import radians, sin, cos, asin, sqrt

EARTH_RADIUS_METERS = 6372797.560856
def haversine(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    )

    c = 2 * asin(sqrt(a))

    # Calculate distance
    distance = EARTH_RADIUS_METERS * c

    return distance