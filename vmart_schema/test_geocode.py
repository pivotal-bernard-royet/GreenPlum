# Test Geocode Nominatim
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="geopy_test")
location = geolocator.geocode("175 5th Avenue NYC")
print(location.address)
print((location.latitude, location.longitude))
print(location.raw)

# Test Geocode Nominatim France
location = geolocator.geocode("33 rue La Fayette Paris France")

print(location.address)
print((location.latitude, location.longitude))
print(location.raw)
