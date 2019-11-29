# import the geocoding services you'd like to try
from geopy.geocoders import ArcGIS, Nominatim
import csv, sys
import pandas as pd

in_file = str(sys.argv[1])
out_file = str('gc_' + in_file)
timeout = int(sys.argv[2])

print('creating geocoding objects.')

arcgis = ArcGIS(timeout=timeout)
nominatim = Nominatim(user_agent='test', timeout=timeout)

# choose and order your preference for geocoders here
geocoders = [ nominatim, arcgis]

def gc(address):
    street = str(address['street'])
    city = str(address['city'])
    state = str(address['state'])
    country = str(address['country'])
    add_concat = street + ", " + city + ", " + state + " " + country
    for gcoder in geocoders:
        location = gcoder.geocode(add_concat)
        if location != None:
            print(f'geocoded record {address.name}: {street}')
            located = pd.Series({
                'lat': location.latitude,
                'lng': location.longitude,
                'time': pd.to_datetime('now')
            })
        else:
            print(f'failed to geolocate record {address.name}: {street}')
            located = pd.Series({
                'lat': 'null',
                'lng': 'null',
                'time': pd.to_datetime('now')
            })
        return located

print('opening input.')
reader = pd.read_csv(in_file, header=0)
print('geocoding addresses.')
reader = reader.merge(reader.apply(lambda add: gc(add), axis=1), left_index=True, right_index=True)
print(f'writing to {out_file}.')
reader.to_csv(out_file, encoding='utf-8', index=False)
print('done.')
