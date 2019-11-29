drop function py_geocode_address(address text);

CREATE FUNCTION py_geocode_address(address text) 
	returns text 
as $$ 
	if 'Nominatim' not in GD:
		from geopy.geocoders import Nominatim
		GD['Nominatim'] = Nominatim
	geolocator = Nominatim(user_agent="py_geocode")
	return geolocator.geocode(address)
$$ LANGUAGE plpythonu;