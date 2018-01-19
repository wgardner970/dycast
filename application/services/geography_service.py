from geoalchemy2.functions import ST_Transform
from geoalchemy2.elements import WKTElement

def get_point_from_lat_long(lat, lon, projection):
    return WKTElement("POINT({0} {1})".format(lon, lat), srid=projection)

def transform_point(point, target_projection):
    return ST_Transform(point, int(target_projection))
