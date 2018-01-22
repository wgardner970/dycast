from sqlalchemy import func
from sqlalchemy.sql.expression import literal
from geoalchemy2.functions import ST_Transform
from geoalchemy2.elements import WKTElement

from application.services import database_service
from application.models.models import Case

def get_point_from_lat_long(lat, lon, projection):
    return WKTElement("POINT({0} {1})".format(lon, lat), srid=projection)

def transform_point(point, target_projection):
    return ST_Transform(point, int(target_projection))
def get_close_space_only(cases_in_cluster_query, close_space):
    subquery = cases_in_cluster_query.subquery()
    query = cases_in_cluster_query.join(subquery, literal(True)) \
        .filter(func.ST_DWithin(Case.location, subquery.c.location, close_space),
                Case.id < subquery.c.id
               )
    return database_service.get_count_for_query(query)

