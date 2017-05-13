import shapely.geometry
import pyproj


# Returns a raster grid with points in the coordinate system specified in global setting 'system_coordinate_system'
def generate_grid(user_coordinate_system, system_coordinate_system, extent_min_x, extent_min_y, extent_max_x, extent_max_y):
    # Set up projections
    projection_user_defined = pyproj.Proj(init="epsg:%s" % user_coordinate_system)
    projection_metric = pyproj.Proj(init='epsg:3857')  # metric; same as EPSG:900913
    projection_system_default = pyproj.Proj(init="epsg:%s" % system_coordinate_system)

    # Create corners of rectangle to be transformed to a grid
    north_west = shapely.geometry.Point((extent_min_x, extent_min_y))
    south_east = shapely.geometry.Point((extent_max_x, extent_max_y))

    stepsize = 100  # 100 meter grid step size

    # Project corners to target projection
    # Transform NW and SE points to 3857
    start = pyproj.transform(projection_user_defined, projection_metric, north_west.x, north_west.y)
    end = pyproj.transform(projection_user_defined, projection_metric, south_east.x, south_east.y)

    # Iterate over 2D area
    gridpoints = []
    x = start[0]
    print "Started generating grid..."
    while x < end[0]:
        y = start[1]
        while y > end[1]:
            transformed_coordinate = pyproj.transform(projection_metric, projection_system_default, x, y)
            point = shapely.geometry.Point(transformed_coordinate)
            gridpoints.append(point)
            y -= stepsize
        x += stepsize

    print "Done generating grid."

    return gridpoints
