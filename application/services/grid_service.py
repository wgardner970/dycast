import shapely.geometry
import pyproj


def generate_grid(srid, extent_min_x, extent_max_x, extent_min_y, extent_max_y):
    # Set up projections
    p_ll = pyproj.Proj(init="epsg:{0}".format(srid))
    p_mt = pyproj.Proj(init='epsg:3857')  # metric; same as EPSG:900913

    # Create corners of rectangle to be transformed to a grid
    north_west = shapely.geometry.Point((extent_min_x, extent_min_y))
    south_east = shapely.geometry.Point((extent_max_x, extent_max_y))

    stepsize = 100  # 100 meter grid step size

    # Project corners to target projection
    # Transform NW and SE points to 3857
    start = pyproj.transform(p_ll, p_mt, north_west.x, north_west.y)
    end = pyproj.transform(p_ll, p_mt, south_east.x, south_east.y)

    # Iterate over 2D area
    gridpoints = []
    x = start[0]
    print "Started generating grid..."
    while x < end[0]:
        y = start[1]
        while y < end[1]:
            transformed_coordinate = pyproj.transform(p_mt, p_ll, x, y)
            point = shapely.geometry.Point(transformed_coordinate)
            gridpoints.append(point)
            y += stepsize
        x += stepsize

    print "Done generating grid."

    return gridpoints
