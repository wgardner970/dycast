import shapely.geometry
import pyproj


def generate_grid(srid, extentMinX, extentMinY, extentMaxX, extentMaxY):
    # Set up projections
    p_ll = pyproj.Proj(init="epsg:{0}".format(srid))
    p_mt = pyproj.Proj(init='epsg:3857')  # metric; same as EPSG:900913

    # Create corners of rectangle to be transformed to a grid
    nw = shapely.geometry.Point((extentMinX, extentMinY))
    se = shapely.geometry.Point((extentMaxX, extentMaxY))

    stepsize = 100  # 100 meter grid step size

    # Project corners to target projection
    # Transform NW point to 3857
    start = pyproj.transform(p_ll, p_mt, nw.x, nw.y)
    end = pyproj.transform(p_ll, p_mt, se.x, se.y)  # .. same for SE

    # Iterate over 2D area
    gridpoints = []
    x = start[0]
    while x < end[0]:
        y = start[1]
        while y < end[1]:
            p = shapely.geometry.Point(pyproj.transform(p_mt, p_ll, x, y))
            gridpoints.append(p)
            y += stepsize
        x += stepsize

    return gridpoints
