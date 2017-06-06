
SET client_min_messages = warning;

INSERT INTO spatial_ref_sys (srid, proj4text) VALUES (54003, '+proj=mill +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +R_A +ellps=WGS84 +datum=WGS84 +units=m no_defs');

CREATE TABLE dead_birds (
    bird_id integer PRIMARY KEY,
    report_date date,
    species varchar(40)
);

CREATE TABLE dead_birds_unprojected (
) INHERITS (dead_birds);

CREATE TABLE dead_birds_projected (
) INHERITS (dead_birds);

ALTER TABLE dead_birds_unprojected ADD CONSTRAINT dead_birds_unprojected_pkey PRIMARY KEY (bird_id);

ALTER TABLE dead_birds_projected ADD CONSTRAINT dead_birds_projected_pkey PRIMARY KEY (bird_id);

SELECT AddGeometryColumn('public', 'dead_birds_unprojected', 'location', 3857, 'POINT', 2);

SELECT AddGeometryColumn('public', 'dead_birds_projected', 'location', 3857, 'POINT', 2);

CREATE INDEX dead_birds_unprojected_locationsidx ON dead_birds_unprojected USING GIST ( location );

CREATE INDEX dead_birds_projected_locationsidx ON dead_birds_projected USING GIST ( location );


CREATE TABLE tmp_daily_case_selection (
) INHERITS (dead_birds_projected);

CREATE INDEX tmp_daily_case_selection_locationsidx ON tmp_daily_case_selection USING GIST ( location );


CREATE TABLE tmp_cluster_per_point_selection (
) INHERITS (dead_birds_projected);

CREATE INDEX tmp_cluster_per_point_selection_locationsidx ON tmp_cluster_per_point_selection USING GIST ( location );


CREATE TABLE effects_polys (
    tile_id integer PRIMARY KEY,
    county smallint
);

CREATE TABLE effects_polys_unprojected (
) INHERITS (effects_polys);

CREATE TABLE effects_polys_projected (
) INHERITS (effects_polys);

ALTER TABLE effects_polys_unprojected ADD CONSTRAINT effects_polys_unprojected_pkey PRIMARY KEY (tile_id);
ALTER TABLE effects_polys_projected ADD CONSTRAINT effects_polys_projected_pkey PRIMARY KEY (tile_id);
SELECT AddGeometryColumn('public', 'effects_polys_unprojected', 'the_geom', 3857, 'MULTIPOLYGON', 2);
SELECT AddGeometryColumn('public', 'effects_polys_projected', 'the_geom', 3857, 'MULTIPOLYGON', 2);

CREATE TABLE risk_table_list (
    table_id integer PRIMARY KEY,
    tablename varchar(40),
    date_generated date,
    monte_carlo_id integer
);

CREATE TABLE risk (
    risk_date date NOT NULL,
    lat float NOT NULL,
    long float NOT NULL,
    num_birds integer,
    close_pairs integer,
    close_space integer,
    close_time integer,
    nmcm float,
    PRIMARY KEY(risk_date, lat, long)
);

CREATE TABLE dist_margs (
    number_of_birds integer,
    close_pairs integer,
    probability float,
    cumulative_probability float,
    close_space integer,
    close_time integer
);

CREATE INDEX dist_margs_numidx ON dist_margs (number_of_birds);
CREATE INDEX dist_margs_cpidx ON dist_margs (close_pairs);
CREATE INDEX dist_margs_csidx ON dist_margs (close_space);
CREATE INDEX dist_margs_ctidx ON dist_margs (close_time);

CREATE OR REPLACE FUNCTION 
    close_space_and_time(close_space float, close_time integer)
    RETURNS bigint
    AS 'select distinct count(*) from tmp_cluster_per_point_selection a, tmp_cluster_per_point_selection b where a.bird_id < b.bird_id and st_distance(a.location, b.location) < $1 and abs(a.report_date - b.report_date) <= $2;'
    LANGUAGE SQL
    STABLE;

CREATE OR REPLACE FUNCTION 
    close_space_only(close_space float)
    RETURNS bigint
    AS 'select distinct count(*) from tmp_cluster_per_point_selection a, tmp_cluster_per_point_selection b where a.bird_id < b.bird_id and st_distance(a.location, b.location) < $1;'
    LANGUAGE SQL
    STABLE;

CREATE OR REPLACE FUNCTION 
    close_time_only(close_time integer)
    RETURNS bigint
    AS 'select distinct count(*) from tmp_cluster_per_point_selection a, tmp_cluster_per_point_selection b 
          where a.bird_id < b.bird_id 
          and abs(a.report_date - b.report_date) <= $1;'
    LANGUAGE SQL
    STABLE;

CREATE OR REPLACE FUNCTION
    cst_cs_ct(close_space float, close_time integer)
    RETURNS SETOF bigint AS 
    $$
    BEGIN
        RETURN NEXT close_space_and_time(close_space, close_time);
        RETURN NEXT close_space_only(close_space);
        RETURN NEXT close_time_only(close_time);
    END;
    $$
    LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION
    nmcm(num_birds integer, cp integer, cs integer, ct integer)
    RETURNS float AS 
    $$
    DECLARE
        result_prob float;
        result_ct integer;
    BEGIN
    SELECT cumulative_probability INTO result_prob FROM dist_margs WHERE number_of_birds = num_birds and close_pairs = cp and close_space = cs and close_time = ct;
    IF FOUND THEN
        RETURN result_prob;
    ELSE
        -- First find out if there are any, before going to the trouble of sorting:
        SELECT close_time INTO result_ct FROM dist_margs WHERE number_of_birds = num_birds and close_pairs >= cp and close_time >= ct;
        IF FOUND THEN
            -- Now actually find out which close_time to use:
            SELECT close_time INTO result_ct FROM dist_margs WHERE number_of_birds = num_birds and close_pairs >= cp and close_time >= ct ORDER BY close_time LIMIT 1;
            -- Then use that close_time in this query:
            SELECT cumulative_probability INTO result_prob FROM dist_margs WHERE number_of_birds = num_birds and close_pairs >= cp and close_time = result_ct and close_space >= cs ORDER BY close_space LIMIT 1;
            IF FOUND THEN
                RETURN result_prob;
            ELSE
                RETURN 0.001;
            END IF;
        ELSE
            RETURN 0.0001;
        END IF;
    END IF;
    END;
    $$
    LANGUAGE plpgsql;
  
