REM $Id: setup_db.bat,v 1.2 2008/04/01 05:44:12 alan Exp alan $
REM
REM This script is for initializing the DYCAST database

REM @echo off

set DBNAME=dycast
set USERNAME=postgres
set DYCAST_PATH=C:\DYCAST\
set DYCAST_APP_PATH=%DYCAST_PATH%\application\
set DYCAST_INIT_PATH=%DYCAST_PATH%\init\
set PG_BIN_PATH=C:\Program Files\PostgreSQL\8.2\bin\
set PG_SHARE_PATH=C:\Program Files\PostgreSQL\8.2\share\contrib\

@echo off
SET /P yesno=This will completely delete any existing database in PostgreSQL named 'dycast'.  Okay to continue (y/n)?

IF NOT "%yesno%"=="y" GOTO:EOF
@echo on

"%PG_BIN_PATH%\dropdb.exe" -U %USERNAME% %DBNAME%
"%PG_BIN_PATH%\createdb.exe" -U %USERNAME% --encoding=UTF8 %DBNAME%
"%PG_BIN_PATH%\createlang.exe" -U %USERNAME% plpgsql %DBNAME%
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%PG_SHARE_PATH%\lwpostgis.sql"
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%PG_SHARE_PATH%\spatial_ref_sys.sql"
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%DYCAST_APP_PATH%\postgres_init.sql"
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%DYCAST_INIT_PATH%\dumped_dist_margs.sql"
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%DYCAST_INIT_PATH%\dumped_county_codes.sql"
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%DYCAST_INIT_PATH%\dumped_effects_polys.sql"
"%PG_BIN_PATH%\psql.exe" -U %USERNAME% -d %DBNAME% -f "%DYCAST_INIT_PATH%\dumped_effects_poly_centers.sql"

If not exist %DYCAST_PATH%\inbox mkdir %DYCAST_PATH%\inbox 
If not exist %DYCAST_PATH%\outbox mkdir %DYCAST_PATH%\outbox 
If not exist %DYCAST_PATH%\outbox\tmp mkdir %DYCAST_PATH%\outbox\tmp 
If not exist %DYCAST_PATH%\outbox\cur mkdir %DYCAST_PATH%\outbox\cur 
If not exist %DYCAST_PATH%\outbox\new mkdir %DYCAST_PATH%\outbox\new 
