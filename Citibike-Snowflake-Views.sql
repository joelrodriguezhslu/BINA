// View erstellen 
CREATE OR REPLACE VIEW 
  WEATHER.PUBLIC.JSON_WEATHER_DATA_VIEW 
AS (
  SELECT
    V:"dt"::TIMESTAMP                   AS OBSERVATION_TIME,
    V:"clouds"."all"::INT                 AS CLOUDS,
    V:"main"."temp"::FLOAT      			AS TEMP_AVG,
    V:"main"."temp_min"::FLOAT  			AS TEMP_MIN,
    V:"main"."temp_max"::FLOAT  			AS TEMP_MAX,
    V:"weather"[0]."main"::STRING         AS WEATHER,
    V:"weather"[0]."description"::STRING  AS WEATHER_DESC,
    V:"weather"[0]."icon"::STRING         AS WEATHER_ICON,
    V:"wind"."deg"::FLOAT                 AS WIND_DIR,
    V:"wind"."speed"::FLOAT               AS WIND_SPEED
  FROM 
    WEATHER.PUBLIC.JSON_WEATHER_DATA
);



// wichtigster Teil -> JOIN two Datasets

create or replace view WEATHER.PUBLIC.CitiWeatherInner as
	Select
	TRIPDURATION,
	STARTTIME,
    hour( starttime) as Stunde,
    iff(hour( starttime) < 13, 'Morgen', iff(hour( starttime) < 18, 'Nachmittag', 'Abend')) as Tageszeit,
	STOPTIME,
	START_STATION_ID,
	START_STATION_NAME,
	START_STATION_LATITUDE,
	START_STATION_LONGITUDE,
	END_STATION_ID,
	END_STATION_NAME,
	END_STATION_LATITUDE,
	END_STATION_LONGITUDE,
	BIKEID,
	USERTYPE,
	(2022 - BIRTH_YEAR)  Age,
	iff(GENDER = 0, 'Unbekannt', iff(Gender = 1, 'Männlich', 'Weiblich')) GENDER,
	OBSERVATION_TIME,
	CLOUDS,
	TEMP_AVG,
	TEMP_MIN,
	TEMP_MAX,
	WEATHER,
	WEATHER_DESC,
	WEATHER_ICON,
	WIND_DIR,
	WIND_SPEED

FROM CITIBIKE.PUBLIC.TRIPS 
  INNER JOIN 
    JSON_WEATHER_DATA_VIEW
  ON 
    DATE_TRUNC('HOUR', OBSERVATION_TIME) = DATE_TRUNC('HOUR', STARTTIME)
  
select * from citiweatherinner limit 100;


// Daily Summary

create or replace view WEATHER.PUBLIC.VW_DAILY_SUMMARY(
	DATUM,
	ANZAHLTRIPS,
	MNNLICH_GEZHLT,
	WEIBLLICH_GEZHLT,
	UNBEKANNT_GEZHLT,
	MORGEN,
	NACHMITTAG,
	ABEND,
	DAUERMINUTEN,
	ANZAHLBIKES,
	TEMPERATURDURCHSCHNITT
) as
SELECT
  CAST(starttime AS date) AS Datum,
  COUNT(*) AS AnzahlTrips,
  count_if(gender = 'Männlich') as Mnnlich_Gezhlt,
  count_if(gender = 'Weiblich') as Weibllich_Gezhlt,
  count_if(gender = 'Unbekannt') as Unbekannt_Gezhlt,
  count_if( Tageszeit = 'Morgen') as Morgen,
    count_if( Tageszeit = 'Nachmittag') as Nachmittag,
      count_if( Tageszeit = 'Abend') as Abend,
  round(SUM(tripduration/60),2) AS DauerMinuten,
  COUNT(DISTINCT bikeid) AS AnzahlBikes, 
  avg(TEMP_AVG) as TemperaturDurchschnitt
  
FROM
    WEATHER.PUBLIC.CitiWeatherInner
GROUP BY
  Datum
ORDER BY
  Datum ASC;


create or replace view WEATHER.PUBLIC.VW_STATION_NAME(
	DATUM,
	STARTSTATIONNAME,
	STARTSTATION_LAT,
	STARTSTATION_LONG,
	ANZAHLTRIPS,
	ANZAHLBIKES,
	DAUERMINUTEN,
	WETTER,
	TEMPERATURDURCHSCHNITT
) as
SELECT
  CAST(starttime AS date) AS Datum,
  start_station_name StartStationName,
  START_STATION_LATITUDE STARTSTATION_LAT,
  START_STATION_LONGITUDE STARTSTATION_LONG,
  COUNT(*) AS AnzahlTrips,
  COUNT(DISTINCT bikeid) AS AnzahlBikes,
  ROUND(SUM(tripduration/60),2) AS DauerMinuten, 
  WEATHER as Wetter,
  avg(TEMP_AVG) as TemperaturDurchschnitt
FROM
   WEATHER.PUBLIC.CitiWeatherInner
GROUP BY
  Datum,
  start_station_name,
  START_STATION_LATITUDE,
  START_STATION_LONGITUDE,
  Wetter
ORDER BY
  Datum;
  
  
create or replace view WEATHER.PUBLIC.VW_STUNDEN_BASIS(
	DATUM,
	STUNDE,
	TAGESZEIT,
	ANZAHLTRIPS,
	DAUERMINUTEN,
	ANZAHLBIKES,
	BENUTZERTYP,
	GESCHLECHT,
	AGE,
	STARTSTATIONNAME,
	STARTSTATION_LAT,
	STARTSTATION_LONG,
	END_STATION_NAME,
	END_STATION_LATITUDE,
	END_STATION_LONGITUDE,
	WETTER,
	TEMPERATURDURCHSCHNITT
) as
SELECT
  CAST(starttime AS date) AS Datum,
  Stunde,
  iff(Stunde < 13, 'Morgen', iff(Stunde < 18, 'Nachmittag', 'Abend')) as Tageszeit,
  COUNT(*) AS AnzahlTrips,
  round(SUM(tripduration/60),2) AS DauerMinuten,
  COUNT(DISTINCT bikeid) AS AnzahlBikes, 
  usertype as Benutzertyp,
  gender as Geschlecht,
  age as Age,
  start_station_name as StartStationName,
  START_STATION_LATITUDE as STARTSTATION_LAT,
  START_STATION_LONGITUDE as STARTSTATION_LONG,
  END_STATION_NAME as EndStationName,
  END_STATION_LATITUDE as ENDSTATION_LAT,
  END_STATION_LONGITUDE as ENDSTATION_LONG,
  WEATHER as Wetter,
  avg(TEMP_AVG) as TemperaturDurchschnitt
FROM
  WEATHER.PUBLIC.CitiWeatherInner
GROUP BY
  Datum, start_station_name, usertype, gender, age,Stunde, weather, start_station_latitude, start_station_longitude, END_STATION_NAME, END_STATION_LATITUDE, END_STATION_LONGITUDE
ORDER BY
  Datum ASC;