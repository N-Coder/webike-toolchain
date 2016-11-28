CREATE TABLE weather
(
  datetime        DATETIME PRIMARY KEY NOT NULL,
  quality         CHAR(1),
  temp            DECIMAL(5, 2),
  temp_flag       CHAR(1),
  dew_point       DECIMAL(5, 2),
  dew_point_flag  CHAR(1),
  rel_hum         INT(11),
  rel_hum_flag    CHAR(1),
  wind_dir        INT(11),
  wind_dir_flag   CHAR(1),
  wind_speed      INT(11),
  wind_speed_flag CHAR(1),
  visibility      DECIMAL(5, 2),
  visibility_flag CHAR(1),
  stn_press       DECIMAL(5, 2),
  stn_press_flag  CHAR(1),
  hmdx            INT(11),
  hmdx_flag       CHAR(1),
  wind_chill      INT(11),
  wind_chill_flag CHAR(1),
  weather         VARCHAR(100)
);
CREATE TABLE trips
(
  imei       CHAR(4) DEFAULT '0'                            NOT NULL,
  trip       INT(11) DEFAULT '0'                            NOT NULL,
  start_time TIMESTAMP(3) DEFAULT '0000-00-00 00:00:00.000' NOT NULL,
  end_time   TIMESTAMP(3) DEFAULT '0000-00-00 00:00:00.000' NOT NULL,
  distance   DECIMAL(10, 5),
  weather    DATETIME,
  metar      TIMESTAMP,
  avg_temp   FLOAT,
  CONSTRAINT `PRIMARY` PRIMARY KEY (imei, trip)
);
CREATE TABLE datest
(
  selected_date DATE PRIMARY KEY NOT NULL
);
CREATE TABLE weather_metar
(
  stamp  TIMESTAMP PRIMARY KEY NOT NULL,
  metar  VARCHAR(300),
  source VARCHAR(10)
);
CREATE TABLE charge_cycles
(
  id             INT(11)                                        NOT NULL AUTO_INCREMENT,
  imei           CHAR(4) DEFAULT ''                             NOT NULL,
  start_time     TIMESTAMP(3) DEFAULT '0000-00-00 00:00:00.000' NOT NULL,
  end_time       TIMESTAMP(3) DEFAULT '0000-00-00 00:00:00.000' NOT NULL,
  type           CHAR(1),
  sample_count   INT(11),
  avg_thresh_val INT(11),
  CONSTRAINT `PRIMARY` PRIMARY KEY (id, imei)
);
CREATE TABLE soc
(
  imei        CHAR(4) DEFAULT ''                             NOT NULL,
  time        TIMESTAMP(3) DEFAULT '0000-00-00 00:00:00.000' NOT NULL,
  volt        FLOAT,
  volt_smooth FLOAT,
  temp        FLOAT,
  temp_smooth FLOAT,
  soc         FLOAT,
  soc_smooth  FLOAT,
  CONSTRAINT `PRIMARY` PRIMARY KEY (imei, time)
);
ALTER TABLE trips
  ADD FOREIGN KEY (weather) REFERENCES weather (datetime);
ALTER TABLE trips
  ADD FOREIGN KEY (metar) REFERENCES weather_metar (stamp);
CREATE INDEX trips_weather
  ON trips (weather);
CREATE INDEX trips_weather_metar_stamp_fk
  ON trips (metar);