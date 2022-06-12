-- drop table pit_stops;
create table pit_stops
(
  raceId int,
  driverId int,
  stop int,
  lap int,
  time time,
  duration time,
  timeInMillis int,
  PRIMARY KEY (raceId, driverId, lap),
  FOREIGN KEY (raceId) references races (raceId),
  FOREIGN KEY (driverId) references drivers (driverId)
);