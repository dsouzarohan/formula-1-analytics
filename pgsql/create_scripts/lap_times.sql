drop table lap_times;
create table lap_times
(
    raceId int,
    driverId int,
    lap int,
    position int,
    time time,
    timeInMillis int,
    PRIMARY KEY (raceId, driverId, lap),
    FOREIGN KEY (raceId) references races (raceid),
    FOREIGN KEY (driverId) references drivers (driverid)
);