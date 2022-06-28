drop table driver_standings
create table driver_standings
(
    driverStandingsId integer not null primary key,
    raceId int,
    driverId int,
    points numeric,
    position int,
    positionText varchar(5),
    wins int,
    FOREIGN KEY (raceId) references races (raceId),
    FOREIGN KEY (driverId) references drivers (driverid)
);