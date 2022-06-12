-- drop table results;
create table results
(
    resultId integer not null primary key,
    raceId int references races (raceid),
    driverId int references drivers (driverid),
    constructorId int references constructors (constructorid),
    carNumber int,
    gridPosition int,
    position int,
    positionText varchar(10),
    positionOrder int,
    points numeric,
    laps int,
    time time,
    timeInMillis int,
    fastestLap int,
    rankFastestLap int,
    fastestLapTime time,
    fastestLapSpeed numeric,
    statusId int references status (statusid)
);