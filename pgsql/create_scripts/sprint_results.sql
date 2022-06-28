drop table sprint_results;
create table sprint_results
(
    sprintResultId integer not null primary key,
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
    fastestLapTime time,
    statusId int references status (statusid)
);