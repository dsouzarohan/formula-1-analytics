drop table qualifying;
create table qualifying(
    qualifyId integer not null primary key,
    raceId int,
    driverId int,
    constructorId int,
    number int,
    position int,
    q1 time,
    q2 time,
    q3 time,
    FOREIGN KEY (raceId) references races (raceId),
    FOREIGN KEY (driverId) references drivers (driverid),
    FOREIGN KEY (constructorId) references constructors (constructorid)
)