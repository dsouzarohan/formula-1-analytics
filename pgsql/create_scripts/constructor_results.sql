--drop table constructor_results;
create table constructor_results
(
    constructorResultId integer not null primary key,
    raceId int references races (raceid),
    constructorId int references constructors (constructorid),
    points numeric,
    status varchar(10)
);