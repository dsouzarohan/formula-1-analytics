-- drop table races;
create table races
(
    raceId integer not null primary key,
    year int,
    round int,
    circuitId int,
    name varchar(100),
    date date,
    time time,
    url varchar,
    FOREIGN KEY (circuitId) references circuits (circuitId),
    FOREIGN KEY (year) references seasons (year)
);