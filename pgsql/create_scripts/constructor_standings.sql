-- drop table constructor_standings;
create table constructor_standings
(
  constructorStandingsId integer not null primary key,
  raceId int,
  constructorId int,
  points numeric,
  position int,
  positionText varchar(5),
  wins int,
  FOREIGN KEY (raceId) references races (raceid),
  FOREIGN KEY (constructorId) references constructors (constructorid)
);