drop table drivers cascade;
create table drivers
(
    driverId    integer not null primary key,
    refname     varchar(100),
    number      integer,
    code        varchar(3),
    forename    varchar(50),
    surname    varchar(50),
    dob         date,
    nationality varchar(30),
    url         varchar(100)
);