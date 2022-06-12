drop table drivers;
create table drivers
(
    driverId    integer not null primary key,
    refname     varchar(100),
    number      integer,
    code        varchar(3),
    forename    varchar(20),
    dob         date,
    nationality varchar(20),
    url         varchar(100)
);