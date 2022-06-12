drop table constructors;
create table constructors
(
    constructorId integer not null primary key,
    constructorRef varchar(100),
    name varchar(100),
    nationality varchar(20),
    url varchar(100)
);