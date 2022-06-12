--drop table circuits;
create table circuits
(
   circuitId integer not null primary key,
   circuitRef varchar(100),
   name varchar(200),
   location varchar(100),
   country varchar(50),
   lat numeric,
   lng numeric,
   altitude int,
   url varchar(200)
)