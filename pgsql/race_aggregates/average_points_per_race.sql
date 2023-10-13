select driver, round(avg(points),2) as average_points_per_race
from (
         select d.refname                 as driver
              , rc.year || ' ' || rc.name as race_name
              , res.position
              , res.points as points
         from results res JOIN races rc ON rc.raceid = res.raceid
              JOIN drivers d ON d.driverid = res.driverid
         where 1=1
           and rc.year = %(race_year)s
           and res.position is not null -- only races where the drivers have finished
         order by rc.raceid,
                  res.position
     ) f
group by driver
order by average_points_per_race desc