select driver, round(avg(points),2) as average_points_per_race
from (
         select d.refname                 as driver
              , rc.year || ' ' || rc.name as race_name
              , res.position
              , res.points as points
         from results res,
              races rc,
              drivers d
         where rc.raceid = res.raceid
           and d.driverid = res.driverid
           and rc.year = 2022
           and res.position is not null -- only races where the drivers have finished
         order by rc.raceid,
                  res.position
     ) f
group by driver
order by average_points_per_race desc