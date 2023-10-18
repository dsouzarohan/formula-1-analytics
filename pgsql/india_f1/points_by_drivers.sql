SELECT
    d.forename,
    SUM(res.points) as total_points
FROM
    drivers d JOIN results res on d.driverid = res.driverid
              JOIN races rc on res.raceid = rc.raceid
WHERE rc.name = 'Indian Grand Prix'
  AND res.position IS NOT NULL
GROUP BY d.forename
ORDER BY total_points DESC