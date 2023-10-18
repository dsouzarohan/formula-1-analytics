SELECT d.forename,
       lt.lap lap_number,
       lt.timeinmillis,
       lt.time
FROM drivers d
         JOIN results res on d.driverid = res.driverid
         JOIN races rc on res.raceid = rc.raceid
         JOIN lap_times lt on rc.raceid = lt.raceid AND res.driverid = lt.driverid
WHERE 1 = 1
--   AND rc.name = 'São Paulo Grand Prix'
  AND rc.circuitid = 18
  AND rc.year = 2021
  AND res.position IS NOT NULL
  AND d.refname IN ('max_verstappen', 'hamilton')
ORDER BY d.forename, lap_number