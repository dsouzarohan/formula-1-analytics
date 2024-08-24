/*

 */


SELECT d.forename,
       lt.lap lap_number,
       lt.timeinmillis,
       lt.time,
       CASE WHEN pt.stop IS NOT NULL THEN 'stop' ELSE null END as is_pitstop,
       rc.year
FROM drivers d
         JOIN results res on d.driverid = res.driverid
         JOIN races rc on res.raceid = rc.raceid
         JOIN lap_times lt on rc.raceid = lt.raceid AND res.driverid = lt.driverid
         LEFT JOIN pit_stops pt on pt.lap = lt.lap AND pt.raceid = lt.raceid AND pt.driverid = lt.driverid
WHERE 1 = 1
  AND rc.name = 'Indian Grand Prix'
  AND rc.year = 2013
  AND res.position IS NOT NULL
  AND res.position <= 5
--   AND d.refname IN ('vettel', 'webber', 'alonso', 'hamilton')

ORDER BY rc.year, d.forename, lap_number
-- LIMIT 5