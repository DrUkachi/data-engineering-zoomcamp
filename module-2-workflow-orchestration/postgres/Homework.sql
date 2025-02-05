--- Question 3 ----
SELECT COUNT(*) FROM yellow_tripdata WHERE tpep_pickup_datetime >= '2020-01-01 00:00:00' AND 
tpep_pickup_datetime < '2021-01-01 00:00:00';

--- Question 4 ----
SELECT COUNT(*) FROM green_tripdata WHERE lpep_pickup_datetime >= '2020-01-01 00:00:00' 
AND tpep_pickup_datetime < '2021-01-01 00:00:00';


--- Question 5 ----
SELECT COUNT(*) FROM yellow_tripdata WHERE tpep_pickup_datetime >= '2021-03-01 00:00:00' AND
tpep_pickup_datetime < '2021-04-01 00:00:00';