/*
Uber Request Data Exploration
Skills used: Create and Import table, Update, Datetime format, Substrings, Case statement, Aggregate functions, Joins, CTE
*/

-- **Create table and Import data**
CREATE TABLE `uberrequest` (
  `Request_id` int NOT NULL,
  `Pickup_point` VARCHAR(255),
  `Driver_id` VARCHAR(255),
  `Status` VARCHAR(255),
  `Request_timestamp` VARCHAR(255),
  `Drop_timestamp` VARCHAR(255),
  PRIMARY KEY (`Request_id`)
);

SHOW VARIABLES LIKE "secure_file_priv";

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Uber Request Data.csv"
into table uberrequest
fields terminated by ','
optionally enclosed by '"'
escaped by '"'
lines terminated by '\n'
ignore 1 lines;

-- DROP TABLE uberrequest;

SELECT * FROM uberrequest;


-- **Cleaning**

-- Format date and time -- Separate date and time into 2 columns
ALTER TABLE uberrequest
ADD Drop_timestampDT VARCHAR(255),
ADD Request_timestampDT VARCHAR(255);

UPDATE uberrequest
SET Drop_timestampDT = REPLACE(Drop_timestamp,"/","-");

UPDATE uberrequest
SET Request_timestampDT = REPLACE(Request_timestamp,"/","-");

UPDATE uberrequest
SET Request_timestampDT = SUBSTRING_INDEX(Request_timestampDT, ":", 2)
WHERE LENGTH(Request_timestampDT) IN (SELECT MAX(LENGTH(Request_timestampDT)));

UPDATE uberrequest
SET Drop_timestampDT = SUBSTRING_INDEX(Drop_timestampDT, ":", 2)
WHERE LENGTH(Drop_timestampDT) IN (SELECT MAX(LENGTH(Drop_timestampDT)));

UPDATE uberrequest
SET Request_timestampDT = STR_TO_DATE(Request_timestampDT, "%d-%m-%Y %H:%i");

UPDATE uberrequest
SET Drop_timestampDT = STR_TO_DATE(Drop_timestampDT, "%d-%m-%Y %H:%i")
WHERE Drop_timestampDT RLIKE "^[0-9]";

-- Check null
SELECT * FROM uberrequest
WHERE Driver_id = "NA"
   OR Drop_timestampDT = "NA";
   
-- Categorize time of the day to early morning (3-7), morning rush (7-9), evening rush (5-7), 
-- off-peak day (9-5), off-peak night (7-11), late night (11-3)
ALTER TABLE uberrequest
ADD Time_of_day VARCHAR(255);

UPDATE uberrequest
SET Time_of_day = CASE
	WHEN TIME(Request_timestampDT) BETWEEN "03:00" AND "07:00" THEN "Early morning"
    WHEN TIME(Request_timestampDT) BETWEEN "07:00" AND "09:00" THEN "Morning rush"
    WHEN TIME(Request_timestampDT) BETWEEN "09:00" AND "17:00" THEN "Off-peak day"
    WHEN TIME(Request_timestampDT) BETWEEN "17:00" AND "19:00" THEN "Evening rush"
    WHEN TIME(Request_timestampDT) BETWEEN "19:00" AND "23:00" THEN "Off-peak night"
    ELSE "Late night" 
    END;
    
-- Drop unused column
ALTER TABLE uberrequest
DROP COLUMN Request_timestamp,
DROP Drop_timestamp;


-- **Exploration**

-- Number of request ids to each pick up point and time of day
SELECT DISTINCT(Pickup_point), 
	   COUNT(Request_id) 
FROM uberrequest
GROUP BY Pickup_point;

SELECT DISTINCT(Pickup_point), 
	   Time_of_day, 
       COUNT(Request_id) 
FROM uberrequest
GROUP BY Pickup_point, Time_of_day
ORDER BY Pickup_point, Time_of_day;

-- Top 3 drivers getting the most request id? Where are these request ids from? 
With CTE AS(
	SELECT Driver_id, 
		   COUNT(Request_id) AS RequestCount 
    FROM uberrequest
    WHERE Driver_id != "NA"
    GROUP BY Driver_id
    ORDER BY COUNT(Request_id) DESC LIMIT 3)
SELECT CTE.Driver_id, 
	   u.Pickup_point, 
       COUNT(u.Request_id) 
FROM CTE
	INNER JOIN uberrequest u ON temp.Driver_id = u.Driver_id
GROUP BY u.Driver_id, u.Pickup_point
ORDER BY CTE.Driver_id;

-- How many completed trip and cancelled trip these top 3 drivers have?
With CTE AS(
	SELECT Driver_id, 
		   COUNT(Request_id) AS RequestCount 
    FROM uberrequest
    WHERE Driver_id != "NA"
    GROUP BY Driver_id
    ORDER BY COUNT(Request_id) DESC LIMIT 3)
SELECT CTE.Driver_id, 
       u.Status, 
       COUNT(u.Status) 
FROM CTE
	INNER JOIN uberrequest u ON CTE.Driver_id = u.Driver_id
GROUP BY CTE.Driver_id, u.Status
ORDER BY CTE.Driver_id;

-- How many completed trip, cancelled and NA trip in total in each pick up point?
SELECT Pickup_point, 
	   Status, 
	   COUNT(Status)
FROM uberrequest
GROUP BY Pickup_point, Status
ORDER BY Pickup_point;

-- Where most cancelled trips occur?
SELECT Pickup_point 
FROM uberrequest
WHERE Status = "Cancelled"
GROUP BY Pickup_point
ORDER BY COUNT(Request_id) DESC LIMIT 1;

-- Which time of the day (categorized) when most trips are requested? 
SELECT Time_of_day, 
	   COUNT(Request_id) 
FROM uberrequest
GROUP BY Time_of_day
ORDER BY COUNT(Request_id) DESC;

-- How many trips are completed, cancelled and not available?
SELECT Time_of_day, 
	   Status, 
       COUNT(Request_id)
FROM uberrequest
GROUP BY Time_of_day, Status
ORDER BY Time_of_day;

-- Average travel time group by pickup point
SELECT Pickup_point, 
	   AVG(TIMESTAMPDIFF(HOUR, Request_timestampDT, Drop_timestampDT)) 
FROM uberrequest
GROUP BY Pickup_point;

-- Pickup time and pickup point relations
SELECT Time_of_day, 
	   Pickup_point, 
       COUNT(Request_id) 
FROM uberrequest
GROUP BY Time_of_day, Pickup_point;