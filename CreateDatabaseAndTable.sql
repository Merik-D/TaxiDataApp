IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'TaxiData')
BEGIN
    CREATE DATABASE TaxiData;
END;
GO

USE TaxiData;
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TaxiTrips' AND xtype='U')
BEGIN
    CREATE TABLE TaxiTrips (
        tpep_pickup_datetime DATETIME,
        tpep_dropoff_datetime DATETIME,
        passenger_count INT,
        trip_distance FLOAT,
        store_and_fwd_flag VARCHAR(3),
        PULocationID INT,
        DOLocationID INT,
        fare_amount FLOAT,
        tip_amount FLOAT
    );
END;
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_tip_avg')
BEGIN
    CREATE INDEX idx_tip_avg ON TaxiTrips (PULocationID, tip_amount);
END;
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_trip_distance')
BEGIN
    CREATE INDEX idx_trip_distance ON TaxiTrips (trip_distance DESC);
END;
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_trip_duration')
BEGIN
    CREATE INDEX idx_trip_duration ON TaxiTrips (tpep_pickup_datetime, tpep_dropoff_datetime);
END;
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_pulocation_search')
BEGIN
    CREATE INDEX idx_pulocation_search ON TaxiTrips (PULocationID);
END;
GO

PRINT 'Query 1: Highest average tip_amount by PULocationID';
SELECT TOP 1 
    PULocationID, 
    AVG(tip_amount) AS AvgTipAmount
FROM TaxiTrips
WHERE tip_amount IS NOT NULL
GROUP BY PULocationID
ORDER BY AvgTipAmount DESC;
GO

PRINT 'Query 2: Top 100 longest trips by trip_distance';
SELECT TOP 100 
    PULocationID, 
    DOLocationID, 
    trip_distance, 
    tpep_pickup_datetime, 
    tpep_dropoff_datetime
FROM TaxiTrips
ORDER BY trip_distance DESC;
GO

PRINT 'Query 3: Top 100 longest trips by travel duration';
SELECT TOP 100 
    PULocationID, 
    DOLocationID, 
    DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) AS TravelDuration,
    tpep_pickup_datetime, 
    tpep_dropoff_datetime
FROM TaxiTrips
WHERE tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL
ORDER BY TravelDuration DESC;
GO

PRINT 'Query 4: Search with conditions including PULocationID';
DECLARE @PULocationID INT = 233;
DECLARE @MinDistance FLOAT = 5.0;
DECLARE @MinTipAmount FLOAT = 2.0;

SELECT 
    PULocationID, 
    DOLocationID, 
    trip_distance, 
    tpep_pickup_datetime, 
    tpep_dropoff_datetime, 
    tip_amount
FROM TaxiTrips
WHERE PULocationID = @PULocationID
  AND trip_distance > @MinDistance
  AND tip_amount > @MinTipAmount
ORDER BY tpep_pickup_datetime DESC;
GO
