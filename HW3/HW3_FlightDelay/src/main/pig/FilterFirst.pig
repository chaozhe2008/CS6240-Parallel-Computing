REGISTER file:/usr/local/pig-0.17.0/lib/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- Load Data
Data = LOAD '$input_path' USING CSVLoader();
Flights = foreach Data generate
            $0 as year,
            $2 as month,
            $5 as flightDate,
            $11 as origin,
            $17 as dest,
            $24 as depTime,
            $35 as arrTime,
            $37 as arrDelay,
            $41 as cancelled,
            $43 as diverted;

-- Pre-filter Flights by Date Range (June 2007 to May 2008)
PreFilteredFlights = FILTER Flights BY (year == 2007 AND month >= 6) OR (year == 2008 AND month <= 5);

-- Filter F1 and F2 with conditions including Date Range
F1 = FILTER PreFilteredFlights BY (origin == 'ORD' AND dest != 'JFK' AND cancelled == '0.00' AND diverted == '0.00');
F2 = FILTER PreFilteredFlights BY (origin != 'ORD' AND dest == 'JFK' AND cancelled == '0.00' AND diverted == '0.00');

-- Join Flights1 and Flights2
JoinedFlights = JOIN F1 BY (dest, flightDate), F2 BY (origin, flightDate);

-- Filter out tuples where the F2 departure time is not after F1 arrival time
FilteredPairsByTime = FILTER JoinedFlights BY F1::arrTime < F2::depTime;

-- Compute the average delay (no need for a separate date filter step as it's already applied)
SumDelays = FOREACH FilteredPairsByTime GENERATE F1::arrDelay + F2::arrDelay AS totalDelay;
GroupAll = GROUP SumDelays ALL;
AverageDelay = FOREACH GroupAll GENERATE AVG(SumDelays.totalDelay);
STORE AverageDelay INTO '$output_path' USING PigStorage(',');
