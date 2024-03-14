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


-- Filter Out Irrelavent Flights and Cancelled/Diverted Flights
F1 = FILTER Flights BY (origin == 'ORD' AND dest != 'JFK' AND cancelled == '0.00' AND diverted == '0.00');
F2 = FILTER Flights BY (origin != 'ORD' AND dest == 'JFK' AND cancelled == '0.00' AND diverted == '0.00');

-- Join Flights1 and Flights2
JoinedFlights = JOIN F1 BY (dest, flightDate), F2 BY (origin, flightDate);

-- Filter out tuples where the F2 departure time is not after F1 arrival time
FilteredPairsByTime = FILTER JoinedFlights BY F1::arrTime < F2::depTime;

-- Filter out tuples not between June 2007 and May 2008 on F1
FilteredPairsByDate = FILTER FilteredPairsByTime BY (F1::year == 2007 AND F1::month >= 6) OR (F1::year == 2008 AND F1::month <= 5);

-- Compute the average delay
SumDelays = FOREACH FilteredPairsByDate GENERATE F1::arrDelay + F2::arrDelay AS totalDelay;
GroupAll = GROUP SumDelays ALL;
AverageDelay = FOREACH GroupAll GENERATE AVG(SumDelays.totalDelay);
STORE AverageDelay INTO '$output_path' USING PigStorage(',');
