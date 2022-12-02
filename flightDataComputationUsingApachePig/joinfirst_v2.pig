DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

flight = LOAD 's3://dominic-bucket/hw3/input/data.csv' USING CSVLoader as (Year:Int, Quarter:Int, Month:Int, DayofMonth:Int,DayOfWeek:Int, FlightDate:chararray, UniqueCarrier:chararray, AirlineID:chararray, Carrier:chararray, TailNum:chararray, FlightNum:Int, Origin:chararray, OriginCityName:chararray, OriginState:chararray, OriginStateFips:Int, OriginStateName:chararray, OriginWac:chararray, Dest:chararray, DestCityName:chararray, DestState:chararray, DestStateFips:chararray, DestStateName:chararray, DestWac:chararray, CRSDepTime:chararray, DepTime:Int, DepDelay:Int, DepDelayMinutes:Int, DepDel15:chararray, DepartureDelayGroups:chararray, DepTimeBlk:chararray, TaxiOut:chararray, WheelsOff:chararray, WheelsOn:chararray, TaxiIn:chararray, CRSArrTime:chararray, ArrTime:Int, ArrDelay:chararray, ArrDelayMinutes:Int, ArrDel15:chararray, ArrivalDelayGroups:chararray, ArrTimeBlk:chararray, Cancelled:Int, CancellationCode:chararray, Diverted:Int, CRSElapsedTime:chararray, ActualElapsedTimeInt:chararray, AirTime:chararray, Flights:chararray, Distance:chararray, DistanceGroup:chararray, CarrierDelay:chararray, WeatherDelay:chararray, NASDelay:chararray, SecurityDelay:chararray, LateAircraftDelay:chararray);

flight = FOREACH flight GENERATE Year, Month, FlightDate, Origin, Dest, DepTime, ArrTime, ArrDelayMinutes, Diverted, Cancelled;

f1 = FILTER flight BY (Origin == 'ORD' AND Dest != 'JFK');

f2 = FILTER flight BY (Origin != 'ORD' AND Dest == 'JFK');

f1f2SameDate = JOIN f1 BY (FlightDate, Dest), f2 BY (FlightDate, Origin);

f1f2SameDateNoDivertNoCancelTwoLeg = FILTER f1f2SameDate by (f1::Diverted == 0 AND f2::Diverted == 0 AND f1::Cancelled == 0 AND f2::Cancelled == 0) AND (f1::ArrTime < f2::DepTime);

f1YearMonthInRange = FILTER f1f2SameDateNoDivertNoCancelTwoLeg BY (f1::Year == 2007 AND f1::Month >= 6) OR (f1::Year == 2008 AND f1::Month <= 5);

f1f2ArrDelayMin = FOREACH f1YearMonthInRange GENERATE f1::FlightDate AS day, (f1::ArrDelayMinutes + f2::ArrDelayMinutes) AS delayMin;

f1f2DayGroup = GROUP f1f2ArrDelayMin BY day;

f1f2DelayAndCount = foreach f1f2DayGroup generate (float)COUNT(f1f2ArrDelayMin.delayMin) AS delayCount, (float)SUM(f1f2ArrDelayMin.delayMin) AS delaySum;

f1f2Group = GROUP f1f2DelayAndCount ALL;

twoLegTotalCountAndSum = foreach f1f2Group generate SUM(f1f2DelayAndCount.delayCount) AS countTotal, SUM(f1f2DelayAndCount.delaySum) AS delayMinTotal;

twoLegAvg = foreach twoLegTotalCountAndSum generate (delayMinTotal / countTotal) AS delaying;

STORE twoLegAvg INTO 's3://dominic-bucket/hw3/o3' USING PigStorage(',');