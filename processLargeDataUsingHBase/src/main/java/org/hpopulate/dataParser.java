package org.hpopulate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;

public class dataParser {
    private String year;
    private String month;
    private String ArrDelayMinutes;
    private String AirlineID;
    private String Cancelled;
    private String Diverted;
    private static List<String> headerNames = Arrays.asList(
            "Year","Quarter","Month", "DayofMonth","DayOfWeek","FlightDate","UniqueCarrier","AirlineID",
            "Carrier","TailNum","FlightNum","Origin","OriginCityName","OriginState","OriginStateFips",
            "OriginStateName","OriginWac","Dest","DestCityName","DestState","DestStateFips","DestStateName",
            "DestWac","CRSDepTime","DepTime", "DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups",
            "DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime", "ArrTime","ArrDelay",
            "ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled","CancellationCode",
            "Diverted","CRSElapsedTime","ActualElapsedTime","AirTime","Flights","Distance","DistanceGroup",
            "CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay");
    private static HashMap<String, Integer> neededColumnsIndices = new HashMap<String, Integer>(){{
        put("Year", headerNames.indexOf("Year"));
        put("Month", headerNames.indexOf("Month"));
        put("ArrDelayMinutes", headerNames.indexOf("ArrDelayMinutes"));
        put("AirlineID", headerNames.indexOf("AirlineID"));
        put("Cancelled", headerNames.indexOf("Cancelled"));
        put("Diverted", headerNames.indexOf("Diverted"));
    }};
    public void parse(String row) {
        if (row.length() > 0) {
            Integer start = 0;
            Integer curr = 0;
            // parse the elements in the csv file by ','.
            // need to check edge cases where a comma is in the middle of a single data
            for (int i = 0; i < row.length() - 1; i++) {
                if ((row.charAt(i) == ',' && row.charAt(i + 1) != ' ')) {
                    for (String k : neededColumnsIndices.keySet()) {
                        if (curr == neededColumnsIndices.get(k)) {
                            String input = row.substring(start, i);
                            if (k.equals("Year")) {
                                year = input;
                            } else if (k.equals("Month")) {
                                month = input;
                            } else if (k.equals("ArrDelayMinutes")) {
                                ArrDelayMinutes = input;
                            } else if (k.equals("AirlineID")) {
                                AirlineID = input;
                            } else if (k.equals("Cancelled")) {
                                Cancelled = input;
                            } else if (k.equals("Diverted")) {
                                Diverted = input;
                            }
                        }
                    }
                    curr += 1;
                    start = i + 1;
                }
            }
        }
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidFlight() {
        return !(Diverted.equals("0.00") || Cancelled.equals("0.00"));
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getArrDelayMinutes() {
        return ArrDelayMinutes;
    }

    public String getAirlineID() {
        return AirlineID;
    }

    public String getCancelled() {
        return Cancelled;
    }

    public String getDiverted() {
        return Diverted;
    }
    public String getKey() {
        return getAirlineID() + getMonth();
    }
}
