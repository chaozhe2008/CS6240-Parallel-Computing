package com.zycao.util;

import com.zycao.model.FlightRecord;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DataParser {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final LocalDate startDate = LocalDate.of(2007, 6, 1);
    private static final LocalDate endDate = LocalDate.of(2008, 5, 31);

    public static FlightRecord convertToFlightRecord(String[] parsed) {

        try {
            FlightRecord record = new FlightRecord();
            record.setFlightDate(parsed[5]);
            record.setOrigin(parsed[11]);
            record.setDest(parsed[17]);
            record.setDepTime(parsed[24]);
            record.setArrTime(parsed[35]);
            record.setArrDelay(parsed[37]);
            record.setCancelled(parsed[41]);
            record.setDiverted(parsed[43]);


            return record;
        } catch (Exception e) {
            // Log error or handle it according to your requirements
            return null;
        }
    }

    public static boolean isValid(FlightRecord record) {
        LocalDate flightDate = LocalDate.parse(record.getFlightDate(), DATE_FORMATTER);
        return ((record.getOrigin().equals("ORD") ^ record.getDest().equals("JFK")) // filter unrelated and direct flights
                && (!flightDate.isBefore(startDate) && !flightDate.isAfter(endDate)) // filter flights out of time range
                && (record.getDiverted().equals("0.00") && record.getCancelled().equals("0.00"))); // filter invalid flights
    }

    public static String convertToStringValue(String type, FlightRecord record) {
        StringBuilder sb = new StringBuilder();
        if (type.equals("F1")){
            sb.append(record.getFlightDate()).append("/");
            sb.append(record.getDest()).append("/");
            sb.append(record.getArrTime()).append("/");
            sb.append(record.getArrDelay());
        } else {
            sb.append(record.getFlightDate()).append("/");
            sb.append(record.getOrigin()).append("/");
            sb.append(record.getDepTime()).append("/");
            sb.append(record.getArrDelay());
        }


        return sb.toString();
    }
}
