package com.zycao.util;

import com.zycao.model.FlightRecord;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DataParser {

    /**
     * Convert each line to an FlightRecord object.
     * @param parsed
     * @return
     */
    public static FlightRecord convertToFlightRecord(String[] parsed) {

        try {
            FlightRecord record = new FlightRecord();
            record.setYear(parsed[0]);
            record.setMonth(parsed[2]);
            record.setFlightDate(parsed[5]);
            record.setCarrier(parsed[6]);
            record.setArrDelay(parsed[37]);
            record.setCancelled(parsed[41]);
            return record;
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * Filter record based on date and cancelled information
     * @param record
     * @return
     */
    public static boolean isValid(FlightRecord record) {
        return (record.getYear().equals("2008") && record.getCancelled().equals("0.00")
                && (!record.getArrDelay().equals("") && !record.getFlightDate().equals(""));
    }

    /**
     * utility function to format values for phase 1 mapper output
     * @param type
     * @param record
     * @return
     */
//    public static String convertToStringValue(String type, FlightRecord record) {
//        StringBuilder sb = new StringBuilder();
//        if (type.equals("F1")){
//            sb.append(record.getFlightDate()).append("/");
//            sb.append(record.getDest()).append("/");
//            sb.append(record.getArrTime()).append("/");
//            sb.append(record.getArrDelay());
//        } else {
//            sb.append(record.getFlightDate()).append("/");
//            sb.append(record.getOrigin()).append("/");
//            sb.append(record.getDepTime()).append("/");
//            sb.append(record.getArrDelay());
//        }
//
//        return sb.toString();
//    }
}
