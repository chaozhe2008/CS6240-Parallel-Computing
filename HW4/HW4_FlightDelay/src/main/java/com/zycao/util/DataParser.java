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
            record.setArrDelay(parsed[37].isEmpty() ? "0.00" : parsed[37]);
            record.setCancelled(parsed[41]);
            return record;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * Filter record based on date and cancelled information
     * @param record
     * @return
     */
    public static boolean isValid(FlightRecord record) {
        return record.getYear().equals("2008") && record.getCancelled().equals("0.00")
                && !record.getArrDelay().equals("") && !record.getFlightDate().equals("");
    }
}
