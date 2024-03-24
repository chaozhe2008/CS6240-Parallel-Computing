package com.zycao.model;

import lombok.Data;

/**
 * an object representing a flight record
 */
@Data
public class FlightRecord {
    private String year;
    private String month;
    private String flightDate;
    private String carrier;
    private String ArrDelay;
    private String cancelled;
}
