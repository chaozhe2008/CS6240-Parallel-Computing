package com.zycao.model;

import lombok.Data;

@Data
public class FlightRecord {
    private String year;
    private String month;
    private String flightDate;
    private String carrier;
    private String ArrDelay;
    private String cancelled;
}
