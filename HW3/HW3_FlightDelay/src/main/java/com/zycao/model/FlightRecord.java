package com.zycao.model;

import lombok.Data;

@Data
public class FlightRecord {
    private String flightDate;
    private String origin;
    private String dest;
    private String depTime;
    private String arrTime;
    private String ArrDelay;
    private String cancelled;
    private String diverted;
}
