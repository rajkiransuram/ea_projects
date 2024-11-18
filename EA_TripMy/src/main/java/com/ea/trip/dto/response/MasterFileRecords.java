package com.ea.trip.dto.response;

import lombok.Data;

@Data
public class MasterFileRecords {

    private Integer recordNo;
    private String source;
    private String fullName;
    private String icNumber;
    private String passport;
    private String Nationality;
    private String address;
    private String dob;
    private String gender;
    private String remarks;
    private String status;
    private String dateCreated;
}
