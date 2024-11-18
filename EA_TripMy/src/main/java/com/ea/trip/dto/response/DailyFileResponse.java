package com.ea.trip.dto.response;

import lombok.Data;

@Data
public class DailyFileResponse {

    private String country;
    private String codePPUC;
    private String value6;
    private String intermediationAgreementCode;
    private String policySubscriptionDate;
    private String cancellationDate;
    private String value1;
    private String value2;
    private String lastName;
    private String value11;
    private String startDateTravel;
    private String endDateTravel;
    private String value8;
    private String grossPremium;
    private String status;
    private String dateLoading;
    private String value4;
    private String beneficiaryGender;
    private String firstName;

    public String getFullName() {
        return firstName + " " + lastName;
    }
}
