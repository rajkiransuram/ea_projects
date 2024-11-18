package com.ea.trip.dto.response;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ValidateCsvResponse implements Serializable {

    private static final long serialVersionUID = -5593679663531011927L;
    private boolean isValid;
    private Map<String, Object> errors = new HashMap<>();
    private List<String> errorRecords =new ArrayList<>();
    private Map<String, Object> info = new HashMap<>();
}
