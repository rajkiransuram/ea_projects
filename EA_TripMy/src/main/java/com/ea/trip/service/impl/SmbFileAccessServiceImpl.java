package com.ea.trip.service.impl;

import com.ea.trip.service.SmbFileAccessService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SmbFileAccessServiceImpl implements SmbFileAccessService {

    @Value("${daily.file.path}")
    private String filePath;

    @Override
    public void call() {

    }

    @Override
    public void read() {
        String sharedFolderPath = "smb://eathpsta007elrs.eashared.net/data-travel/";
        // "\\eathpsta007elrs.eashared.net\data-travel"
        String domain = ""; // If there's no domain, leave this empty
       String username = "thaddw-svcmarc_uat";
        String password = "1R78ib*f@prO5iva3lt9b$Se";


        SmbFileAccess smbFileAccess = new SmbFileAccess(sharedFolderPath, domain, username, password);

        // Reading a file
        smbFileAccess.readFile(filePath+"/240828_TripMYSales_EA.csv");

        // Writing to a file
      //  smbFileAccess.writeFile("240828_TripMYSales_EA", "Hello, world!");
    }

    @Override
    public void write() {

    }
}
