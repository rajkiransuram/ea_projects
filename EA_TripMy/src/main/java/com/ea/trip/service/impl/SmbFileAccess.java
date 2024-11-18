package com.ea.trip.service.impl;

import jcifs.CIFSContext;
import jcifs.context.SingletonContext;
import jcifs.smb.NtlmPasswordAuthenticator;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import jcifs.smb.SmbFileOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class SmbFileAccess {
    private static final Logger logger = LogManager.getLogger(SmbFileAccess.class);

    private String sharedFolderPath;
    private CIFSContext context;

    public SmbFileAccess(String sharedFolderPath, String domain, String username, String password) {
        this.sharedFolderPath = sharedFolderPath;
        NtlmPasswordAuthenticator auth = new NtlmPasswordAuthenticator(username, password);
        this.context = SingletonContext.getInstance().withCredentials(auth);
    }

    public void readFile(String filePath) {
        try {
            SmbFile file = new SmbFile(sharedFolderPath + filePath, context);
            SmbFileInputStream smbFileInputStream = new SmbFileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(smbFileInputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();
            smbFileInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public void writeFile(String filePath, String content) {
        try {
            SmbFile file = new SmbFile(sharedFolderPath + filePath, context);
            SmbFileOutputStream smbFileOutputStream = new SmbFileOutputStream(file);
            OutputStreamWriter writer = new OutputStreamWriter(smbFileOutputStream);
            writer.write(content);
            writer.flush();
            writer.close();
            smbFileOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
