package com.ea.trip.controller;

import com.ea.trip.dto.response.DailyFileResponse;
import com.ea.trip.dto.response.MasterFileRecords;
import com.ea.trip.service.FileProcessService;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RestController
@RequestMapping("/file")
public class FileController {

    private static final Logger logger = LogManager.getLogger(FileController.class);

    @Autowired
    ResourceLoader resourceLoader;

    @Value("${blacklist.file.path}")
    private String blackListFilePath;

    @Value("${daily.file.path}")
    private String dailyFilePath;

    @Value("${new.file.path}")
    private String newFilePath;

    @Value("${output.file.path}")
    private String outputFilePath;

    @Autowired
    FileProcessService fileProcessService;


    private String[] csvHeaderNames = {"Country","Code PPUC","Value6","IntermediationAgreementCode","PolicySubscriptionDate","CancellationDate","Value1","Value2","LastName","Value11","StartDateTravel","EndDateTravel","Value8","GrossPremium","Status","DateLoading","Value4","BeneficiaryGender","FirstName"};

    @GetMapping(value = "/validate")
    public ResponseEntity<Map<String, Object>> validateFile() throws IOException {
        Map<String, Object> object = new HashMap<>();
        Map<String, Object> errors = new HashMap<>();
        HttpStatus httpStatus = HttpStatus.OK;

        List<MasterFileRecords> records = new ArrayList<>();
        try {
            records = readExcelIntoPojo(blackListFilePath);
        }catch (Exception ex){
            logger.error("ERROR while processing xls file - "+ex.getMessage());
        }
      //  object.put("RecordList",records);

        Resource fileSystemResource = resourceLoader.getResource("file:/" + dailyFilePath);
        List<DailyFileResponse> csvFileList = readCsvFile(fileSystemResource.getInputStream(),records);
        object.put("DailyRecordList",csvFileList);

        return new ResponseEntity<>(object, httpStatus);
    }

    @GetMapping(value = "/processFile")
    public ResponseEntity<Map<String, String>> processFile() {
        Map<String, String> object = new HashMap<>();
        HttpStatus httpStatus = HttpStatus.OK;

        String sanFileResult = fileProcessService.checkSanctionedFile();
        object.put("Sanction File Status : ",sanFileResult);

        String resultString =  fileProcessService.fileProcess();
        object.put("File Process Status : ", resultString);

        String archiveFilesResult = fileProcessService.archiveFiles();
        object.put("Files Archiving Status : ", archiveFilesResult);

        logger.info("File process successfully completed ");
        return new ResponseEntity<>(object, httpStatus);
    }

    @PostMapping(value = "/uploadCsv")
    public ResponseEntity<Map<String, Object>> uploadCsvFile(@RequestParam MultipartFile file ) throws IOException {
        Map<String, Object> object = new HashMap<>();
        Map<String, Object> errors = new HashMap<>();
        HttpStatus httpStatus = HttpStatus.OK;
        if (file.isEmpty()) {
            errors.put("ERROR_1001", "Please select a file to upload.");
        }
        List<MasterFileRecords> records = new ArrayList<>();
        try {
            records = readExcelIntoPojo(blackListFilePath);
        }catch (Exception ex){
            logger.error("ERROR while processing xls file - "+ex.getMessage());
        }
        List<DailyFileResponse> csvFileList = readCsvFile(file.getInputStream(),records);
        object.put("DailyRecordList",csvFileList);
        return new ResponseEntity<>(object, httpStatus);
    }

    private List<DailyFileResponse> readCsvFile(InputStream is,List<MasterFileRecords> records) throws IOException {
        List<DailyFileResponse> csvFileList = new ArrayList<>();
        CSVPrinter successPrinter = CSVFormat.DEFAULT.print(new FileWriter(getFilePath(true)));
        CSVPrinter errorPrinter = CSVFormat.DEFAULT.print(new FileWriter(getFilePath(false)));
        try (Reader reader = new InputStreamReader(new BOMInputStream(is));
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader().withSkipHeaderRecord(true).withIgnoreHeaderCase().withTrim())) {
            successPrinter.printRecord(csvParser.getHeaderNames());
            errorPrinter.printRecord(csvParser.getHeaderNames());
            List<CSVRecord> csvRecords = csvParser.getRecords();
            List<CSVRecord> successRecords = new ArrayList<>();
            List<CSVRecord> updatedDailyFileRecords = new ArrayList<>();
            boolean valid =false;

            for (CSVRecord csvRecord : csvRecords) {
                CSVRecord updatedRecord = csvRecord;
                if(isValidate(csvRecord,records)) {
                    for (String name : csvParser.getHeaderNames()) {
                        if(name.equalsIgnoreCase("Status")){
                            List<String> errRecordList = getListFromIterator(csvRecord.iterator());
                            errRecordList.remove(14);
                            errRecordList.add(14,"3");
                            updatedRecord = listToCSVRecord(errRecordList);
                        }
                    }
                    if (updatedRecord !=null){
                        successPrinter.printRecord(updatedRecord);
                    }
                }
                successRecords.add(csvRecord);
            }

            for (CSVRecord csvRecord : successRecords) {
                valid = isValidate(csvRecord,records);
                DailyFileResponse csvFileResponse = new DailyFileResponse();
                csvFileResponse.setCountry(csvRecord.get("Country"));
                csvFileResponse.setCodePPUC(csvRecord.get("Code PPUC"));
                csvFileResponse.setValue6(csvRecord.get("Value6"));
                csvFileResponse.setIntermediationAgreementCode(csvRecord.get("IntermediationAgreementCode"));
                csvFileResponse.setPolicySubscriptionDate(csvRecord.get("PolicySubscriptionDate"));
                csvFileResponse.setCancellationDate(csvRecord.get("CancellationDate"));
                csvFileResponse.setValue1(csvRecord.get("Value1"));
                csvFileResponse.setValue2(csvRecord.get("Value2"));
                csvFileResponse.setLastName(csvRecord.get("LastName"));
                csvFileResponse.setValue11(csvRecord.get("Value11"));
                csvFileResponse.setStartDateTravel(csvRecord.get("StartDateTravel"));
                csvFileResponse.setEndDateTravel(csvRecord.get("EndDateTravel"));
                csvFileResponse.setValue8(csvRecord.get("Value8"));
                csvFileResponse.setGrossPremium(csvRecord.get("GrossPremium"));
                csvFileResponse.setStatus(csvRecord.get("Status"));
                csvFileResponse.setDateLoading(csvRecord.get("DateLoading"));
                csvFileResponse.setValue4(csvRecord.get("Value4"));
                csvFileResponse.setBeneficiaryGender(csvRecord.get("BeneficiaryGender"));
                csvFileResponse.setFirstName(csvRecord.get("FirstName"));
                csvFileList.add(csvFileResponse);
                if(valid){
                    CSVRecord errCsvRecord = null;
                    for (String name : csvParser.getHeaderNames()) {
                        if(name.equalsIgnoreCase("Status")){
                            List<String> errRecordList = getListFromIterator(csvRecord.iterator());
                            errRecordList.remove(14);
                            errRecordList.add(14,"3");
                            errCsvRecord = listToCSVRecord(errRecordList);
                        }
                    }
                    errorPrinter.printRecord(errCsvRecord !=null ? errCsvRecord : csvRecord );
                }else {
                    errorPrinter.printRecord(csvRecord);
                }
            }

            // Close resources
            successPrinter.close();
            errorPrinter.close();
            csvParser.close();
            reader.close();
            logger.info("Processing complete.");
        } catch (Exception e) {
            logger.error(" Error R parsing CSV File..! " + e.getMessage());
        }
        return csvFileList;
    }

    public <T> List<T> getListFromIterator(Iterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    private boolean isValidate(CSVRecord csvRecord, List<MasterFileRecords> records) {
        boolean found=false;
        String firstName = csvRecord.get("FirstName");
        String lastName = csvRecord.get("LastName");
        String fullName = firstName + " " + lastName;
        found = records.stream().distinct().anyMatch(record -> record.getFullName().contains(fullName));
        return found;
    }

    private String  getFilePath(boolean val) {
        String fileName = null;
        String path = null;
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd");
        String nowDateTime = formatter.format(dateTime);
        if (val){
            fileName = "TripMYSales_EA";
            path = newFilePath;
            if (!Files.exists(Paths.get(newFilePath))) {
                try {
                    Files.createDirectories(Paths.get(path));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }else{
            fileName = "TripMYSales_EA_";
            path = outputFilePath;
            if (!Files.exists(Paths.get(outputFilePath))) {
                try {
                    Files.createDirectories(Paths.get(path));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return path+nowDateTime+"_"+fileName+".csv";
    }
    private List<MasterFileRecords> readExcelIntoPojo(String filePath) throws IOException {

        List<MasterFileRecords> pojoList = new ArrayList<>();
        InputStream excelFile = Files.newInputStream(Paths.get(filePath));
        String password="GMI032024";

        XSSFWorkbook workbook=(XSSFWorkbook) WorkbookFactory.create(excelFile,password);
        XSSFSheet sheet=workbook.getSheet("Blacklist - Personal Customers");
        Iterator<Row> iterator = sheet.iterator();

        // Skip header row if needed
        if (iterator.hasNext()) {
            iterator.next(); // Skip header row
        }
        MasterFileRecords pojo = new MasterFileRecords(); // Create instance of your POJO class
        try {
            while (iterator.hasNext()) {
                Row currentRow = iterator.next();
                Iterator<Cell> cellIterator = currentRow.iterator();
                // Map each cell to the corresponding field in your POJO
                // Assuming you have 3 columns: col1, col2, col3
                pojo.setRecordNo((int) cellIterator.next().getNumericCellValue());
                pojo.setSource(cellIterator.next().getStringCellValue());
                pojo.setFullName(cellIterator.next().getStringCellValue());
               /* pojo.setIcNumber(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setPassport(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setNationality(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setAddress(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setDob(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setGender(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setRemarks(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setStatus(!Objects.equals(cellIterator.next().getStringCellValue(), "") ? cellIterator.next().getStringCellValue():null);
                pojo.setDateCreated(!Objects.equals(String.valueOf(cellIterator.next().getNumericCellValue()), "")? String.valueOf(cellIterator.next().getNumericCellValue()):null);*/
                pojoList.add(pojo);
            }
        }catch (Exception e){
            logger.error("Error Record ------>  "+pojo.getRecordNo());
        }
        workbook.close();
        return pojoList;
    }

    public static CSVRecord listToCSVRecord(List<String> dataList) {
        StringBuilder sb = new StringBuilder();
        for (String data : dataList) {
            sb.append(data).append(",");
        }
        String csvString = sb.toString();
        // Remove the trailing comma
        if (!csvString.isEmpty()) {
            csvString = csvString.substring(0, csvString.length() - 1);
        }
        try {
            // Create CSVFormat according to your CSV format
            CSVFormat format = CSVFormat.DEFAULT;
            // Parse the CSV string to get CSVRecord
            Iterable<CSVRecord> records = format.parse(new StringReader(csvString));
            // Since we expect only one record
            return records.iterator().next();
        } catch (Exception e) {
           logger.error("Error while parsing CSVFormat : "+e.getMessage(),e); // Handle parsing exceptions appropriately
        }
        return null;
    }

    public static Map<String, String> convertToMap(String csvRecord, String delimiter) {
        Map<String, String> map = new HashMap<>();
        String[] tokens = csvRecord.split(delimiter);

        // Assuming the CSV has a key-value pair structure like "key1,value1,key2,value2"
        for (int i = 0; i < tokens.length; i += 2) {
            String key = tokens[i].trim();
            String value = (i + 1 < tokens.length) ? tokens[i + 1].trim() : ""; // handle odd number of tokens
            map.put(key, value);
        }

        return map;
    }

    public static CSVRecord mapToCSVRecord(Map<String, String> map) throws IOException {
        StringBuilder csvRecord = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            csvRecord.append(entry.getValue()).append(",");
        };
        // Remove the trailing comma and return the CSV record
        String csvRow = csvRecord.deleteCharAt(csvRecord.length() - 1).toString();
        StringReader stringReader = new StringReader(csvRow);
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(map.keySet().toArray(new String[0])).parse(stringReader);
        return records.iterator().next();
    }
}
