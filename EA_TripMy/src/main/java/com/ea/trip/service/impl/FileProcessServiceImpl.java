package com.ea.trip.service.impl;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class FileProcessServiceImpl implements FileProcessService {

    @Value("${blacklist.file.path}")
    private String blackListFilePath;

    @Value("${daily.file.path}")
    private String dailyFilePath;

    @Value("${new.file.path}")
    private String newFilePath;

    @Value("${output.file.path}")
    private String outputFilePath;
    @Value("${pwd.enabled}")
    private String pwdEnable;
    @Value("${pwd.value}")
    private String password;
    @Value("${sheet.name}")
    private String sheetName;
    @Value("${archive.source.path}")
    private String srcDir;
    @Value("${archive.target.path}")
    private String targetDir;

    @Value("${sanctioned.file.path}")
    private String sanctionedFileDir;

    @Value("${sanctioned.target.filename}")
    private String targetFileName;

    @Value("${sanctioned.new.filename}")
    private String newFileName;

    private static final Logger logger = LogManager.getLogger(FileProcessServiceImpl.class);

    //4:00 PM Daily
    @Scheduled(cron = "0 0 8 * * ?")
    public void callFileProcess(){
        logger.info("START FILE PROCESS.....!");
        String sanctionFileResult = checkSanctionedFile(); //master is re-nameing and backup the master file into Archive folder
        logger.info("sanctionFileResult - {} ",sanctionFileResult);
        String fileProcessResult = fileProcess(); //read Daily file get fname & Lname concatnamte and chjeck the name agains master file name list generate output file in put the AZ-FS outgoing folder
        logger.info("fileProcessResult - {} ",fileProcessResult);
        String archivingFileResult = archiveFiles(); // after process move the daily files to Archive
        logger.info("archivingFileResult - {} ", archivingFileResult);
        logger.info("END FILE PROCESS.....!");
    }

    @Override
    public String checkSanctionedFile() {
        // Specify the directory paths
        String directoryPath = sanctionedFileDir;  // Change to your directory
        String status = "";
        // Step 2: Check if the file 'Sanction Blacklist - Individuals - *.xlsx' exists and rename it
        File directory = new File(directoryPath);
        File[] matchingFiles = directory.listFiles((dir, name) -> name.matches(targetFileName));
        if (matchingFiles != null && matchingFiles.length > 0) {
            archivedMasterFile();
            deleteExistingFile();
          //  if(isDeleted) {
                File oldFile = matchingFiles[0];  // If there's more than one match, use the first one
                File newFile = new File(directoryPath + newFileName);
                if (oldFile.renameTo(newFile)) {
                    logger.info("File renamed successfully.");
                    status = "File renamed successfully.";
                } else {
                    logger.info("Failed to rename the file.");
                    status = "Failed to rename the file.";
                }
       //     }
        } else {
            logger.info("No matching file found to rename.");
            status = "No matching file found to rename.";
        }

    return status;
    }

    private void deleteExistingFile(){
        // Step 1: Check if 'Sanction_Blacklist_Individuals.xlsx' exists in x folder and delete it
       // boolean isDeleted = false;
        String xFolderPath = sanctionedFileDir;     // Change to x folder location
        Path filePathToDelete = Paths.get(xFolderPath + newFileName);
        if (Files.exists(filePathToDelete)) {
            try {
                Files.delete(filePathToDelete);
          //      isDeleted = true;
                logger.info("File deleted successfully.");
            } catch (Exception e) {
                logger.error("Error while delete existing file - {} ",e.getMessage());
            }
        } else {
            logger.info("No matching file found to delete.");
        }
        //return isDeleted;
    }

    private void archivedMasterFile(){
        Path sourceDir = Paths.get(sanctionedFileDir); // Replace with your source directory path
        Path archiveDir = Paths.get(sanctionedFileDir+"/Archive/"); // Replace with your archive directory path

        String regex = "Sanction Blacklist - Individuals - .*\\.xlsx"; // Regex for file name
        Pattern pattern = Pattern.compile(regex);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceDir)) {
            for (Path file : stream) {
                String fileName = file.getFileName().toString();
                // Check if the file name matches the pattern
                if (pattern.matcher(fileName).matches()) {
                    // Destination path for the file in the archive directory
                    Path targetPath = archiveDir.resolve(file.getFileName());
                    // Copy file from source to target (archive directory)
                    Files.copy(file, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    logger.info("File copied to archive: " + fileName);
                }
            }
        } catch (IOException e) {
            logger.error("Error while coping Master file in Archive folder - {}",e.getMessage());
        }
    }

    @Override
    public String fileProcess() {
        List<MasterFileRecords> records = new ArrayList<>();
        List<DailyFileResponse> csvFileList= new ArrayList<>();
        String nowDate = getPreviousDay();
        try {
            records = readExcelIntoPojo(blackListFilePath);
        }catch (Exception ex){
            logger.error("ERROR while processing xlsx file - "+ex.getMessage());
            throw new RuntimeException("ERROR while processing xlsx file", ex);
        }
        List<File> filesList  = getFilesFromDirectory(dailyFilePath);
        for (File file : filesList){
       // File filePath = new File(dailyFilePath+"/"+nowDate+"_TripMYSales_EA.csv");
        try {
            InputStream targetStream = Files.newInputStream(file.toPath());
            csvFileList = readCsvFile(targetStream,records, file.toPath());
        } catch (IOException e) {
            throw new RuntimeException("Error while validating daily file against Master file", e);
        }
        }
        logger.info("Processed file records size - "+csvFileList.size());
        return "File Process Successfully Completed.....! ";
    }

    private String archiveFilesFromIncoming() {
        Path sourceDir = Paths.get(srcDir);
        Path archiveDir = Paths.get(targetDir);
        String status = "";
        if (!Files.exists(archiveDir)) {
            try {
                Files.createDirectories(archiveDir);
            } catch (IOException e) {
               logger.error("Failed to create target directory - {} ",e.getMessage());
            }
        }

        try (Stream<Path> files = Files.list(sourceDir)) {
            // Iterate over the files in the source directory and move them
            files.filter(f -> !f.getFileName().endsWith("Archive")).forEach(file -> {
                try {
                    // Define the target path
                    Path targetPath = archiveDir.resolve(file.getFileName());
                    // Move the file to the target directory
                    Files.move(file, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    System.out.println("Moved: " + file);
                } catch (IOException e) {
                    logger.error("Error moving file: " + file);
                    logger.error("Error while moving file: {}" , e.getMessage());
                }
            });
            logger.info("All files moved successfully.");
            status = "All files moved successfully.";
        } catch (IOException e) {
            logger.error("Error reading source directory. {}",e.getMessage());
        }
        return status;
    }

    @Override
    public String archiveFiles() {
        return archiveFilesFromIncoming();
    }

    private List<File> getFilesFromDirectory(String dailyFilePath) {
        File directory = new File(dailyFilePath);
        return Arrays.stream(Objects.requireNonNull(directory.listFiles())).filter(file -> file.getName().contains("_TripMYSales_EA")).collect(Collectors.toList());
    }

    private String getPreviousDay(){
        LocalDate yesterdayDate = LocalDate.now().minusDays(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyMMdd");
        return formatter.format(yesterdayDate);
    }

    private List<MasterFileRecords> readExcelIntoPojo(String filePath) throws IOException {

        List<MasterFileRecords> pojoList = new ArrayList<>();
        InputStream excelFile = Files.newInputStream(Paths.get(filePath));

        XSSFWorkbook workbook= new XSSFWorkbook();
        if(pwdEnable.equalsIgnoreCase("true")){
               workbook=(XSSFWorkbook) WorkbookFactory.create(excelFile,password);
        }else{
              workbook=(XSSFWorkbook) WorkbookFactory.create(excelFile);
        }
        XSSFSheet sheet=workbook.getSheet(sheetName);
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
        logger.info("Master file record size - "+pojoList.size());
        workbook.close();
        return pojoList;
    }

    private List<DailyFileResponse> readCsvFile(InputStream is, List<MasterFileRecords> records, Path filePath) throws IOException {
        List<DailyFileResponse> csvFileList = new ArrayList<>();
        String fileDate = getFileDate(filePath);
        CSVPrinter successPrinter = CSVFormat.DEFAULT.print(new FileWriter(getFilePath(true,fileDate)));
        CSVPrinter errorPrinter = CSVFormat.DEFAULT.print(new FileWriter(getFilePath(false,fileDate)));
        try (Reader reader = new InputStreamReader(new BOMInputStream(is));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(';').withFirstRecordAsHeader().withSkipHeaderRecord(true).withIgnoreHeaderCase().withTrim())) {
            successPrinter.printRecord(csvParser.getHeaderNames());
            errorPrinter.printRecord(csvParser.getHeaderNames());
            List<CSVRecord> csvRecords = csvParser.getRecords();
            List<CSVRecord> successRecords = new ArrayList<>();
            List<CSVRecord> updatedDailyFileRecords = new ArrayList<>();
            boolean valid =false;
            int counter = 0;
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
                        logger.info("Matched Records : "+ counter++);
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

    private String getFileDate(Path filePath) {
        String file = String.valueOf(filePath.getFileName());
        String[] fileNameArray = file.split("_");
        return fileNameArray[0];
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

    private String  getFilePath(boolean val,String date) {
        String fileName = null;
        String path = null;
        String nowDate = getPreviousDay();
        if (val){
            fileName = "sanction_list_Tripcom_GEN";
            path = newFilePath;
            if (!Files.exists(Paths.get(newFilePath))) {
                try {
                    Files.createDirectories(Paths.get(path));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }else{
            fileName = "TripMYSales_EA";
            path = outputFilePath;
            if (!Files.exists(Paths.get(outputFilePath))) {
                try {
                    Files.createDirectories(Paths.get(path));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return path+date+"_"+fileName+".csv";
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
}
