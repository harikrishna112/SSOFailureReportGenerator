package sso.failure.report;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import oracle.jdbc.datasource.impl.OracleDataSource;

public class DBQueryExecutor {
	/* 43 */ private static String PROP_FILES_PATH = "";
	private static String TYPE = "";
	private static String SQL_FILE_PATH = "";
	/* 44 */ private static Properties prop = new Properties();

	private Map<String,String> fileNameMap = new HashMap<String,String>();
	private Map<String,String[]> statusAndDate = new HashMap<String,String[]>();
	private PrintWriter pw;
	/* 47 */ private FileWriter out = null;

	/* 49 */ private Connection conn = null;
	private Connection fileNameConn = null;
	private Connection fileStatusConn = null;

	/* 53 */ private static final SimpleDateFormat fullDateFormatter = new SimpleDateFormat("MMddyyyy");

	public static void main(String[] args) throws Exception {
		/* 57 */ System.out.println("Entering in main..... ");
		/* 58 */ InputStream inputStream = null;

		try {
			/* 61 */ String filePath = args[0];
			String startDate = null;
			int noOfDays =0;
			if(args.length >1) {
				startDate = args[1];
				noOfDays= args.length ==3 ? Integer.valueOf( args[2] ==null ? "7" : args[2]) : 7;
			}
			/* 63 */ String user = null;
			/* 64 */ PROP_FILES_PATH = filePath;
			// + "\\dbquery.properties";
			/* 65 */ System.out.println("PROP_FILES_PATH = " + PROP_FILES_PATH);
			/* 66 */ inputStream = new FileInputStream(PROP_FILES_PATH);
			/* 67 */ prop.load(inputStream);
			/* 68 */ System.out.println("Properties loaded successfully..... ");

			String dataSource = prop.getProperty("DATASOURCE_URL");
			/* 74 */ String providerURL = prop.getProperty("USER_NAME");
			/* 75 */ String dbPassword = prop.getProperty("PASSWORD");
			String csvFilePath = prop.getProperty("inputCsvFilePath");
			String outPutcsvFilePath = prop.getProperty("outputCsvFilePath");
			String loggerReportsFilesPath = prop.getProperty("loggerReportFolderPath");
			String trackerReportsFilesPath = prop.getProperty("trackerReportFolderPath");

			System.out.println("Input Csv Path = " + csvFilePath);

			System.out.println("Output Csv Path = " + outPutcsvFilePath);
			/* 77 */ DBQueryExecutor obj = new DBQueryExecutor();

			if (startDate != null && !startDate.trim().isEmpty()) {
				obj.createInputFile(startDate, loggerReportsFilesPath, trackerReportsFilesPath,noOfDays);
			}else {
				obj.extractReportData(dataSource, providerURL,user, dbPassword, csvFilePath,outPutcsvFilePath );
//				obj.isEligLoadDataExists("477", "TIWAA", "ABABIO", "C:\\Users\\kirankumar-pedakotla\\Downloads\\sso-failure-program\\isisCommHealthEligData.txt.bak");
			}

//			

		}
		/* 82 */ catch (Exception e) {

			/* 84 */ e.printStackTrace();
		}
	}

	public boolean isEligLoadDataExists(String clientId, String firstName, String lastName,String filePath)  {
		BufferedReader reader;
		try {
			reader = this.getFileBufferedReader(filePath);
		
		String currentLine =null;
		
		while((currentLine= reader.readLine()) != null) {
			//TODO find the clientId, fistname and lastname
			String cid = currentLine.substring(1, 6);
			String fname = currentLine.substring(29, 49);
			String lname = currentLine.substring(49, 69);
			System.out.println(cid+"----"+cid.length()+"--"+fname+fname.length()+"--"+"==="+lname+"---"+lname.length());
			if(cid.trim().equalsIgnoreCase(clientId) && firstName.equalsIgnoreCase(fname.trim()) && lname.trim().equalsIgnoreCase(lastName)) {
				return true;
			}
		}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	private BufferedReader getFileBufferedReader(String fileName) throws IOException, JSchException, SftpException {
//		BufferedReader reader = null;
		//TODO write logic to connect to the 35V server through ssh and get inputStream
		
		
		String host = "remote_host"; // Remote hostname or IP
        int port = 22;               // SSH port, typically 22
        String username = "harikrishna-kasagoni";
        String password = "Navyasr!143";
        //String remoteFile = "/path/to/remote/file.txt";
        String remoteFile = "/web/maw/perplans/datatransformation/dataload/arch/";

        JSch jsch = new JSch();

            // Establishing SSH connection
            Session session = jsch.getSession(username, host, port);
            session.setPassword(password);
            
            // Avoiding UnknownHostKey issue, don't use in production
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            
            session.connect();
 // Open SFTP channel
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;

            // Reading file from remote server
            InputStream inputStream = sftpChannel.get(remoteFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            
            reader = new BufferedReader(new FileReader(fileName));
		
		
		return reader;
	}
	private void createInputFile(String startDate, String loggerReportsFilesPath, String trackerReportsFilesPath, int noOfDays) throws ParseException {
		// TODO Auto-generated method stub
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyy");
		try {
			System.out.println(startDate);
			Date weekStartDate = sdf.parse(startDate);
			System.out.println(weekStartDate);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(weekStartDate);
			String[] loggerFileNames = new String[noOfDays];
			String[] trackerFileNames = new String[noOfDays];
			int calculatedMonth = (calendar.get(Calendar.MONTH) + 1);
			String calculatedMonthName = calculatedMonth < 10 ? ("0" + String.valueOf(calculatedMonth))
					: String.valueOf(calculatedMonth);
			int calculatedDay = calendar.get(Calendar.DAY_OF_MONTH);
			String calculatedDayName = calculatedDay < 10 ? ("0" + String.valueOf(calculatedDay))
					: String.valueOf(calculatedDay);
			loggerFileNames[0] = ("SSOLoggingReport_" + calendar.get(Calendar.YEAR) + "-" + calculatedMonthName + "-"
					+ calculatedDayName + ".csv");
			trackerFileNames[0] = ("SSOTrackerReport_" + calendar.get(Calendar.YEAR) + "-" + calculatedMonthName + "-"
					+ calculatedDayName + ".csv");

			List<String[]> newInputRecords = new ArrayList<>();

			for (int i = 1; i < noOfDays; i++) {
				calendar.add(Calendar.DAY_OF_YEAR, 1);
				calculatedMonth = (calendar.get(Calendar.MONTH) + 1);
				calculatedDay = calendar.get(Calendar.DAY_OF_MONTH);
				calculatedDayName = calculatedDay < 10 ? ("0" + String.valueOf(calculatedDay))
						: String.valueOf(calculatedDay);
				calculatedMonthName = calculatedMonth < 10 ? ("0" + String.valueOf(calculatedMonth))
						: String.valueOf(calculatedMonth);
				loggerFileNames[i] = ("SSOLoggingReport_" + calendar.get(Calendar.YEAR) + "-" + calculatedMonthName
						+ "-" + calculatedDayName + ".csv");
				trackerFileNames[i] = ("SSOTrackerReport_" + calendar.get(Calendar.YEAR) + "-" + calculatedMonthName
						+ "-" + calculatedDayName + ".csv");

			}

			System.out.println(loggerFileNames.length + "---" +loggerFileNames[loggerFileNames.length-1]);
			System.out.println(trackerFileNames.length + "---" + trackerFileNames[trackerFileNames.length-1]);
			String currentFileDate = "", initialFileDate = "";
			for (int x = 0; x < noOfDays; x++) {

				int startIndex = trackerFileNames[x].indexOf("SSOTrackerReport_");
				int endIndex = trackerFileNames[x].indexOf(".csv", startIndex + "SSOTrackerReport_".length());

				if (startIndex != -1 && endIndex != -1) {
					currentFileDate = trackerFileNames[x].substring(startIndex + "SSOTrackerReport_".length(),
							endIndex);
					if (x == 0) {
						initialFileDate = currentFileDate;
					}
				}
				String logFilePath = loggerReportsFilesPath + File.separator + loggerFileNames[x];
//				logFilePath = logFilePath.replaceAll("/", "\\");
				System.out.println(logFilePath);
				
				if(!(new File(logFilePath)).exists()) {
					System.out.println("================================= Log file path "+ logFilePath   +" does not exists assuming tracker file also not exists, skipping data exists");
					continue;
				}
					
				List<CSVRecord> logRecords = this.getCsvRecordsOfFile(logFilePath);

				String trackFilePath = trackerReportsFilesPath + File.separator + trackerFileNames[x];
//				trackFilePath = trackFilePath.replaceAll("^\"|\"$", "");
				System.out.println(trackFilePath);
				List<CSVRecord> trackReportRecords = this.getCsvRecordsOfFile(trackFilePath);

				if(logRecords == null || trackReportRecords ==null) {
					System.out.println( "Log Records :"+ (logRecords ==null ? "0":logRecords.size()));
					System.out.println( "Track Records :"+ (trackReportRecords ==null ? "0":trackReportRecords.size()));
					continue;
				}
				
				List<CSVRecord> autoChoiceLogRecords = logRecords.stream().filter(obj -> {

					boolean match = obj.get("SOURCE NAME").toLowerCase().contains("autochoice");
//							System.out.println(obj.get("SOURCE NAME") +"==="+match +"======" + obj.toMap().toString());
					return match;
				}).collect(Collectors.toList());

				List<CSVRecord> autoChoiceTrackRecords = trackReportRecords.stream().filter(trackObj -> {

					boolean trackMatch = ((trackObj.get("SOURCE NAME").toLowerCase().contains("autochoice"))
							&& (trackObj.get("SUCCESS STATUS").toLowerCase().contains("n")));
//							System.out.println(trackObj.get("SOURCE NAME").toLowerCase()+"==="+trackMatch +"===="+( trackObj.get("SOURCE NAME").toLowerCase().contains("autochoice"))+"====" +  trackObj.get("SUCCESS STATUS") +"===" + ( trackObj.get("SUCCESS STATUS").toLowerCase().contains("n")));
					return trackMatch;
				}).collect(Collectors.toList());

				
				System.out.println(logRecords.size() + "===" + autoChoiceLogRecords.size() + "======="
						+ autoChoiceLogRecords.get(0).toString());
				System.out.println(trackReportRecords.size() + "===" + autoChoiceTrackRecords.size() + "======="
						+ autoChoiceTrackRecords.get(0).toString());
				for (CSVRecord autoChoiceLogRecord : autoChoiceLogRecords) {

					String clientId = autoChoiceLogRecord.get("CLIENT ID");
					List<CSVRecord> autoChoiceClientTrackRecords = autoChoiceTrackRecords.stream()
							.filter(trackRecordItem -> {

								boolean match = trackRecordItem.get("CLIENT ID").equalsIgnoreCase(clientId);
//							System.out.println(obj.get("SOURCE NAME") +"==="+match +"======" + obj.toMap().toString());
								return match;
							}).collect(Collectors.toList());

					List<CSVRecord> clientDuplicateTrackRecords = this.findDuplicates(autoChoiceClientTrackRecords);
					int newRecCount = 0;
					for (CSVRecord clientTrackRecord : clientDuplicateTrackRecords) {

						newInputRecords.add(new String[] { clientId, clientTrackRecord.get("CLIENT NAME"),
								clientTrackRecord.get("FIRST NAME"), clientTrackRecord.get("LAST NAME"),
								(clientTrackRecord.get("LOG DESC") == null ? "" : clientTrackRecord.get("LOG DESC")).replace("Auto SSOHelper:", ""), "", "", currentFileDate });
						newRecCount++;
					}
					System.out.println("==== new recoreds for client" + clientId + " count is " + newRecCount + "===");
				}
			}

			Path filePath = Paths.get(PROP_FILES_PATH);
			Path parentDir = filePath.getParent();
			Path inputFilePath = null;
			if (parentDir != null) {
				inputFilePath = parentDir
						.resolve("Weekly_SSO_Failure_Report_From_" + initialFileDate + "_" + currentFileDate+".csv");
				Files.createFile(inputFilePath);
				System.out
						.println("input file path derive=================================" + inputFilePath.toString());

				System.out.println("writing created input to the input file.");

				try (BufferedWriter writer = Files.newBufferedWriter(inputFilePath);

						CSVPrinter csvPrinter = new CSVPrinter(writer,
								CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase())) {
					// Print the header record
					csvPrinter.printRecord("CLIENT_ID", "CLIENT_NAME", "FIRST_NAME", "LAST_NAME", "Reason_for_Failure",
							"Last_Elig_Load", "Latest_Elig_File_date", "Failed_date");

					// Print the updated records
					for (String[] record : newInputRecords) {
						csvPrinter.printRecord((Object[]) record);
					}
				}
			} else {
				System.out.println("=========input file cannot be created======");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	private List<CSVRecord> findDuplicates(List<CSVRecord> csvRecords) {
		Map<String, List<CSVRecord>> map = new HashMap<>();
		for (CSVRecord csvRecord : csvRecords) {
			String key = csvRecord.get("FIRST NAME") + "|" + csvRecord.get("LAST NAME");
			map.computeIfAbsent(key, k -> new ArrayList<>()).add(csvRecord);
		}

		List<CSVRecord> duplicates = new ArrayList<>();
		for (List<CSVRecord> csvRecordList : map.values()) {
			if (csvRecordList.size() > 1) {
				duplicates.add(csvRecordList.get(csvRecordList.size() - 1));
			}
		}

		return duplicates;
	}

	private List<CSVRecord> getCsvRecordsOfFile(String csvFilePath) {
		System.out.println("reading the csv file" + csvFilePath);
		List<CSVRecord> records = null;
		CSVFormat format = CSVFormat.EXCEL.withFirstRecordAsHeader().withIgnoreHeaderCase()
				.withIgnoreSurroundingSpaces(true).withQuote(null).withEscape('\\').withTrim()
				.withSkipHeaderRecord(true);
		// Read and process the CSV file
		try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
				CSVParser csvParser = new CSVParser(reader, format)) {
			records = csvParser.getRecords();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Exception in reading the csv file " + csvFilePath);
		}
		return records;
	}

	private void extractReportData(String datasourceName, String providerURL, String user, String dbPassword,
			String csvFilePath, String outputCsvFilePath) throws Exception {
		/* 91 */ System.out.println("Inside getPaymentInfoList() \n");
		/* 92 */ DataSource dataSource = getDataSource(datasourceName, providerURL, dbPassword);

		/* 94 */ String reasonFailureSQL = "SELECT hse.hse_gid," + " hse.HSE_FIRSTNAME, "
				+ "hse.HSE_LASTNAME,HSE.HSE_STATE," + "hse.hse_isenabled, " + " hsh.HSH_ORIGINATEDAPPID, "
				+ "hse.hse_username, " + "hse.hse_email "
				+ " FROM ISIS_OWNER.T_IOHSE_HOUSEHOLDSEARCH  hse, ISIS_OWNER.T_IOCLI_CLIENT cli,ISIS_OWNER.T_IOACO_ACCOUNTOWNER acc,ISIS_OWNER.T_IOHSH_HOUSEHOLD  hsh "
				+ " WHERE hse.hse_client = cli.cli_gid " + "AND hse.HSE_GID=hsh.HSH_GID"
				+ " AND hse.HSE_ACCOUNTOWNER = acc.ACO_GID " + "AND cli.cli_clientid = ? "
				+ " and UPPER(hse.hse_firstname) like UPPER(?)         "
				+ " and UPPER(hse.hse_lastname) like UPPER(?)   ";
		String fileNameSQL = "select * from ISIS_OWNER.registered_file_isrf where emp_isis_client_id in (?) ";
		String fileStatusSQL = "select * from ISIS_OWNER.processed_file_isrf_hist where PROCESS_FILE_NM like ? order by FILE_TIMESTAMP desc ";

		/* 98 */ ResultSet rs = null;
		this.conn = dataSource.getConnection();
		this.conn.setAutoCommit(false);
		PreparedStatement fileNamePrtstmt = this.conn.prepareStatement(fileNameSQL);
		PreparedStatement prtstmt = dataSource.getConnection().prepareStatement(reasonFailureSQL);
		PreparedStatement fileStatusPrtstmt = dataSource.getConnection().prepareStatement(fileStatusSQL);
		try {

			int notFoundUsersCount = 0;
			int notEligibleUsersCount = 0;
			int foundUsersCount = 0;
			int deltedusersCount = 0;
			int inputRecordCount = 0;
			List<CSVRecord> records = new ArrayList<>();
			CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withQuote('"');
			List<String[]> updatedRecords = new ArrayList<>();
//			getProcessStatusAndDate(fileNamePrtstmt, fileStatusPrtstmt, clientId1);
			// Read and process the CSV file
			try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
				CSVParser csvParser = new CSVParser(reader, format);

				String fileStatus = null;
				String fileDate = null;
//				System.out.println("Total No of Records in input file:  "+ csvParser.getRecords().size());
				for (CSVRecord record : csvParser) {
					inputRecordCount++;
					String clientId = record.get("CLIENT_ID");
					clientId = clientId != null ? clientId.replaceAll("\"", "") : clientId;
//					System.out.println("CSV Value======" + clientId + " Csv Value Length:====="+ clientId.length());
//					System.out.println("NON CSV Value: " + clientId1 + "NON  Csv Value Length:"+ clientId1.length());
					if (clientId.length() < 3) {
						clientId = String.format("%" + (3 - clientId.length() + "s"), " ").replace(' ', '0') + clientId;
					}
//					getProcessStatusAndDate(fileNamePrtstmt, fileStatusPrtstmt, clientId);
//					getProcessStatusAndDate(fileNamePrtstmt, fileStatusPrtstmt, "170");
					String firstName = record.get("FIRST_NAME").trim().replaceAll("[^a-zA-Z0-9]", "").replaceAll("\"", "");
//					System.out.println("CSV Value======" + firstName + " Csv Value Length:====="+ firstName.length());
					String lastName = record.get("LAST_NAME").trim().replaceAll("[^a-zA-Z0-9]", "").replaceAll("\"", "");
//					System.out.println("CSV Value======" + lastName + " Csv Value Length:====="+ lastName.length());
					String reasonForFailure = record.get("Reason_for_Failure").replaceAll("\"", "");
					String clientName = record.get("CLIENT_NAME").replaceAll("\"", "").replace("Auto SSOHelper:", "");
					if (reasonForFailure.toLowerCase().contains("employee not found")) {

						prtstmt.setString(1, clientId);
						prtstmt.setString(2, firstName);
						prtstmt.setString(3, lastName);
						System.out.println("main query executed");
						rs = prtstmt.executeQuery();
						boolean recordFound = false;
						boolean allrecordsEnabled = true;
						boolean recordEnabled = false;
						while (rs.next()) {
							recordFound = true;
							if (rs.getInt(5) == 1) {
								recordEnabled = true;
							} else {
								allrecordsEnabled = false;
							}

						}
						
						String[] processStatusAndDate = this.getProcessStatusAndDate(fileNamePrtstmt, fileStatusPrtstmt,
								clientId);
						fileDate = (processStatusAndDate == null ? "" : (processStatusAndDate[1] ==null ? "" : processStatusAndDate[1]));
						fileStatus = (processStatusAndDate == null ? "" : (processStatusAndDate[0] ==null ? "" : processStatusAndDate[0]));
						System.out.println(" client id: " + clientId + ", firstname: " + firstName + ", lastname: "
								+ lastName + " found one record with Reason_for_Failure  equal to Employee not found" +" filestatus :" + fileStatus+" File Date :" + fileDate);
						String failedDate = record.get("Failed_date");
						
						
						if (!recordFound) {
							notFoundUsersCount++;
							Boolean clientExistsInEligFile = this.isEligLoadDataExists(clientId, firstName, lastName, this.getFileName(fileNamePrtstmt, clientId));
							String logDesc=" Employee not found: User is not present in DB";
							if(clientExistsInEligFile) {
								logDesc = "Employee not found: User is not present in DB and present in elig file loaded on "+fileDate;
								//TODO add additional condition logic
							}else {
								logDesc = "Employee not found: User is not present in DB and not present in elig file loaded on "+ fileDate;
							}
							String iColumnValue ="";
							SimpleDateFormat dateFormat  = new SimpleDateFormat("MM-dd-yyyy");
							if(fileDate != null && fileDate.isEmpty() && failedDate != null & failedDate.isEmpty()) {
								Date failedDateValue =dateFormat.parse(failedDate);
								Date eligLoadDateValue =dateFormat.parse(fileDate);
								
								if(failedDateValue.compareTo(eligLoadDateValue) >0) {
									iColumnValue = "File load process completed successfully on "+fileDate;
								}else if(failedDateValue.compareTo(eligLoadDateValue) ==0){
									iColumnValue ="‘File load process was failed due to Minimum threshold percentage matching criteria is 70% or it might be any other reason also as per process status in DB"+fileDate;
								}
							}
							
							updatedRecords
									.add(new String[] { clientId, clientName,firstName,
											lastName, logDesc,
											fileStatus, fileDate, failedDate,iColumnValue });
						} else if (!allrecordsEnabled) {
							foundUsersCount++;
							updatedRecords.add(new String[] { clientId, clientName,
									firstName, lastName,
									"Employee not found: User is present in DB with disable state.", fileStatus,
									fileDate, failedDate });
						} else {
							deltedusersCount++;
							System.out.println(" client id: " + clientId + ", firstname: " + firstName + ", lastname: "
									+ lastName + " found in the database but all enabled.");
						}

					} else {
						notEligibleUsersCount++;
						String[] processStatusAndDate = this.getProcessStatusAndDate(fileNamePrtstmt, fileStatusPrtstmt,
								clientId);
						updatedRecords.add(new String[] { clientId, clientName, firstName,
								lastName, reasonForFailure, (processStatusAndDate == null ? "" : (processStatusAndDate[0] ==null ? "" : processStatusAndDate[0])),
								(processStatusAndDate == null ? "" : (processStatusAndDate[1] ==null ? "" : processStatusAndDate[1])),
								record.get("Failed_date") });
						System.out.println(
								" client id: " + clientId + ", firstname: " + firstName + ", lastname: " + lastName
										+ " found one record with Reason_for_Failure not equal to Employee not found" +"file Status: "+ (processStatusAndDate == null ? "" : processStatusAndDate[1]));
					}
					Thread.sleep(3000);
				}

				System.out.println(" Total input Reocrds: " + inputRecordCount + "\r\n Total Output records:"
						+ updatedRecords.size() + " \r\n users not found in db: " + notFoundUsersCount
						+ " \r\n Not eligible Users : " + notEligibleUsersCount + " \r\n Found user count: "
						+ foundUsersCount + " \r\n Found and enabled user removed from output: " + deltedusersCount);

				if (!Files.exists(Paths.get(outputCsvFilePath))) {
					System.out.println(" Output file does not exists creating new file.");
					Files.createFile(Paths.get(outputCsvFilePath));
				}
				System.out.println("writing output to the output file.");

				try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputCsvFilePath));

						CSVPrinter csvPrinter = new CSVPrinter(writer, format)) {
					// Print the header record
					csvPrinter.printRecord("CLIENT_ID", "CLIENT_NAME", "FIRST_NAME", "LAST_NAME", "Reason_for_Failure",
							"Last_Elig_Load", "Latest_Elig_File_date", "Failed_date","");

					// Print the updated records
					for (String[] record : updatedRecords) {
						csvPrinter.printRecord((Object[]) record);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		/* 186 */ catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			System.out.println(e.getStackTrace().toString());
			/* 188 */ this.conn.rollback();
			/* 189 */ throw e;
		} finally {

			this.conn.close();
		}
	}

	private String getFileName(PreparedStatement fileNamePrtstmt, String clientId) throws SQLException {
		fileNamePrtstmt.setString(1, clientId);
		ResultSet fileNameRs = fileNamePrtstmt.executeQuery();
		String fileName = null;
		while (fileNameRs.next()) {
			fileName = fileNameRs.getString("FILE_NM");
			break;
		}
		return fileName;
	}

	private String[] getFileStatusAndDate(PreparedStatement fileStatusPrtstmt, String fileName) throws SQLException {

		if (fileName != null) {
			String fileStatus = null;
			String fileDate = null;
			fileStatusPrtstmt.setString(1, fileName);
			System.out.println("status query executed"+fileName);
			ResultSet fileStatusRs = fileStatusPrtstmt.executeQuery();
			while (fileStatusRs.next()) {
				fileStatus = fileStatusRs.getString("PROCESS_STATUS");
				SimpleDateFormat dformat = new SimpleDateFormat("MM-dd-yyyy");
				if(fileStatus.equalsIgnoreCase("PROCESS_COMPLETED")) {
					fileDate = dformat.format(fileStatusRs.getTimestamp("PROCESS_END_TIME"));
					fileStatus = fileDate;
				}else {
					fileDate = dformat.format(fileStatusRs.getTimestamp("FILE_TIMESTAMP"));
					fileStatus = fileDate;
				}
				break;
			}
			return new String[] { fileStatus, fileDate };
		} else {
			return null;
		}
	}

	private String[] getProcessStatusAndDate(PreparedStatement fileNamePrtstmt, PreparedStatement fileStatusPrtstmt,
			String clientId) throws SQLException {
		String fileName = null;
		if(this.fileNameMap.get(clientId) !=null) {
			fileName =  this.fileNameMap.get(clientId);
		}else {
			fileName  = this.getFileName(fileNamePrtstmt, clientId);
			this.fileNameMap.put(clientId, fileName);
		}
		
		if(this.statusAndDate.get(fileName) != null) {
			return this.statusAndDate.get(fileName);
		}else {
			String[] returnValue =  this.getFileStatusAndDate(fileStatusPrtstmt, fileName);
			this.statusAndDate.put(fileName, returnValue);
			return returnValue;
		}
		
	}

	private OracleDataSource getDataSource(String datasourceUrl, String userName, String password) throws SQLException {
		/* 484 */ System.out.println("Inside  getDataSource()\n");
///* 485 */     Context ctx = null;
///* 486 */     Hashtable<Object, Object> ht = new Hashtable<>();
///* 487 */     ht.put("java.naming.factory.initial", contextFactory);
///* 488 */     ht.put("java.naming.provider.url", providerURL);
		/* 489 */ OracleDataSource dataSource = new OracleDataSource();
		try {

			dataSource.setURL(datasourceUrl);
			dataSource.setUser(userName);
			dataSource.setPassword(password);
			/* 493 */ } catch (Exception e) {
			/* 494 */ e.printStackTrace();
		}
		/* 496 */ return dataSource;
	}
}
