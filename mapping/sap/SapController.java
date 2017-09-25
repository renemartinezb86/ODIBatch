package mapping.sap;

import main.tde.java.Common;

import java.io.*;

import java.sql.*;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by iskael on 25-04-17.
 */
public class SapController {

    private Common common;
    BufferedWriter out = null;
    BufferedWriter errorOut = null;
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.89:1530/ESBU01.TDENOPCL.INTERNAL";
    private String user = "system";
    private String pass = "3ricss0n";
    String failXMLPath = "";
    String sapFilePath = "";
    String sapFileOk = "";
    String sapFileRecover = "";


    private Connection connection = null;

    private Connection getConnection() throws Exception {
        if (connection == null || connection.isClosed()) {
            Class.forName("oracle.jdbc.OracleDriver");
            connection = DriverManager.getConnection(connectionURI, user, pass);
        }
        return connection;
    }

    public SapController() {
        super();
    }

    public Boolean isOnOSB(String id) {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/sap.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\sap.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        common = new Common(connectionURI, user, pass);
        String osbQuery =
            "SELECT\n" + "       SYS_S.CODE SOURCE_SYSTEM_CODE,\n" + "       ESB_MAPPING.SOURCE_CODE,\n" +
            "       ESB_MAPPING.CONTEXT,\n" + "       SYS_D.CODE DESTINATION_SYSTEM_CODE,\n" +
            "       ESB_MAPPING.DESTINATION_CODE,\n" + "       ESB_CDM_FIELD.NAME FIELD_NAME,\n" +
            "       ESB_CDM_ENTITY.NAME ENTITY_NAME\n" + "FROM ESB_MAPPING\n" +
            "INNER JOIN ESB_CDM_FIELD    ON ESB_MAPPING.FIELD_ID           = ESB_CDM_FIELD.ID\n" +
            "INNER JOIN ESB_CDM_ENTITY   ON ESB_CDM_FIELD.ENTITY_ID        = ESB_CDM_ENTITY.ID\n" +
            "INNER JOIN ESB_SYSTEM SYS_S ON ESB_MAPPING.SOURCE_SYSTEM      = SYS_S.ID\n" +
            "INNER JOIN ESB_SYSTEM SYS_D ON ESB_MAPPING.DESTINATION_SYSTEM = SYS_D.ID\n" +
            "WHERE ESB_MAPPING.CONTEXT = 'BIM_0001@Create'\n" + "and ESB_CDM_FIELD.NAME = 'productId'\n" +
            "and ESB_MAPPING.SOURCE_CODE = '%s' --código accesorio informado por SAP";

        Statement stmt = null;
        try {
            stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            String finalQuery = String.format(osbQuery, id);
            ResultSet rs = stmt.executeQuery(finalQuery);
            rs.last();
            if (rs.getRow() > 0) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;

    }

    public String checkFailure() {
        List<String> processedFiles = new ArrayList<String>();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/sap.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\sap.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                failXMLPath = prop.getProperty("failXMLPath");
                sapFilePath = prop.getProperty("sapFilePath");
                sapFileOk = prop.getProperty("sapFileOk");
                sapFileRecover = prop.getProperty("sapFileRecover");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        common = new Common(connectionURI, user, pass);

        String result = "";
        Date actualDate = new Date();
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMddHHmmss");
        String fileToWrite = "SAP_OK_".concat(sdfForFile.format(actualDate).concat(".txt"));

        FileWriter fw = null;
        String line = "";

        String errorFileToWrite = "SAP_ERROR_".concat(sdfForFile.format(actualDate).concat(".txt"));

        FileWriter errorFw = null;
        String errorLine = "";

        try {
            List<String[]> params = common.readSapFile(failXMLPath);

            File folder = new File(failXMLPath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File file : files) {
                    processedFiles.add(file.getName());
                }
            }

            boolean firstOk = true;
            boolean firstError = true;

            String osbQuery =
                "SELECT\n" + "       SYS_S.CODE SOURCE_SYSTEM_CODE,\n" + "       ESB_MAPPING.SOURCE_CODE,\n" +
                "       ESB_MAPPING.CONTEXT,\n" + "       SYS_D.CODE DESTINATION_SYSTEM_CODE,\n" +
                "       ESB_MAPPING.DESTINATION_CODE,\n" + "       ESB_CDM_FIELD.NAME FIELD_NAME,\n" +
                "       ESB_CDM_ENTITY.NAME ENTITY_NAME\n" + "FROM ESB_MAPPING\n" +
                "INNER JOIN ESB_CDM_FIELD    ON ESB_MAPPING.FIELD_ID           = ESB_CDM_FIELD.ID\n" +
                "INNER JOIN ESB_CDM_ENTITY   ON ESB_CDM_FIELD.ENTITY_ID        = ESB_CDM_ENTITY.ID\n" +
                "INNER JOIN ESB_SYSTEM SYS_S ON ESB_MAPPING.SOURCE_SYSTEM      = SYS_S.ID\n" +
                "INNER JOIN ESB_SYSTEM SYS_D ON ESB_MAPPING.DESTINATION_SYSTEM = SYS_D.ID\n" +
                "WHERE ESB_MAPPING.CONTEXT = 'BIM_0001@Create'\n" + "and ESB_CDM_FIELD.NAME = 'productId'\n" +
                "and ESB_MAPPING.SOURCE_CODE = '%s' --código accesorio informado por SAP";

            String header = "CODIGO|DESCRIPCION|DEPARTAMENTO|CLASE|SUBCLASE|CREACION|ACTUALIZACION|BORRADO";
            for (String[] param : params) {
                Statement stmt = null;
                stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                String finalQuery = String.format(osbQuery, param[0]);
                //ResultSet rsd = stmt.executeQuery("ALTER SESSION SET CURRENT_SCHEMA = TDEFMWUAT01");
                ResultSet rs = stmt.executeQuery(finalQuery);
                rs.last();
                if (rs.getRow() > 0) {
                    if (firstOk) {
                        fw = new FileWriter(sapFileRecover + fileToWrite, true);
                        out = new BufferedWriter(fw);
                        writeLine(header);
                        firstOk = false;
                    }
                    for (int i = 0; i < param.length; i++) {
                        line += param[i] + "|";
                    }
                    if (!line.isEmpty()) {
                        line = line.substring(0, line.length() - 1);
                    }
                    writeLine(line);
                    line = "";
                } else {
                    if (firstError) {
                        errorFw = new FileWriter(failXMLPath + errorFileToWrite, true);
                        errorOut = new BufferedWriter(errorFw);
                        writeErrorLine(header);
                        firstError = false;
                    }
                    for (int i = 0; i < param.length; i++) {
                        errorLine += param[i] + "|";
                    }
                    if (!errorLine.isEmpty()) {
                        errorLine = errorLine.substring(0, errorLine.length() - 1);
                    }
                    writeErrorLine(errorLine);
                    errorLine = "";
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null)
                    out.close();
                if (fw != null)
                    fw.close();

                if (errorOut != null)
                    errorOut.close();
                if (errorFw != null)
                    errorFw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        for (String fileName : processedFiles) {
            File file = new File(failXMLPath + fileName);
            if (file.getName().contains("txt")) {
                //file.renameTo(new File(sapFileOk + fileName));
                if (file.delete()) {
                    System.out.println("Deleting: " + failXMLPath + fileName);
                } else {
                    System.out.println("Error quen deleting: " + failXMLPath + fileName);
                }
            }
        }
        return result;
    }

    public String processFailure() {
        List<String> processedFiles = new ArrayList<String>();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/sap.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\sap.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                failXMLPath = prop.getProperty("failXMLPath");
                sapFilePath = prop.getProperty("sapFilePath");
                sapFileOk = prop.getProperty("sapFileOk");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        common = new Common(connectionURI, user, pass);

        String result = "";
        Date actualDate = new Date();
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMddHHmmss");
        String fileToWrite = "SAP_OK_".concat(sdfForFile.format(actualDate).concat(".txt"));

        FileWriter fw = null;
        String line = "";

        String errorFileToWrite = "SAP_ERROR_".concat(sdfForFile.format(actualDate).concat(".txt"));

        FileWriter errorFw = null;
        String errorLine = "";

        try {
            List<String[]> params = common.readSapFile(sapFilePath);

            File folder = new File(sapFilePath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File file : files) {
                    processedFiles.add(file.getName());
                }
            }

            String osbQuery =
                "SELECT\n" + "       SYS_S.CODE SOURCE_SYSTEM_CODE,\n" + "       ESB_MAPPING.SOURCE_CODE,\n" +
                "       ESB_MAPPING.CONTEXT,\n" + "       SYS_D.CODE DESTINATION_SYSTEM_CODE,\n" +
                "       ESB_MAPPING.DESTINATION_CODE,\n" + "       ESB_CDM_FIELD.NAME FIELD_NAME,\n" +
                "       ESB_CDM_ENTITY.NAME ENTITY_NAME\n" + "FROM ESB_MAPPING\n" +
                "INNER JOIN ESB_CDM_FIELD    ON ESB_MAPPING.FIELD_ID           = ESB_CDM_FIELD.ID\n" +
                "INNER JOIN ESB_CDM_ENTITY   ON ESB_CDM_FIELD.ENTITY_ID        = ESB_CDM_ENTITY.ID\n" +
                "INNER JOIN ESB_SYSTEM SYS_S ON ESB_MAPPING.SOURCE_SYSTEM      = SYS_S.ID\n" +
                "INNER JOIN ESB_SYSTEM SYS_D ON ESB_MAPPING.DESTINATION_SYSTEM = SYS_D.ID\n" +
                "WHERE ESB_MAPPING.CONTEXT = 'BIM_0001@Create'\n" + "and ESB_CDM_FIELD.NAME = 'productId'\n" +
                "and ESB_MAPPING.SOURCE_CODE = '%s' --código accesorio informado por SAP";

            String header = "CODIGO|DESCRIPCION|DEPARTAMENTO|CLASE|SUBCLASE|CREACION|ACTUALIZACION|BORRADO";


            boolean firstOk = true;
            boolean firstError = true;
            for (String[] param : params) {
                Statement stmt = null;
                stmt = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                String finalQuery = String.format(osbQuery, param[0]);
                //ResultSet rsd = stmt.executeQuery("ALTER SESSION SET CURRENT_SCHEMA = TDEFMWUAT01");
                ResultSet rs = stmt.executeQuery(finalQuery);
                rs.last();
                if (rs.getRow() > 0) {
                    if (firstOk) {
                        fw = new FileWriter(sapFilePath + fileToWrite, true);
                        out = new BufferedWriter(fw);
                        writeLine(header);
                        firstOk = false;
                    }
                    for (int i = 0; i < param.length; i++) {
                        line += param[i] + "|";
                    }
                    if (!line.isEmpty()) {
                        line = line.substring(0, line.length() - 1);
                    }
                    writeLine(line);
                    line = "";
                } else {
                    if (firstError) {
                        errorFw = new FileWriter(failXMLPath + errorFileToWrite, true);
                        errorOut = new BufferedWriter(errorFw);
                        writeErrorLine(header);
                        firstError = false;
                    }
                    for (int i = 0; i < param.length; i++) {
                        errorLine += param[i] + "|";
                    }
                    if (!errorLine.isEmpty()) {
                        errorLine = errorLine.substring(0, errorLine.length() - 1);
                    }
                    writeErrorLine(errorLine);
                    errorLine = "";
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null)
                    out.close();
                if (fw != null)
                    fw.close();

                if (errorOut != null)
                    errorOut.close();
                if (errorFw != null)
                    errorFw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }


        for (String fileName : processedFiles) {
            File file = new File(sapFilePath + fileName);
            if (file.getName().contains("txt")) {
                file.renameTo(new File(sapFileOk + file.getName()));
            }
        }
        return result;
    }

    private void writeLine(String line) throws Exception {
        out.write(line);
        out.newLine();
    }

    private void writeErrorLine(String line) throws Exception {
        errorOut.write(line);
        errorOut.newLine();
    }
}
