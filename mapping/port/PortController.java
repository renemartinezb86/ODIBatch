package mapping.port;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.*;

import java.sql.*;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import main.tde.java.Common;

public class PortController {
    private static String acpnPortInPath = "D:\\Work\\ODI\\result\\";
    private static String acpnPortOutPath = "D:\\Work\\ODI\\result\\";
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    private String country = "CH";
    private String queryIn = "";
    private String queryOut = "";
    private Common common;

    public PortController() {
        super();
        common = new Common();
    }

    public String mapEocToPortIn() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/port.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\port.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                acpnPortInPath = prop.getProperty("acpnPortInPath");
                country = prop.getProperty("country");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                queryIn = prop.getProperty("queryIn");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        try {

            Class.forName("oracle.jdbc.OracleDriver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return "ERROR";
        }

        java.util.Date actualDate = new java.util.Date();
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMdd");
        String fileToWrite = "Altas_PP_FullStack_".concat(sdfForFile.format(actualDate).concat(".txt"));

        Connection connection = null;
        BufferedWriter bw = null;
        FileWriter fw = null;
        List<String> lines = new ArrayList<String>();
        String header = "";
        int count = 0;
        try {
            fw = new FileWriter(acpnPortInPath + fileToWrite);
            bw = new BufferedWriter(fw);

            try {
                connection = DriverManager.getConnection(connectionURI, user, pass);
                Statement stmt = null;
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(queryIn);
                String line = "";
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rs.next()) {
                    count++;
                    line = "";
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i == 1) {
                            if (rs.getObject(i) != null) {
                                line += rs.getObject(i).toString();
                            }
                        } else {
                            if (rs.getObject(i) != null) {
                                line += ";" + rs.getObject(i).toString();
                            }
                        }
                    }
                    line += "\n";
                    lines.add(line);
                }
                if (count > 0) {
                    header = (String.format("%012d", count));
                    header += "\n";
                    bw.write(header);
                }
                for (String l : lines) {
                    bw.write(l);
                }
            } catch (SQLException e) {

                e.printStackTrace();
                return "ERROR";
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {

                ex.printStackTrace();
            }
        }

        if (connection != null) {
        } else {
        }
        return result;
    }


    public String mapEocToPortOut() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/port.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\port.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                acpnPortInPath = prop.getProperty("acpnPortOutPath");
                country = prop.getProperty("country");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                queryOut = prop.getProperty("queryOut");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        try {

            Class.forName("oracle.jdbc.OracleDriver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return "ERROR";
        }

        java.util.Date actualDate = new java.util.Date();
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMdd");
        String fileToWrite = "Bajas_PP_FullStack_".concat(sdfForFile.format(actualDate).concat(".txt"));

        Connection connection = null;
        BufferedWriter bw = null;
        FileWriter fw = null;
        List<String> lines = new ArrayList<String>();
        String header = "";
        int count = 0;
        try {
            fw = new FileWriter(acpnPortInPath + fileToWrite);
            bw = new BufferedWriter(fw);

            try {
                connection = DriverManager.getConnection(connectionURI, user, pass);
                Statement stmt = null;
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(queryOut);
                String line = "";
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                while (rs.next()) {
                    count++;
                    line = "";
                    for (int i = 1; i <= columnsNumber; i++) {
                        if (i == 1) {
                            if (rs.getObject(i) != null) {
                                line += rs.getObject(i).toString();
                            }
                        } else {
                            if (rs.getObject(i) != null) {
                                line += ";" + rs.getObject(i).toString();
                            }
                        }
                    }
                    line += "\n";
                    lines.add(line);
                }
                if (count > 0) {
                    header = (String.format("%012d", count));
                    header += "\n";
                    bw.write(header);
                }
                for (String l : lines) {
                    bw.write(l);
                }
            } catch (SQLException e) {

                e.printStackTrace();
                return "ERROR";
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {

                ex.printStackTrace();
            }
        }

        if (connection != null) {
        } else {
        }
        return result;
    }
}

