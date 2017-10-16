package mapping.ib;

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

import java.nio.charset.StandardCharsets;

import java.sql.*;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import main.tde.java.Common;

public class ContactController {
    private static String ibPosPath = "D:\\Work\\ODI\\result\\";
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    private String country = "CH";
    private String queryContactados = "";
    private String queryTotal = "";
    private Common common;

    public ContactController() {
        super();
        common = new Common();
    }

    public String mapPosToIB() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/cont.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\cont.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                ibPosPath = prop.getProperty("ibPosPath");
                country = prop.getProperty("country");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                //queryContactados = prop.getProperty("queryContactados");
                queryTotal = prop.getProperty("queryTotal");
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
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat sdfForQuery = new SimpleDateFormat("dd-MM-yyyy");
        String fileToWrite = "IB_KPI_SIEBEL_".concat(sdfForFile.format(actualDate).concat(".csv"));

        Connection connection = null;
        BufferedWriter bw = null;
        String line = "";
        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ibPosPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));
            line = "Tipo;Código;Fecha;Nombre KPI;Valor KPI";
            line += "\n";
            bw.write(line);

            try {
                connection = DriverManager.getConnection(connectionURI, user, pass);
                Statement stmt = null;
                stmt = connection.createStatement();
                /*String finalQuery =
                    String.format(queryTotal, sdfForQuery.format(actualDate), sdfForQuery.format(actualDate),
                                  sdfForQuery.format(actualDate), sdfForQuery.format(actualDate));*/
                ResultSet rs = stmt.executeQuery(queryTotal);
                while (rs.next()) {
                    String tipo = "Single";
                    String codigo = "codigo";
                    if (hasColumn(rs, "codigo") && rs.getString("codigo") != null) {
                        codigo = rs.getString("codigo");
                    }
                    String fecha = sdfForFile.format(actualDate);
                    String nombre = "CONTACTIBILIDAD SOBRE LA BASE";
                    int contactados = 0;
                    if (hasColumn(rs, "contactados")) {
                        contactados = rs.getInt("contactados");
                    }
                    int total = 0;
                    if (hasColumn(rs, "total")) {
                        total = rs.getInt("total");
                    }
                    //Tipo;Código;Fecha;Nombre KPI;Valor KPI
                    line = tipo + ";";
                    line += codigo + ";";
                    line += fecha + ";";
                    line += nombre + ";";
                    line += contactados / total * 100 + "";
                    line += "\n";
                    bw.write(line);
                }
            } catch (SQLException e) {
                System.out.println("Connection Failed! Check output console");
                e.printStackTrace();
                return "ERROR";
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
            } catch (IOException ex) {

                ex.printStackTrace();
            }
        }
        if (connection != null) {
        } else {
            System.out.println("Failed to make connection!");
        }
        return result;
    }

    public static boolean hasColumn(ResultSet rs, String columnName) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();
        for (int x = 1; x <= columns; x++) {
            if (columnName.equals(rsmd.getColumnName(x))) {
                return true;
            }
        }
        return false;
    }
}

