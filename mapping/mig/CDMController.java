package mapping.mig;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;

import java.io.IOException;

import java.io.InputStream;

import java.io.PrintWriter;
import java.io.StringWriter;

import java.sql.Connection;

import java.sql.DriverManager;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.Statement;

import java.text.SimpleDateFormat;

import java.util.Date;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import main.tde.java.Common;

public class CDMController {

    private Common common;

    public CDMController() {
        super();
        common = new Common();
    }

    public String mapDBtoFile() {
        long startTime = System.currentTimeMillis();
        String result = "";
        String connectionURI = "jdbc:oracle:thin:@10.49.4.89:1530:ESBU01";
        String user = "TDEESDATA01";
        String pass = "TDEESDATA01";

        Date actualDate = new Date();
        SimpleDateFormat sdftxt = new SimpleDateFormat("yyyyMMdd");
        String fileFormat = "txt";
        String fileToWrite = "TDE_EnMigracion_".concat(sdftxt.format(actualDate).concat(fileFormat));


        Properties prop = new Properties();
        String propFileName = "/u01/entel/jars/migration.properties";
        try {
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                fileToWrite = prop.getProperty("fileName");
                fileFormat = prop.getProperty("fileFormat");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        fileToWrite = fileToWrite.concat(sdftxt.format(actualDate).concat(fileFormat));

        Connection connection = null;
        Statement stmt = null;

        FileWriter fw = null;
        BufferedWriter bw = null;

        try {
            connection = DriverManager.getConnection(connectionURI, user, pass);
            String query =
                "SELECT BATCH_ID, RESOURCE_TYPE, RESOURCE_ID, DOCUMENT_TYPE, DOCUMENT_ID, SUBSCRIPTION_ID, CUSTOMER_ID, STATE, STATE_TIMESTAMP FROM ESB_MIGRATION_INFO";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            fw = new FileWriter(fileToWrite);
            bw = new BufferedWriter(fw);

            while (rs.next()) {
                if (rs.getString("BATCH_ID") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("BATCH_ID").concat("|"));
                }

                if (rs.getString("RESOURCE_TYPE") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("RESOURCE_TYPE").concat("|"));
                }

                if (rs.getString("RESOURCE_ID") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("RESOURCE_ID").concat("|"));
                }

                if (rs.getString("DOCUMENT_TYPE") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("DOCUMENT_TYPE").concat("|"));
                }

                if (rs.getString("DOCUMENT_ID") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("DOCUMENT_ID").concat("|"));
                }

                if (rs.getString("SUBSCRIPTION_ID") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("SUBSCRIPTION_ID").concat("|"));
                }

                if (rs.getString("CUSTOMER_ID") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("CUSTOMER_ID").concat("|"));
                }

                if (rs.getString("STATE") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("STATE").concat("|"));
                }

                if (rs.getString("STATE_TIMESTAMP") == null) {
                    bw.write("|");
                } else {
                    bw.write(rs.getString("STATE_TIMESTAMP"));
                }
                bw.write("\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.println(String.format("Tiempo de ejecución ETL_MigrationInfo_DB_to_CDM: %02d min, %02d sec",
                                             TimeUnit.MILLISECONDS.toMinutes(elapsedTime),
                                             TimeUnit.MILLISECONDS.toSeconds(elapsedTime) -
                                             TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(elapsedTime))));
        }

        return result;
    }
}
