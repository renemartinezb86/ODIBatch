package mapping.ib;

import main.tde.java.Common;

import java.io.*;

import java.sql.*;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by iskael on 25-04-17.
 */
public class SimController {

    private Common common;
    BufferedWriter out = null;
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    String eocOrderXMLPath = "";
    String xSalesPath = "";

    private Connection connection = null;

    private Connection getConnection() throws Exception {
        if (connection == null || connection.isClosed()) {
            Class.forName("oracle.jdbc.OracleDriver");
            connection = DriverManager.getConnection(connectionURI, user, pass);
        }
        return connection;
    }

    public SimController() {
        super();

    }

    public String eocOrderToSimPre() {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/sim.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\sim.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                eocOrderXMLPath = prop.getProperty("eocOrderXMLPath");
                xSalesPath = prop.getProperty("xSalesPath");
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
        String fileToWrite = "IB_CAMBIO_SIMCARDS_PREPAGO_".concat(sdfForFile.format(actualDate).concat(".csv"));

        FileWriter fw = null;
        String line = "";

        try {
            fw = new FileWriter(xSalesPath + fileToWrite, true);
            out = new BufferedWriter(fw);

            List<String[]> params = common.readSimPreFile(eocOrderXMLPath);
            String header =
                "C�digo de la Orden de Activaci�n;C�digo del Contrato;Codigo Sucursal;RUT/RUC C�digo del Personal;C�digo del Cliente;Nombre del Cliente;Ciudad de Domic�lio del Cliente;MSISDN;iMSI;Fecha Cambio SIM Card";
            writeLine(header);
            for (String[] param : params) {
                for (int i = 0; i < param.length; i++) {
                    line += param[i] + ";";
                }
                if (!line.isEmpty()) {
                    line = line.substring(0, line.length() - 1);
                }
                writeLine(line);
                line = "";
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null)
                    out.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    private void writeLine(String line) throws Exception {
        out.write(line);
        out.newLine();
    }
}
