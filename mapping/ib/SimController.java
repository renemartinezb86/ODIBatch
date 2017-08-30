package mapping.ib;

import main.tde.java.Common;

import java.io.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\sim.properties";
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

        String line = "";
        BufferedWriter bw = null;

        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xSalesPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));

            List<String[]> params = common.readSimPreFile(eocOrderXMLPath);
            String header =
                "Código de la Orden de Activación;Código del Contrato;Codigo Sucursal;RUT/RUC Código del Personal;Código del Cliente;Nombre del Cliente;Ciudad de Domicílio del Cliente;MSISDN;iMSI;Fecha Cambio SIM Card";
            bw.write(header);
            bw.newLine();
            for (String[] param : params) {
                for (int i = 0; i < param.length; i++) {
                    line += param[i] + ";";
                }
                if (!line.isEmpty()) {
                    line = line.substring(0, line.length() - 1);
                }
                bw.write(line);
                bw.newLine();
                line = "";
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
        return result;
    }
}
