package mapping.ib;

import java.io.BufferedWriter;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.sql.Connection;
import java.sql.DriverManager;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import main.tde.java.Common;

public class ActivePreController {

    private Common common;
    BufferedWriter out = null;
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    String odiInputPath = "";
    String odiOutputPath = "";

    private Connection connection = null;

    private Connection getConnection() throws Exception {
        if (connection == null || connection.isClosed()) {
            Class.forName("oracle.jdbc.OracleDriver");
            connection = DriverManager.getConnection(connectionURI, user, pass);
        }
        return connection;
    }

    public ActivePreController() {
        super();
    }

    public String eocOrderToActivePre() {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/actpre.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\actpre.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                odiInputPath = prop.getProperty("odiInputPath");
                odiOutputPath = prop.getProperty("odiOutputPath");
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
        String fileToWrite = "IB_ACTV_PREPAGO_".concat(sdfForFile.format(actualDate).concat(".csv"));

        FileWriter fw = null;
        String line = "";

        try {
            fw = new FileWriter(odiOutputPath + fileToWrite, true);
            out = new BufferedWriter(fw);

            List<String[]> params = common.readActivePreFile(odiInputPath);
            String header =
                "Codigo de la Venta;Codigo del Contrato;Codigo Sucursal;RUT/RUC Codigo del Personal;Codigo del Cliente;Tipo de documento;Tipo RUC;Nombre del Cliente;Ciudad de Domicilio del Cliente;MSISDN;IMEI;IMSI;CustomerSince;Portabilidad;Operador Origen;Plan Origen Portabilidad;Product Offer;Fecha de la Venta;Fecha de Activacion";
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
