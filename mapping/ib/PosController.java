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

public class PosController {
    private static String ibPosPath = "D:\\Work\\ODI\\result\\";
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    private String country = "CH";
    private Common common;

    public PosController() {
        super();
        common = new Common();
    }

    public String mapPosToIB() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/pos.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\pos.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                ibPosPath = prop.getProperty("ibPosPath");
                country = prop.getProperty("country");
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
        try {

            Class.forName("oracle.jdbc.OracleDriver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return "ERROR";
        }

        java.util.Date actualDate = new java.util.Date();
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMddHHmmss");
        String fileToWrite = "IB_PUNTOS_DE_VENTA_".concat(sdfForFile.format(actualDate).concat(".csv"));

        Connection connection = null;
        BufferedWriter bw = null;
        String line = "";
        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ibPosPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));
            line =
                "Codigo Sucursal;RUC/RUT Sucursal;Nombre Sucursal;Estado Sucursal;Ciudad Sucursal;Region Sucursal;Telefono Sucursal;E-mail Sucursal;Codigo Socio;RUC/RUT Socio;Nombre Socio;Canal Socio;Nivel Socio;Estado Socio;Telefono Socio;E-mail Socio";
            line += "\n";
            bw.write(line);

            try {
                connection = DriverManager.getConnection(connectionURI, user, pass);
                Statement stmt = null;
                String query =
                    "SELECT \n" + "  X.X_COD_STORE         Codigo_Sucursal, \n" +
                    "  NVL(S1.ATTRIB_38,'96806980-2')             RUT_Sucursal, -- Para socios, el RUT de la sucursal es el RUT del Socio y para tiendas propias (No tienen Socios) el RUT de Entel\n" +
                    "  X.NAME                Nombre_Sucursal, \n" + "  X.X_STORE_STATUS      Estado_Sucursal, \n" +
                    "  C.CITY                Ciudad_Sucursal,\n" + "  C.STATE               Region_Sucursal,\n" +
                    "  CON.PH_NUM            Telefono_Sucursal,\n" + "  CON.EMAIL_ADDR        Email_Sucursal,\n" +
                    "  S.X_COD_WAREHOUSE     Codigo_Socio,\n" + "  S1.ATTRIB_38          RUT_Socio,\n" +
                    "  S.NAME                Nombre_Socio,\n" +
                    "  X.X_CANALES        Canal_Socio, -- Canal de la Sucursal ya que el socio es dueña de esta sucursal.\n" +
                    "  T.TIER_CD             NIVEL_SOC,\n" + "  S.CUST_STAT_CD        Estado_Socio,\n" +
                    "  S.MAIN_PH_NUM         Telefono_Socio,\n" + "  S.MAIN_EMAIL_ADDR     Email_Socio\n" + "FROM\n" +
                    "  SIEBEL.S_ORG_EXT X,\n" + "  SIEBEL.S_ADDR_PER C,\n" + "  SIEBEL.S_CON_ADDR CON,\n" +
                    "  SIEBEL.S_ORG_EXT S,\n" + "  SIEBEL.S_ORG_EXT_X S1,\n" + "  SIEBEL.S_OU_PRTNR_TIER T\n" +
                    "WHERE\n" + "  X.INT_ORG_FLG = 'Y' AND X.X_STORE_INDICATOR = 'Y'\n" +
                    "  AND X.PR_ADDR_ID = C.ROW_ID (+) " +
                    "  AND con.ADDR_PER_ID (+)= c.row_id and con.ACCNT_ID (+)= x.row_id \n" +
                    "  AND X.X_STORE_PARTNER = s.par_row_id (+)\n" + "  and s.par_row_id = s1.row_id (+)\n" +
                    "  AND S.PR_PRTNR_TIER_ID = T.ROW_ID (+)";
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    String CODIGO_SUCURSAL = "T_NA";
                    if (hasColumn(rs, "CODIGO_SUCURSAL") && rs.getString("CODIGO_SUCURSAL") != null) {
                        CODIGO_SUCURSAL = rs.getString("CODIGO_SUCURSAL");
                    }
                    String RUT_SUCURSAL = "NA";
                    if (hasColumn(rs, "RUT_SUCURSAL") && rs.getString("RUT_SUCURSAL") != null) {
                        RUT_SUCURSAL = rs.getString("RUT_SUCURSAL");
                    }
                    String NOMBRE_SUCURSAL = "Sin Registro";
                    if (hasColumn(rs, "NOMBRE_SUCURSAL") && rs.getString("NOMBRE_SUCURSAL") != null) {
                        NOMBRE_SUCURSAL = rs.getString("NOMBRE_SUCURSAL");
                    }
                    String ESTADO_SUCURSAL = "Sin Registro";
                    if (hasColumn(rs, "ESTADO_SUCURSAL") && rs.getString("ESTADO_SUCURSAL") != null) {
                        ESTADO_SUCURSAL = rs.getString("ESTADO_SUCURSAL");
                    }
                    String CIUDAD_SUCURSAL = "Sin Registro";
                    if (hasColumn(rs, "CIUDAD_SUCURSAL") && rs.getString("CIUDAD_SUCURSAL") != null) {
                        CIUDAD_SUCURSAL = rs.getString("CIUDAD_SUCURSAL");
                    }
                    String REGION_SUCURSAL = "Sin Registro";
                    if (hasColumn(rs, "REGION_SUCURSAL") && rs.getString("REGION_SUCURSAL") != null) {
                        REGION_SUCURSAL = rs.getString("REGION_SUCURSAL");
                    }
                    String TELEFONO_SUCURSAL = "Sin Registro";
                    if (hasColumn(rs, "TELEFONO_SUCURSAL") && rs.getString("TELEFONO_SUCURSAL") != null) {
                        TELEFONO_SUCURSAL = rs.getString("TELEFONO_SUCURSAL");
                    }
                    String EMAIL_SUCURSAL = "Sin Registro";
                    if (hasColumn(rs, "EMAIL_SUCURSAL") && rs.getString("EMAIL_SUCURSAL") != null) {
                        EMAIL_SUCURSAL = rs.getString("EMAIL_SUCURSAL");
                    }
                    String CODIGO_SOCIO = "S_NA";
                    if (hasColumn(rs, "CODIGO_SOCIO") && rs.getString("CODIGO_SOCIO") != null) {
                        CODIGO_SOCIO = rs.getString("CODIGO_SOCIO");
                    }
                    String RUT_SOCIO = "NA";
                    if (hasColumn(rs, "RUT_SOCIO") && rs.getString("RUT_SOCIO") != null) {
                        RUT_SOCIO = rs.getString("RUT_SOCIO");
                    }
                    String NOMBRE_SOCIO = "Sin Registro";
                    if (hasColumn(rs, "NOMBRE_SOCIO") && rs.getString("NOMBRE_SOCIO") != null) {
                        NOMBRE_SOCIO = rs.getString("NOMBRE_SOCIO");
                    }
                    String CANAL_SOCIO = "NA";
                    if (hasColumn(rs, "CANAL_SOCIO") && rs.getString("CANAL_SOCIO") != null) {
                        CANAL_SOCIO = rs.getString("CANAL_SOCIO");
                    }
                    String NIVEL_SOC = "Sin Registro";
                    if (hasColumn(rs, "NIVEL_SOC") && rs.getString("NIVEL_SOC") != null) {
                        NIVEL_SOC = rs.getString("NIVEL_SOC");
                    }
                    String ESTADO_SOCIO = "NA";
                    if (hasColumn(rs, "ESTADO_SOCIO") && rs.getString("ESTADO_SOCIO") != null) {
                        ESTADO_SOCIO = rs.getString("ESTADO_SOCIO");
                    }
                    String TELEFONO_SOCIO = "Sin Registro";
                    if (hasColumn(rs, "TELEFONO_SOCIO") && rs.getString("TELEFONO_SOCIO") != null) {
                        TELEFONO_SOCIO = rs.getString("TELEFONO_SOCIO");
                    }
                    String EMAIL_SOCIO = "Sin Registro";
                    if (hasColumn(rs, "EMAIL_SOCIO") && rs.getString("EMAIL_SOCIO") != null) {
                        EMAIL_SOCIO = rs.getString("EMAIL_SOCIO");
                    }
                    String FLAG_SOC = "";
                    if (hasColumn(rs, "FLAG_SOC") && rs.getString("FLAG_SOC") != null) {
                        FLAG_SOC = rs.getString("FLAG_SOC");
                    }
                    //Codigo Sucursal;RUC/RUT Sucursal;Nombre Sucursal;Estado Sucursal;Ciudad Sucursal;Region Sucursal;Telefono Sucursal;E-mail Sucursal;Codigo Socio;RUC/RUT Socio;Nombre Socio;Canal Socio;Nivel Socio;Estado Socio;Telefono Socio;E-mail Socio
                    line = CODIGO_SUCURSAL + ";";
                    line += RUT_SUCURSAL + ";";
                    line += NOMBRE_SUCURSAL + ";";
                    if (ESTADO_SUCURSAL.length() > 10) {
                        line += ESTADO_SUCURSAL.substring(1, 10) + ";";
                    } else {
                        line += ESTADO_SUCURSAL + ";";
                    }
                    line += CIUDAD_SUCURSAL + ";";
                    line += REGION_SUCURSAL + ";";
                    line += TELEFONO_SUCURSAL + ";";
                    line += EMAIL_SUCURSAL + ";";
                    line += CODIGO_SOCIO + ";";
                    line += RUT_SOCIO + ";";
                    line += NOMBRE_SOCIO + ";";
                    line += CANAL_SOCIO + ";";
                    line += NIVEL_SOC + ";";
                    if (ESTADO_SOCIO.length() > 10) {
                        line += ESTADO_SOCIO.substring(1, 10) + ";";
                    } else {
                        line += ESTADO_SOCIO + ";";
                    }
                    line += TELEFONO_SOCIO + ";";
                    line += EMAIL_SOCIO + "";
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


    public String[] readSiebelData(String msisdn) {
        String[] array = new String[6];

        try {

            Class.forName("oracle.jdbc.OracleDriver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionURI, user, pass);
            Statement stmt = null;
            String query =
                "SELECT C.SOC_SECURITY_NUM DOC_EJEC_RESPONSABLE, EX.PAR_DUNS_NUM TIPO_DOCUMENTO_CLI, EX.NAME NOMBRE_CLI,P.CITY CIUDAD ,S.X_TDE_IMEI IMEI, " +
                "EX.CUST_SINCE_DT CUSTOMER_SINCE " +
                "from siebel.s_Asset S, siebel.s_ORG_EXT EX, siebel.S_ADDR_PER P, siebel.S_SRV_REQ R, SIEBEL.S_CONTACT C " +
                "where 1=1 " + "AND serial_num='" + msisdn + "' " + "AND EX.ROW_ID = S.OWNER_ACCNT_ID " +
                "AND R.ASSET_ID = S.ROW_ID " + "AND EX.PR_ADDR_ID = P.ROW_ID " + "AND R.OWNER_EMP_ID = C.ROW_ID " +
                "AND S.STATUS_CD ='Activo'";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String DOC_EJEC_RESPONSABLE = "";
                if (hasColumn(rs, "DOC_EJEC_RESPONSABLE") && rs.getString("DOC_EJEC_RESPONSABLE") != null) {
                    DOC_EJEC_RESPONSABLE = rs.getString("DOC_EJEC_RESPONSABLE");
                }
                String TIPO_DOCUMENTO_CLI = "";
                if (hasColumn(rs, "TIPO_DOCUMENTO_CLI") && rs.getString("TIPO_DOCUMENTO_CLI") != null) {
                    TIPO_DOCUMENTO_CLI = rs.getString("TIPO_DOCUMENTO_CLI");
                }
                String NOMBRE_CLI = "";
                if (hasColumn(rs, "NOMBRE_CLI") && rs.getString("NOMBRE_CLI") != null) {
                    NOMBRE_CLI = rs.getString("NOMBRE_CLI");
                }
                String CIUDAD = "";
                if (hasColumn(rs, "CIUDAD") && rs.getString("CIUDAD") != null) {
                    CIUDAD = rs.getString("CIUDAD");
                }
                String IMEI = "";
                if (hasColumn(rs, "IMEI") && rs.getString("IMEI") != null) {
                    IMEI = rs.getString("IMEI");
                }
                String CUSTOMER_SINCE = "";
                if (hasColumn(rs, "CUSTOMER_SINCE") && rs.getString("CUSTOMER_SINCE") != null) {
                    CUSTOMER_SINCE = rs.getString("CUSTOMER_SINCE");
                }
                array[0] = DOC_EJEC_RESPONSABLE;
                array[1] = TIPO_DOCUMENTO_CLI;
                array[2] = NOMBRE_CLI;
                array[3] = CIUDAD;
                array[4] = IMEI;
                array[5] = CUSTOMER_SINCE;

            }
        } catch (SQLException e) {

            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
        }

        if (connection != null) {
        } else {
            System.out.println("Failed to make connection!");
        }
        return array;
    }
}

