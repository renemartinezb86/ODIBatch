package mapping.ib;

import main.tde.java.Common;

import java.io.*;

import java.nio.charset.StandardCharsets;

import java.sql.*;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by iskael on 25-04-17.
 */
public class EquiposController {

    private Common common;
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    private String POSconnectionURI = "jdbc:oracle:thin:@10.51.14.72:1591/XCENTPD1.tdeprdcl.internal";
    private String POSuser = "CONSULTA";
    private String POSpass = "consulta";
    private String POSquery = "";
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

    public EquiposController() {
        super();

    }

    public String eocOrderToEquiposPre() {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/equipos.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\equipos.properties";
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
        String fileToWrite = "IB_VENTAS_EQUIPOS_PREPAGO_".concat(sdfForFile.format(actualDate).concat(".csv"));
        String line = "";

        BufferedWriter bw = null;
        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xSalesPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));

            List<String[]> params = common.readEquiposPreFile(eocOrderXMLPath);
            String header =
                "Código de la Venta;Código del Contrato;Codigo Sucursal;RUT/RUC Código del Personal;Código del Cliente;Nombre del Cliente;Ciudad de Domicílio del Cliente;MSISDN;IMEI;Fecha de la Venta;Product Offer;Precio";
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

    public String posDBToEquiposPre() {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/equipos.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\equipos.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                xSalesPath = prop.getProperty("POSxSalesPath");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                POSconnectionURI = prop.getProperty("POSconnectionURI");
                POSuser = prop.getProperty("POSuser");
                POSpass = prop.getProperty("POSpass");
                POSquery = prop.getProperty("POSquery");
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
        String fileToWrite = "IB_VENTAS_EQUIPOS_PREPAGO_".concat(sdfForFile.format(actualDate).concat(".OP.csv"));
        String line = "";
        BufferedWriter bw = null;

        /*POSquery =
            "SELECT\n" + "  AGENCIA || CAJA || TICKET || FOLIO || '-' || ROW_NUMBER()\n" + "  OVER (\n" +
            "    PARTITION BY FOLIO, FECHA\n" + "    ORDER BY FECHA ) AS COD_VENTA,\n" +
            "  AGENCIA            AS COD_SURUSAL,\n" + "  VENDEDOR,\n" + "  CIUDAD,\n" + "  SERIAL_NBR,\n" +
            "  ITEM_ID,\n" + "  FECHA,\n" + "  MONTO\n" + "FROM (SELECT\n" + "        nvl((SELECT p2.string_value\n" +
            "             FROM dtv.trn_trans_p p2\n" +
            "             WHERE (p2.rtl_loc_id || p2.business_date || p2.wkstn_id || p2.trans_seq) =\n" +
            "                   (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" +
            "                   AND p2.property_code IN ('CompanyId')), 'PCS')     AS EMPRESA,\n" +
            "        trn.rtl_loc_id                                                AS AGENCIA,\n" + "\n" +
            "        (SELECT employee_id\n" + "         FROM dtv.CRM_PARTY\n" +
            "         WHERE PARTY_ID = trn.OPERATOR_PARTY_ID AND rownum < 2)       AS VENDEDOR,\n" +
            "        trn.wkstn_id                                                  AS CAJA,\n" +
            "        trn.trans_seq                                                 AS TICKET,\n" +
            "        TO_CHAR(trn.end_datetime, 'YYYYMMDDHH24MISS')                 AS FECHA,\n" + "\n" + "\n" +
            "        ttr.item_id                                                   AS ITEM_ID,\n" +
            "        ttr.unit_price - nvl(ttr.vat_amt, ttr.unit_price * 19 / 100)  AS MONTO,\n" +
            "        --En caso de no estar persistido el TAX se usa el 19%\n" +
            "        p.string_value                                                AS TIPO_DOC,\n" +
            "        itm.description                                               AS DESCRIPTION,\n" +
            "        ttr.serial_nbr                                                AS SERIAL_NBR,\n" +
            "        nvl((SELECT p2.string_value\n" + "             FROM dtv.trn_trans_p p2\n" +
            "             WHERE (p2.rtl_loc_id || p2.business_date || p2.wkstn_id || p2.trans_seq) =\n" +
            "                   (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" +
            "                   AND p2.property_code IN ('FOLIO')), '')            AS FOLIO,\n" +
            "        (SELECT crml.city\n" + "         FROM dtv.crm_party_locale_information crml\n" +
            "         WHERE crml.party_id =\n" + "               (SELECT party_id\n" +
            "                FROM dtv.crm_party crm\n" +
            "                WHERE p3.string_value = crm.CUST_ID) AND ROWNUM <= 1) AS CIUDAD\n" + "      FROM\n" +
            "        dtv.trn_trans trn\n" +
            "        INNER JOIN dtv.trl_sale_lineitm ttr ON (ttr.rtl_loc_id || ttr.business_date || ttr.wkstn_id || ttr.trans_seq) =\n" +
            "                                               (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" +
            "        INNER JOIN dtv.trl_rtrans_lineitm trl\n" +
            "          ON (trl.rtl_loc_id || trl.business_date || trl.wkstn_id || trl.trans_seq || trl.rtrans_lineitm_seq) =\n" +
            "             (ttr.rtl_loc_id || ttr.business_date || ttr.wkstn_id || ttr.trans_seq || ttr.rtrans_lineitm_seq)\n" +
            "        INNER JOIN dtv.trn_trans_p p ON (p.rtl_loc_id || p.business_date || p.wkstn_id || p.trans_seq) =\n" +
            "                                        (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" +
            "        INNER JOIN dtv.trn_trans_p p3 ON (p3.rtl_loc_id || p3.business_date || p3.wkstn_id || p3.trans_seq) =\n" +
            "                                         (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" +
            "        INNER JOIN dtv.itm_item itm ON (ttr.item_id = itm.item_id)\n" + "      WHERE\n" +
            "        itm.item_typcode = 'STANDARD'\n" + "        AND trn.trans_typcode = 'RETAIL_SALE'\n" +
            "        AND trn.trans_statcode = 'COMPLETE'\n" + "        AND trl.void_flag = 0\n" +
            "        AND trl.rtrans_lineitm_typcode = 'ITEM'\n" + "        AND itm.not_inventoried_flag = 0\n" +
            "        AND itm.merch_level_4 IN ('3') --equipos\n" + "        AND itm.merch_level_3 = '2'\n" +
            "        AND p.property_code IN ('TRANSACTION_DOC_TYPE', 'DocumentType')\n" +
            "        AND p.string_value NOT IN ('NOTA_CREDITO_TERCERO', 'NOTA_CREDITO')\n" +
            "        AND p3.property_code = 'CUSTOMER_RUT'\n" +
            "        AND trn.business_date = TO_CHAR(SYSDATE, 'DD-MON-YY')\n" +
            "  --  and trn.end_datetime > TO_CHAR (SYSDATE-20, 'DD-MON-YY')\n" +
            "  --  and trn.end_datetime < TO_CHAR (SYSDATE, 'DD-MON-YY')\n" + ")";*/

        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xSalesPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));
            List<String[]> params = common.readPOSEquiposPreFile(POSconnectionURI, POSuser, POSpass, POSquery);
            String header =
                "Código de la Venta;Código del Contrato;Codigo Sucursal;RUT/RUC Código del Personal;Código del Cliente;Nombre del Cliente;Ciudad de Domicílio del Cliente;MSISDN;IMEI;Fecha de la Venta;Product Offer;Precio";
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
