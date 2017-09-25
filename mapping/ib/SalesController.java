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
public class SalesController {

    private Common common;
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.105:1530/OCRMI01.tdenopcl.internal";
    private String user = "EAIUSER";
    private String pass = "EAIUSER";
    String eocOrderXMLPath = "";
    String xSalesPath = "";
    private String POSconnectionURI = "jdbc:oracle:thin:@10.51.14.72:1591/XCENTPD1.tdeprdcl.internal";
    private String POSuser = "CONSULTA";
    private String POSpass = "consulta";
    private String POSquery = "";

    private Connection connection = null;

    private Connection getConnection() throws Exception {
        if (connection == null || connection.isClosed()) {
            Class.forName("oracle.jdbc.OracleDriver");
            connection = DriverManager.getConnection(connectionURI, user, pass);
        }
        return connection;
    }

    public SalesController() {
        super();

    }

    public String eocOrderToVentaPre() {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/sales.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\sales.properties";
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
        String fileToWrite = "IB_VENTAS_PREPAGO_".concat(sdfForFile.format(actualDate).concat(".csv"));
        String line = "";
        BufferedWriter bw = null;
        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xSalesPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));

            List<String[]> params = common.readSalesPreFile(eocOrderXMLPath);
            String header =
                "C�digo de la Venta;C�digo del Contrato;Codigo Sucursal;RUT/RUC C�digo del Personal;C�digo del Cliente;Tipo de documento;Tipo RUC;Nombre del Cliente;Ciudad de Domic�lio del Cliente;MSISDN;IMEI;IMSI;CustomerSince;Portabilidad;Operador Origen;Plan Origen Portabilidad;Product Offer;Fecha de la Venta;Precio";
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

    public String posDBToSalesPre() {
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/sales.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\sales.properties";
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
        String fileToWrite = "IB_VENTAS_PREPAGO_".concat(sdfForFile.format(actualDate).concat(".OP.csv"));
        String line = "";
        BufferedWriter bw = null;
        
        /*POSquery = "SELECT\n" + 
        "  AGENCIA || CAJA || TICKET || FOLIO || '-' || ROW_NUMBER()\n" + 
        "  OVER (\n" + 
        "    PARTITION BY FOLIO, FECHA\n" + 
        "    ORDER BY FECHA ) AS COD_VENTA,\n" + 
        "  AGENCIA            AS COD_SURUSAL,\n" + 
        "  VENDEDOR,\n" + 
        "  CIUDAD,\n" + 
        "  SERIAL_NBR,\n" + 
        "  ICCID,\n" + 
        "  ITEM_ID,\n" + 
        "  FECHA,\n" + 
        "  MONTO\n" + 
        "FROM (SELECT\n" + 
        "        nvl((SELECT p2.string_value\n" + 
        "             FROM dtv.trn_trans_p p2\n" + 
        "             WHERE (p2.rtl_loc_id || p2.business_date || p2.wkstn_id || p2.trans_seq) =\n" + 
        "                   (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" + 
        "                   AND p2.property_code IN ('CompanyId')), 'PCS')     AS EMPRESA,\n" + 
        "        trn.rtl_loc_id                                                AS AGENCIA,\n" + 
        "\n" + 
        "        (SELECT employee_id\n" + 
        "         FROM dtv.CRM_PARTY\n" + 
        "         WHERE PARTY_ID = trn.OPERATOR_PARTY_ID AND rownum < 2)       AS VENDEDOR,\n" + 
        "        trn.wkstn_id                                                  AS CAJA,\n" + 
        "        trn.trans_seq                                                 AS TICKET,\n" + 
        "        TO_CHAR(trn.end_datetime, 'YYYYMMDDHH24MISS')                 AS FECHA,\n" + 
        "        nvl((SELECT p2.string_value\n" + 
        "             FROM dtv.trn_trans_p p2\n" + 
        "             WHERE (p2.rtl_loc_id || p2.business_date || p2.wkstn_id || p2.trans_seq) =\n" + 
        "                   (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" + 
        "                   AND p2.property_code IN ('OperationType')), '')    AS TRANSACCION,\n" + 
        "        ttr.item_id                                                   AS ITEM_ID,\n" + 
        "        ttr.unit_price - nvl(ttr.vat_amt, ttr.unit_price * 19 / 100)  AS MONTO,\n" + 
        "        --En caso de no estar persistido el TAX se usa el 19%\n" + 
        "        p.string_value                                                AS TIPO_DOC,\n" + 
        "        itm.description                                               AS DESCRIPTION,\n" + 
        "        CASE\n" + 
        "        WHEN itm.item_id LIKE '4%'\n" + 
        "          THEN ttr.serial_nbr\n" + 
        "        ELSE ''\n" + 
        "        END                                                           AS SERIAL_NBR,\n" + 
        "        nvl((SELECT p2.string_value\n" + 
        "             FROM dtv.trn_trans_p p2\n" + 
        "             WHERE (p2.rtl_loc_id || p2.business_date || p2.wkstn_id || p2.trans_seq) =\n" + 
        "                   (trn.rtl_loc_id || trn.business_date || trn.wkstn_id || trn.trans_seq)\n" + 
        "                   AND p2.property_code IN ('FOLIO')), '')            AS FOLIO,\n" + 
        "        nvl((SELECT p4.string_value\n" + 
        "             FROM dtv.TRL_RTRANS_LINEITM_P p4\n" + 
        "             WHERE (p4.rtl_loc_id || p4.business_date || p4.wkstn_id || p4.trans_seq || p4.rtrans_lineitm_seq) =\n" + 
        "                   (trl.rtl_loc_id || trl.business_date || trl.wkstn_id || trl.trans_seq || trl.rtrans_lineitm_seq)\n" + 
        "                   AND p4.property_code IN ('ICCID')), '')            AS ICCID,\n" + 
        "        (SELECT crml.city\n" + 
        "         FROM dtv.crm_party_locale_information crml\n" + 
        "         WHERE crml.party_id =\n" + 
        "               (SELECT party_id\n" + 
        "                FROM dtv.crm_party crm\n" + 
        "                WHERE p3.string_value = crm.CUST_ID) AND ROWNUM <= 1) AS CIUDAD\n" + 
        "      FROM\n" + 
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
        "        INNER JOIN dtv.itm_item itm ON (ttr.item_id = itm.item_id)\n" + 
        "      WHERE ttr.rtrans_lineitm_seq = trl.rtrans_lineitm_seq\n" + 
        "            AND ttr.item_id = itm.item_id\n" + 
        "            AND itm.item_typcode = 'STANDARD'\n" + 
        "            AND trn.trans_typcode = 'RETAIL_SALE'\n" + 
        "            AND trn.trans_statcode = 'COMPLETE'\n" + 
        "            AND p.property_code IN ('TRANSACTION_DOC_TYPE', 'DocumentType')\n" + 
        "            AND trl.void_flag = 0\n" + 
        "            AND trl.rtrans_lineitm_typcode = 'ITEM'\n" + 
        "            AND itm.not_inventoried_flag = 0\n" + 
        "            AND ((itm.merch_level_4 = '5'--SIMCARD\n" + 
        "                  AND itm.merch_level_3 = '2') OR\n" + 
        "                 (LENGTH(itm.item_id) = 6--PACK\n" + 
        "                  AND itm.item_id LIKE '4%')\n" + 
        "            )\n" + 
        "\n" + 
        "            AND p3.property_code = 'CUSTOMER_RUT'\n" + 
        "            AND p.string_value NOT IN ('NOTA_CREDITO_TERCERO', 'NOTA_CREDITO')\n" + 
        "            AND trn.business_date = TO_CHAR(SYSDATE, 'DD-MON-YY')\n" + 
        "  --and trn.end_datetime > TO_CHAR (SYSDATE-300, 'DD-MON-YY')\n" + 
        "  --and trn.end_datetime < TO_CHAR (SYSDATE, 'DD-MON-YY')\n" + 
        ")";*/
        
        try {
            bw =
                new BufferedWriter(new OutputStreamWriter(new FileOutputStream(xSalesPath + fileToWrite),
                                                          StandardCharsets.ISO_8859_1));
            List<String[]> params = common.readPOSSalesPreFile(POSconnectionURI, POSuser, POSpass, POSquery);
            String header =
                "C�digo de la Venta;C�digo del Contrato;Codigo Sucursal;RUT/RUC C�digo del Personal;C�digo del Cliente;Tipo de documento;Tipo RUC;Nombre del Cliente;Ciudad de Domic�lio del Cliente;MSISDN;IMEI;IMSI;CustomerSince;Portabilidad;Operador Origen;Plan Origen Portabilidad;Product Offer;Fecha de la Venta;Precio";
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
