package main.tde.java;

import java.io.BufferedReader;
import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.sql.Connection;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;

import java.sql.DriverManager;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import javax.xml.xpath.XPath;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Common {

    private Connection connection = null;

    private String connectionURI;
    private String user;
    private String pass;

    public Connection getConnection() throws Exception {
        if (connection == null || connection.isClosed()) {
            Class.forName("oracle.jdbc.OracleDriver");
            connection = DriverManager.getConnection(connectionURI, user, pass);
        }
        return connection;
    }

    public Common(String connectionURI, String user, String pass) {
        super();
        this.setConnectionURI(connectionURI);
        this.setUser(user);
        this.setPass(pass);
    }

    public Common() {
        super();
    }

    public List<String[]> readSapFile(String path) {
        List<String[]> params = new ArrayList();
        int count = 0;
        String sCurrentLine;
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            File folder = new File(path);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File file : files) {
                    if (file.getName().contains("txt")) {
                        fileInputStream = new FileInputStream(file.getPath().toString());
                        inputStreamReader = new InputStreamReader(fileInputStream);
                        bufferedReader = new BufferedReader(inputStreamReader);
                        while ((sCurrentLine = bufferedReader.readLine()) != null) {
                            if (count > 0) {
                                if (!sCurrentLine.contains("DEPARTAMENTO")) {
                                    params.add(sCurrentLine.split("\\|"));
                                }
                            } else {
                                count++;
                            }
                        }
                    }
                }
            } else {
                fileInputStream = new FileInputStream(path);
                inputStreamReader = new InputStreamReader(fileInputStream);
                bufferedReader = new BufferedReader(inputStreamReader);
                while ((sCurrentLine = bufferedReader.readLine()) != null) {
                    if (count > 0) {
                        params.add(sCurrentLine.split("\\|"));
                    } else {
                        count++;
                    }
                }
            }
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStreamReader != null) {
                inputStreamReader.close();
            }
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return params;
    }

    public List<String[]> readECMFile(String path) {
        List<String[]> params = new ArrayList();
        try {
            File folder = new File(path);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File xmlFile : files) {
                    if (xmlFile.getName().contains("XML")) {
                        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                        dbFactory.setNamespaceAware(true);
                        DocumentBuilder dBuilder;
                        dBuilder = dbFactory.newDocumentBuilder();
                        Document doc = dBuilder.parse(xmlFile);
                        doc.getDocumentElement().normalize();

                        String sku = "";
                        String charspecificationSubtype = "";
                        String model = "";
                        String brand = "";

                        String productofferingID =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1", "ID").getLength() >
                            0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "ID").item(0).getTextContent() : "";

                        String name =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "name").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "name").item(0).getTextContent() : "";

                        NodeList priceNodes = doc.getElementsByTagNameNS("http://www.entel.cl/EBO/Money/v1", "amount");
                        float price = 0;
                        for (int i = 0; i < priceNodes.getLength(); i++) {
                            if (priceNodes.item(i).getTextContent() != null &&
                                !priceNodes.item(i).getTextContent().equals("")) {
                                price += Float.parseFloat(priceNodes.item(i).getTextContent());
                            }
                        }

                        String description =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "description").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "description").item(0).getTextContent() : "";

                        String family =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "family").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "family").item(0).getTextContent() : "";

                        String specificationSubtype =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "specificationSubtype").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "specificationSubtype").item(0).getTextContent() : "";
                        String salesCategory =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "salesCategory").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "salesCategory").item(0).getTextContent() : "";
                        String country =
                            doc.getElementsByTagNameNS("http://www.entel.cl/ESO/MessageHeader/v1",
                                                       "Consumer").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/ESO/MessageHeader/v1",
                                                       "Consumer").item(0).getTextContent() : "CHL";
                        NodeList productSpecCharacteristics =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBM/ProductOffering/Get/v1",
                                                       "ProductSpecCharacteristic");

                        if (productSpecCharacteristics.getLength() == 0) {
                            productSpecCharacteristics =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/ProductOffering/Publish_File/v1",
                                                           "ProductSpecCharacteristic");
                        }

                        for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                            Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                            NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                            for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("sku")) {
                                    sku = getSkuValue(productSpecCharacteristicNodes);
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("specificationSubSubtype")) {
                                    charspecificationSubtype =
                                        getSpecificationSubtypeValue(productSpecCharacteristicNodes);
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("model")) {
                                    model = getModelValue(productSpecCharacteristicNodes);
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("brand")) {
                                    brand = getBrandValue(productSpecCharacteristicNodes);
                                }
                            }
                        }
                        String[] param = new String[] {
                            productofferingID, sku, family, specificationSubtype, salesCategory, country,
                            charspecificationSubtype, description, name, price + "", model, brand
                        };
                        if (family.equalsIgnoreCase("Mobile") && isValidSpecificationSubtype(specificationSubtype) &&
                            isValidSpecificationSubtypeValue(specificationSubtype, charspecificationSubtype)) {
                            params.add(param);
                        }
                        xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() + File.separator +
                                                  "PROCESSED" + File.separator + xmlFile.getName()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return params;
    }

    public List<String[]> readCatalogFile(String path) {
        List<String[]> params = new ArrayList();
        try {

            File folder = new File(path);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File xmlFile : files) {
                    if (xmlFile.getName().contains("XML")) {
                        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                        dbFactory.setNamespaceAware(true);
                        DocumentBuilder dBuilder;
                        dBuilder = dbFactory.newDocumentBuilder();
                        Document doc = dBuilder.parse(xmlFile);
                        doc.getDocumentElement().normalize();

                        String sku = "";
                        String charspecificationSubtype = "";
                        String model = "";
                        String technology = "";
                        String subFamily = "";
                        String finalPrice = "";
                        String productofferingID =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1", "ID").getLength() >
                            0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "ID").item(0).getTextContent() : "";
                        String name =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "name").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "name").item(0).getTextContent() : "";
                        NodeList priceNodes = doc.getElementsByTagNameNS("http://www.entel.cl/EBO/Money/v1", "amount");

                        String unit = "";
                        NodeList unitsNodes = doc.getElementsByTagNameNS("http://www.entel.cl/EBO/Money/v1", "units");

                        for (int j = 0; j < unitsNodes.getLength(); j++) {
                            Node unitsNode = unitsNodes.item(j);
                            if (unitsNode.getTextContent() != null && !unitsNode.getTextContent().equals("")) {
                                unit = unitsNode.getTextContent();
                                break;
                            }
                        }

                        float price = 0;
                        for (int i = 0; i < priceNodes.getLength(); i++) {
                            if (priceNodes.item(i).getTextContent() != null &&
                                !priceNodes.item(i).getTextContent().equals("")) {
                                price += Float.parseFloat(priceNodes.item(i).getTextContent());
                            }
                        }

                        finalPrice = price + "";
                        String description =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "description").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/ProductOffering/v1",
                                                       "description").item(0).getTextContent() : "";

                        String startDate =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/TimePeriod/v1",
                                                       "startDate").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBO/TimePeriod/v1",
                                                       "startDate").item(0).getTextContent() : "";
                        if (!startDate.equals("")) {
                            startDate = startDate.replace("-", "");
                            startDate = startDate.replace(":", "");
                            startDate = startDate.replace("T", "");
                            startDate = startDate.substring(0, startDate.indexOf('.'));
                        }
                        NodeList productSpecCharacteristics =
                            doc.getElementsByTagNameNS("http://www.entel.cl/EBM/ProductOffering/Get/v1",
                                                       "ProductSpecCharacteristic");
                        if (productSpecCharacteristics.getLength() == 0) {
                            productSpecCharacteristics =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/ProductOffering/Publish_File/v1",
                                                           "ProductSpecCharacteristic");
                        }

                        for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                            Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                            NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                            for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("sku")) {
                                    sku = getSkuValue(productSpecCharacteristicNodes);
                                    if (sku != null && !sku.equals("")) {
                                        break;
                                    }
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("specificationSubSubtype")) {
                                    charspecificationSubtype =
                                        getSpecificationSubtypeValue(productSpecCharacteristicNodes);
                                    if (charspecificationSubtype != null && !sku.equals("")) {
                                        break;
                                    }
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("model")) {
                                    model = getModelValue(productSpecCharacteristicNodes);
                                    if (model != null && !sku.equals("")) {
                                        break;
                                    }
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("technology")) {
                                    technology = getTechnologyValue(productSpecCharacteristicNodes);
                                    if (technology != null && !sku.equals("")) {
                                        break;
                                    }
                                }

                                if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                    productSpecCharacteristicNode.getNodeName().split(":")[1].equals("id") &&
                                    productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("subFamily")) {
                                    subFamily = getSubFamilyValue(productSpecCharacteristicNodes);
                                    if (subFamily != null && !sku.equals("")) {
                                        break;
                                    }
                                }
                            }
                        }
                        if (subFamily == null || subFamily.equals("")) {
                            subFamily = "NA";
                        }
                        if (charspecificationSubtype == null || charspecificationSubtype.equals("")) {
                            charspecificationSubtype = "NA";
                        }
                        if (technology == null || technology.equals("")) {
                            technology = "NA";
                        }
                        String[] param = new String[] {
                            productofferingID, name, description, subFamily, charspecificationSubtype, technology, name,
                            model, sku, finalPrice, startDate, startDate
                        };
                        params.add(param);
                        xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() + File.separator +
                                                  "PROCESSED" + File.separator + xmlFile.getName()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return params;
    }

    public boolean isValidSpecificationSubtype(String specificationSubtype) {
        return specificationSubtype.equalsIgnoreCase("Add-on") || specificationSubtype.equalsIgnoreCase("Equipment") ||
               specificationSubtype.equalsIgnoreCase("Product");
    }

    public boolean isValidSpecificationSubtypeValue(String specificationSubtype, String charspecificationSubtype) {
        boolean result = false;

        switch (specificationSubtype) {
        case "Add-on":
            switch (charspecificationSubtype) {
            case "Mix":
                result = true;
                break;
            }
            break;
        case "Equipment":
            switch (charspecificationSubtype) {
            case "Bundle":
                result = true;
                break;
            case "Modem":
                result = true;
                break;
            case "Cellphone":
                result = true;
                break;
            case "Card":
                result = true;
                break;
            case "Accessory":
                result = true;
                break;
            }
            break;
        case "Product":
            switch (charspecificationSubtype) {
            case "Delivery":
                result = true;
                break;
            }
            break;
        }
        return result;
    }

    public String getSkuValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getSubFamilyValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getTechnologyValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getPriceValue(NodeList productOfferingPriceNodes) {
        String value = "";
        for (int j = 0; j < productOfferingPriceNodes.getLength(); j++) {
            Node productOfferingPriceNode = productOfferingPriceNodes.item(j);
            if (productOfferingPriceNode.getNodeName().split(":").length > 1 &&
                productOfferingPriceNode.getNodeName().split(":")[1].equals("amount")) {
                value = productOfferingPriceNode.getFirstChild().getTextContent();
            }
        }
        return value;
    }

    public String getBrandValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getSpecificationSubtypeValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getSpecificationSubSubtypeValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getModelValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String[] readSiebelData(String salesCode, String MSISDN) {
        String[] array = new String[] { "", "", "", "", "", "", "", "", "" };
        try {
            Statement stmt = null;
            String query =
                "select scon.soc_security_num \"doc_ejec_responsable\",\n" + "         suse.login \"ejec_login\",\n" +
                "         soex.par_duns_num \"tipo_documento_cli\",\n" + "         soex.name \"nombre_cli\",\n" +
                "         sadp.city \"ciudad\",\n" + "         sasx.attrib_35 \"imei\",\n" +
                "         soex.cust_since_dt \"customer_since\",\n" + "         ssrq.integration_id \"order_id\",\n" +
                "         ssrq.ACT_CLOSE_DT \"fecha_venta\",\n" + "         sast.START_DT \"fecha_activacion\"" +
                "    from siebel.s_asset sast,\n" + "         siebel.s_org_ext soex,\n" +
                "         siebel.s_addr_per sadp,\n" + "         siebel.s_asset_x sasx,\n" +
                "         siebel.s_srv_req ssrq,\n" + "         siebel.s_user suse,\n" +
                "         siebel.s_contact scon\n" + "   where sast.owner_accnt_id = soex.par_row_id\n" +
                "     and soex.pr_addr_id = sadp.row_id(+)\n" + "     and sast.row_id = sasx.par_row_id(+)\n" +
                "     and soex.row_id = ssrq.cst_ou_id(+)\n" + "     and ssrq.owner_emp_id = suse.par_row_id(+)\n" +
                "     and ssrq.owner_emp_id = scon.par_row_id(+)\n" + "     and sast.status_cd = 'Activo'\n" +
                "     and ssrq.integration_id = '" + salesCode + "'" + "     and sast.SERIAL_NUM = '" + MSISDN + "'";

            stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String DOC_EJEC_RESPONSABLE = "";
                if (hasColumn(rs, "doc_ejec_responsable") && rs.getString("doc_ejec_responsable") != null) {
                    DOC_EJEC_RESPONSABLE = rs.getString("doc_ejec_responsable");
                }
                String EJEC_LOGIN = "";
                if (hasColumn(rs, "ejec_login") && rs.getString("ejec_login") != null) {
                    EJEC_LOGIN = rs.getString("ejec_login");
                }
                String TIPO_DOCUMENTO_CLI = "";
                if (hasColumn(rs, "tipo_documento_cli") && rs.getString("tipo_documento_cli") != null) {
                    TIPO_DOCUMENTO_CLI = rs.getString("tipo_documento_cli");
                }
                String NOMBRE_CLI = "";
                if (hasColumn(rs, "nombre_cli") && rs.getString("nombre_cli") != null) {
                    NOMBRE_CLI = rs.getString("nombre_cli");
                }
                String CIUDAD = "";
                if (hasColumn(rs, "ciudad") && rs.getString("ciudad") != null) {
                    CIUDAD = rs.getString("ciudad");
                }
                java.util.Date fecha_venta = null;
                if (hasColumn(rs, "fecha_venta") && rs.getDate("fecha_venta") != null) {
                    fecha_venta = rs.getDate("fecha_venta");
                }
                java.util.Date fecha_activacion = null;
                if (hasColumn(rs, "fecha_activacion") && rs.getDate("fecha_activacion") != null) {
                    fecha_activacion = rs.getDate("fecha_activacion");
                }
                String IMEI = "";
                if (hasColumn(rs, "imei") && rs.getString("imei") != null) {
                    IMEI = rs.getString("imei");
                }
                java.util.Date CUSTOMER_SINCE = null;
                if (hasColumn(rs, "customer_since") && rs.getDate("customer_since") != null) {
                    CUSTOMER_SINCE = rs.getDate("customer_since");
                }
                array[0] = DOC_EJEC_RESPONSABLE;
                array[1] = EJEC_LOGIN;
                array[2] = TIPO_DOCUMENTO_CLI;
                array[3] = NOMBRE_CLI;
                array[4] = CIUDAD;
                array[5] = IMEI;

                SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
                if (CUSTOMER_SINCE != null) {
                    array[6] = df.format(CUSTOMER_SINCE);
                } else {
                    array[6] = "";
                }
                if (fecha_venta != null) {
                    array[7] = df.format(fecha_venta);
                } else {
                    array[7] = "";
                }
                if (fecha_activacion != null) {
                    array[8] = df.format(fecha_activacion);
                } else {
                    array[8] = "";
                }
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (connection != null) {
        } else {
        }
        return array;
    }

    public List<String[]> readEBSDB(String url, String buser, String bpass) {
        List<String[]> params = new ArrayList();
        Connection bconnection = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (ClassNotFoundException e) {
        }
        try {
            bconnection = DriverManager.getConnection(connectionURI, user, pass);
        } catch (SQLException e) {
        }
        try {
            Statement stmt = null;
            String query =
                "select a.segment1 codigo_articulo,\n" + "       a.description,\n" +
                "       a.primary_unit_of_measure,\n" + "       a.attribute1 descripcion_ingles,\n" +
                "       a.attribute2 descripcion_reposiciones,\n" + "       a.attribute3 modelo_equipo,\n" +
                "       a.attribute6 peso,\n" + "       a.attribute7 tipo_articulo,\n" +
                "       a.attribute8 subtipo,\n" + "       a.attribute11 proveedor,\n" +
                "       a.attribute12 tecnologia,\n" + "       a.list_price_per_unit precio_de_lista,\n" +
                "       a.last_update_date,\n" + "       a.creation_date,\n" + "       'TERMINALES' departamento,\n" +
                "       d.segment1 clase,\n" + "       d.segment2 subclase\n" + "  from inv.mtl_system_items_b  a,\n" +
                "       inv.mtl_parameters      b,\n" + "       inv.mtl_item_categories c,\n" +
                "       inv.mtl_categories_b    d\n" + " where 1 = 1\n" +
                "   and a.organization_id = b.organization_id\n" +
                "   and a.inventory_item_id = c.inventory_item_id\n" + "   and c.category_id = d.category_id\n" +
                "   and b.organization_code = 'NPE'\n" + "   and a.attribute7 = 'Accesorio'\n" +
                "   and d.structure_id =\n" + "       (select x.structure_id\n" +
                "          from inv.mtl_category_sets_b x, mtl_category_sets_tl y\n" +
                "         where x.category_set_id = y.category_set_id\n" +
                "           and y.category_set_name = 'Inventory'\n" + "           and y.language = 'ESA')";

            stmt = bconnection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String[] array = new String[17];
                String codigo_articulo = "";
                if (hasColumn(rs, "codigo_articulo") && rs.getString("codigo_articulo") != null) {
                    codigo_articulo = rs.getString("codigo_articulo");
                }
                String description = "";
                if (hasColumn(rs, "description") && rs.getString("description") != null) {
                    description = rs.getString("description");
                }
                String primary_unit_of_measure = "";
                if (hasColumn(rs, "primary_unit_of_measure") && rs.getString("primary_unit_of_measure") != null) {
                    primary_unit_of_measure = rs.getString("primary_unit_of_measure");
                }
                String descripcion_ingles = "";
                if (hasColumn(rs, "descripcion_ingles") && rs.getString("descripcion_ingles") != null) {
                    descripcion_ingles = rs.getString("descripcion_ingles");
                }
                String descripcion_reposiciones = "";
                if (hasColumn(rs, "descripcion_reposiciones") && rs.getString("descripcion_reposiciones") != null) {
                    descripcion_reposiciones = rs.getString("descripcion_reposiciones");
                }
                String modelo_equipo = "";
                if (hasColumn(rs, "modelo_equipo") && rs.getString("modelo_equipo") != null) {
                    modelo_equipo = rs.getString("modelo_equipo");
                }
                String peso = "";
                if (hasColumn(rs, "peso") && rs.getString("peso") != null) {
                    peso = rs.getString("peso");
                }
                String tipo_articulo = "";
                if (hasColumn(rs, "tipo_articulo") && rs.getString("tipo_articulo") != null) {
                    tipo_articulo = rs.getString("tipo_articulo");
                }
                String subtipo = "";
                if (hasColumn(rs, "subtipo") && rs.getString("subtipo") != null) {
                    subtipo = rs.getString("subtipo");
                }
                String proveedor = "";
                if (hasColumn(rs, "proveedor") && rs.getString("proveedor") != null) {
                    proveedor = rs.getString("proveedor");
                }
                String tecnologia = "";
                if (hasColumn(rs, "tecnologia") && rs.getString("tecnologia") != null) {
                    tecnologia = rs.getString("tecnologia");
                }
                String precio_de_lista = "";
                if (hasColumn(rs, "precio_de_lista") && rs.getString("precio_de_lista") != null) {
                    precio_de_lista = rs.getString("precio_de_lista");
                }
                String last_update_date = "";
                if (hasColumn(rs, "last_update_date") && rs.getString("last_update_date") != null) {
                    last_update_date = rs.getString("last_update_date");
                }
                String creation_date = "";
                if (hasColumn(rs, "creation_date") && rs.getString("creation_date") != null) {
                    creation_date = rs.getString("creation_date");
                }
                String departamento = "";
                if (hasColumn(rs, "departamento") && rs.getString("departamento") != null) {
                    departamento = rs.getString("departamento");
                }
                String clase = "";
                if (hasColumn(rs, "clase") && rs.getString("clase") != null) {
                    clase = rs.getString("clase");
                }
                String subclase = "";
                if (hasColumn(rs, "subclase") && rs.getString("subclase") != null) {
                    subclase = rs.getString("subclase");
                }
                array[0] = codigo_articulo;
                array[1] = description;
                array[2] = primary_unit_of_measure;
                array[3] = descripcion_ingles;
                array[4] = descripcion_reposiciones;
                array[5] = modelo_equipo;
                array[6] = peso;
                array[7] = tipo_articulo;
                array[8] = subtipo;
                array[9] = proveedor;
                array[10] = tecnologia;
                array[11] = precio_de_lista;
                array[12] = last_update_date;
                array[13] = creation_date;
                array[14] = departamento;
                array[15] = clase;
                array[16] = subclase;
                params.add(array);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (connection != null) {
        } else {
        }
        return params;
    }

    public List<String[]> readActivePreFile(String path) {
        List<String[]> params = new ArrayList();
        try {
            File folder = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setNamespaceAware(true);
            DocumentBuilder dBuilder;
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = null;
            String[] param = null;
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File xmlFile : files) {
                    try {
                        if (xmlFile.getName().contains("XML")) {
                            doc = dBuilder.parse(xmlFile);
                            doc.getDocumentElement().normalize();

                            String area =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "area").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "area").item(0).getTextContent() : "";
                            if (area.equalsIgnoreCase("Activacion de Linea") || area.equalsIgnoreCase("Retorno In") ||
                                area.equalsIgnoreCase("Portabilidad")) {
                                String MSISDN = "";
                                String contractCode = "";
                                String sucursalCode = "";
                                String clientCode = "";
                                String IMSI = "";
                                String portabilityFlag = "0";
                                String originPlanPortability = "";
                                String originOperator = "";
                                String productOrderCode = "";
                                String activationDate = "";

                                XPath xpath = XPathFactory.newInstance().newXPath();
                                xpath.setNamespaceContext(new NamespaceResolver(doc));

                                //Get productOrderCode
                                XPathExpression expr =
                                    xpath.compile("/ns2:FMS-PublishProductOrder_REQ/ns2:Body/ns2:CustomerOrder/ns2:item/ns2:ProductOffering[ns2:Product/ns2:ProductSpecification/ns2:ProductSpecCharacteristic/ns2:name='specificationSubtype' and ns2:Product/ns2:ProductSpecification/ns2:ProductSpecCharacteristic/ns2:value='Plan']/ns2:ID/text()");
                                NodeList nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

                                try {
                                    productOrderCode = nodes.item(0).getTextContent();
                                } catch (NullPointerException ex) {
                                    productOrderCode = "";
                                }

                                //Get activationDate
                                expr =
                                    xpath.compile("/ns2:FMS-PublishProductOrder_REQ/ns2:Body/ns2:CustomerOrder/ns2:createdDate/text()");
                                nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

                                try {
                                    activationDate = transformaFecha(nodes.item(0).getTextContent(), "yyyyMMddHHmmss");
                                } catch (NullPointerException ex) {
                                    activationDate = "";
                                }

                                //Get originPlanPortability
                                expr =
                                    xpath.compile("/ns2:FMS-PublishProductOrder_REQ/ns2:Body/ns2:CustomerOrder/ns2:RelatedParty/ns2:PortabilityOrder/ns2:subscriberType/text()");
                                nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

                                try {
                                    if (!nodes.item(0).getTextContent().isEmpty()) {
                                        portabilityFlag = "1";
                                        originPlanPortability = nodes.item(0).getTextContent();
                                    }
                                } catch (NullPointerException ex) {
                                    originPlanPortability = "";
                                }

                                //Get originOperator
                                expr =
                                    xpath.compile("/ns2:FMS-PublishProductOrder_REQ/ns2:Body/ns2:CustomerOrder/ns2:RelatedParty/ns2:PortabilityOrder/ns2:originOperator/text()");
                                nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

                                try {
                                    originOperator = nodes.item(0).getTextContent();
                                } catch (NullPointerException ex) {
                                    originOperator = "";
                                }

                                NodeList imsiNodes =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "IMSI").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "IMSI").item(0).getChildNodes() : null;
                                if (imsiNodes != null) {
                                    for (int j = 0; j < imsiNodes.getLength(); j++) {
                                        Node msisdnNode = imsiNodes.item(j);
                                        if (msisdnNode.getNodeName().split(":").length > 1 &&
                                            msisdnNode.getNodeName().split(":")[1].equals("SN")) {
                                            IMSI = msisdnNode.getTextContent();
                                        }
                                    }
                                }
                                
                                Node iccidNodes =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "ICCID").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "ICCID").item(0) : null;
                                if (iccidNodes != null) {
                                    IMSI = iccidNodes.getTextContent();
                                }

                                NodeList productSpecCharacteristics =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "ProductSpecCharacteristic").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "ProductSpecCharacteristic") : null;
                                if (productSpecCharacteristics != null) {
                                    for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                                        Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                                        NodeList productSpecCharacteristicNodes =
                                            productSpecCharacteristic.getChildNodes();

                                        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("name") &&
                                                productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("contractId")) {
                                                contractCode = getContractCodeValue(productSpecCharacteristicNodes);
                                                if (!contractCode.isEmpty()) {
                                                    break;
                                                }
                                            }
                                        }
                                        if (!contractCode.isEmpty()) {
                                            break;
                                        }
                                    }
                                }

                                NodeList salesChannels =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "SalesChannel").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "SalesChannel") : null;
                                if (salesChannels != null) {

                                    for (int i = 0; i < salesChannels.getLength(); i++) {
                                        Node salesChannel = salesChannels.item(i);
                                        NodeList salesChannelNodes = salesChannel.getChildNodes();

                                        for (int j = 0; j < salesChannelNodes.getLength(); j++) {
                                            Node salesChannelNode = salesChannelNodes.item(j);
                                            if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                                salesChannelNode.getNodeName().split(":")[1].equals("orderCommercialChannel")) {
                                                sucursalCode = salesChannelNode.getTextContent();
                                            }
                                            if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                                salesChannelNode.getNodeName().split(":")[1].equals("ID")) {
                                                if (!salesChannelNode.getTextContent().isEmpty()) {
                                                    sucursalCode = salesChannelNode.getTextContent();
                                                }
                                                if (!sucursalCode.isEmpty()) {
                                                    break;
                                                }
                                            }
                                        }
                                        if (!sucursalCode.isEmpty()) {
                                            break;
                                        }
                                    }
                                }

                                NodeList customerAccounts =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "CustomerAccount").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "CustomerAccount") : null;
                                if (customerAccounts != null) {
                                    for (int i = 0; i < customerAccounts.getLength(); i++) {
                                        Node customerAccount = customerAccounts.item(i);
                                        NodeList customerAccountNodes = customerAccount.getChildNodes();

                                        for (int j = 0; j < customerAccountNodes.getLength(); j++) {
                                            Node customerAccountNode = customerAccountNodes.item(j);
                                            if (customerAccountNode.getNodeName().split(":").length > 1 &&
                                                customerAccountNode.getNodeName().split(":")[1].equals("ID")) {
                                                clientCode = customerAccountNode.getTextContent();
                                                if (!clientCode.isEmpty()) {
                                                    break;
                                                }
                                            }
                                        }
                                        if (!clientCode.isEmpty()) {
                                            break;
                                        }
                                    }
                                }

                                String salesCode =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "shoppingCartID").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "shoppingCartID").item(0).getTextContent() : "";

                                NodeList msisdnNodes =
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "MSISDN").getLength() > 0 ?
                                    doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                               "MSISDN").item(0).getChildNodes() : null;
                                if (msisdnNodes != null) {
                                    for (int j = 0; j < msisdnNodes.getLength(); j++) {
                                        Node msisdnNode = msisdnNodes.item(j);
                                        if (msisdnNode.getNodeName().split(":").length > 1 &&
                                            msisdnNode.getNodeName().split(":")[1].equals("SN")) {
                                            MSISDN = msisdnNode.getTextContent();
                                        }
                                    }
                                }

                                String[] siebelData = readSiebelData(salesCode, MSISDN);

                                String rut = siebelData[0];
                                String docType = siebelData[2];
                                String clientName = siebelData[3];
                                String clientCity = siebelData[4];
                                String imei = siebelData[5];
                                String customerSince = siebelData[6];
                                String fecha_venta = siebelData[7];
                                String fecha_activacion = siebelData[8];

                                param = new String[] {
                                    salesCode, contractCode, sucursalCode, rut, clientCode, docType, "", clientName,
                                    clientCity, MSISDN, imei, IMSI, customerSince, portabilityFlag, originOperator,
                                    originPlanPortability, productOrderCode, fecha_venta, fecha_activacion
                                };
                                params.add(param);
                            }
                            xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() +
                                                      File.separator + "PROCESADO" + File.separator +
                                                      xmlFile.getName()));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() + File.separator +
                                                  "ERROR" + File.separator + xmlFile.getName()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return params;
    }


    public String[] readSiebelData(String customerId) {
        String[] array = new String[6];
        try {
            Statement stmt = null;
            String query =
                "select soex.duns_num \"doc_ejec_responsable\",\n" +
                "         soex.par_duns_num \"tipo_documento_cli\",\n" + "         soex.name \"nombre_cli\",\n" +
                "         sadp.city \"ciudad\",\n" + "         sast.serial_num \"IMEI\",\n" +
                "         soex.cust_since_dt \"customer_since\"\n" + "    from siebel.s_asset sast,\n" +
                "         siebel.s_org_ext soex,\n" + "         siebel.s_addr_per sadp\n" +
                "   where sast.owner_accnt_id (+) = soex.par_row_id\n" + "     and soex.pr_addr_id = sadp.row_id(+)\n" +
                "     and serial_num='" + "msisdn" + "'";
            String queryId =
                "select scon.soc_security_num \"doc_ejec_responsable\",\n" + "         suse.login \"ejec_login\",\n" +
                "         soex.par_duns_num \"tipo_documento_cli\",\n" + "         soex.name \"nombre_cli\",\n" +
                "         sadp.city \"ciudad\",\n" + "         sasx.attrib_35 \"imei\",\n" +
                "         soex.cust_since_dt \"customer_since\",\n" + "         ssrq.integration_id \"order_id\"\n" +
                "    from siebel.s_asset sast,\n" + "         siebel.s_org_ext soex,\n" +
                "         siebel.s_addr_per sadp,\n" + "         siebel.s_asset_x sasx,\n" +
                "         siebel.s_srv_req ssrq,\n" + "         siebel.s_user suse,\n" +
                "         siebel.s_contact scon\n" + "   where sast.owner_accnt_id (+) = soex.par_row_id\n" +
                "     and soex.pr_addr_id = sadp.row_id(+)\n" + "     and sast.row_id = sasx.par_row_id(+)\n" +
                "     and soex.row_id = ssrq.cst_ou_id(+)\n" + "     and ssrq.owner_emp_id = suse.par_row_id(+)\n" +
                "     and ssrq.owner_emp_id = scon.par_row_id(+)\n" + "     and ssrq.integration_id = '" + customerId +
                "'" + "";
            stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(queryId);
            while (rs.next()) {
                String DOC_EJEC_RESPONSABLE = "";
                if (hasColumn(rs, "doc_ejec_responsable") && rs.getString("doc_ejec_responsable") != null) {
                    DOC_EJEC_RESPONSABLE = rs.getString("doc_ejec_responsable");
                }
                String TIPO_DOCUMENTO_CLI = "";
                if (hasColumn(rs, "tipo_documento_cli") && rs.getString("tipo_documento_cli") != null) {
                    TIPO_DOCUMENTO_CLI = rs.getString("tipo_documento_cli");
                }
                String NOMBRE_CLI = "";
                if (hasColumn(rs, "nombre_cli") && rs.getString("nombre_cli") != null) {
                    NOMBRE_CLI = rs.getString("nombre_cli");
                }
                String CIUDAD = "";
                if (hasColumn(rs, "ciudad") && rs.getString("ciudad") != null) {
                    CIUDAD = rs.getString("ciudad");
                }
                String IMEI = "";
                if (hasColumn(rs, "imei") && rs.getString("imei") != null) {
                    IMEI = rs.getString("imei");
                }
                java.util.Date CUSTOMER_SINCE = null;
                if (hasColumn(rs, "customer_since") && rs.getDate("customer_since") != null) {
                    CUSTOMER_SINCE = rs.getDate("customer_since");
                }
                array[0] = DOC_EJEC_RESPONSABLE;
                array[1] = TIPO_DOCUMENTO_CLI;
                array[2] = NOMBRE_CLI;
                array[3] = CIUDAD;
                array[4] = IMEI;

                SimpleDateFormat df = new SimpleDateFormat("yyyMMddHHmmss");
                if (CUSTOMER_SINCE != null) {
                    array[5] = df.format(CUSTOMER_SINCE);
                } else {
                    array[5] = "";
                }
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (connection != null) {
        } else {
        }
        return array;
    }

    public String[] readSiebelDataAct(String customerId) {
        String[] array = new String[6];
        try {
            Statement stmt = null;
            String queryId =
                "select scon.soc_security_num \"doc_ejec_responsable\",\n" + "         suse.login \"ejec_login\",\n" +
                "         soex.par_duns_num \"tipo_documento_cli\",\n" + "         soex.name \"nombre_cli\",\n" +
                "         sadp.city \"ciudad\",\n" + "         sasx.attrib_35 \"imei\",\n" +
                "         soex.cust_since_dt \"customer_since\",\n" + "         ssrq.integration_id \"order_id\"\n" +
                "    from siebel.s_asset sast,\n" + "         siebel.s_org_ext soex,\n" +
                "         siebel.s_addr_per sadp,\n" + "         siebel.s_asset_x sasx,\n" +
                "         siebel.s_srv_req ssrq,\n" + "         siebel.s_user suse,\n" +
                "         siebel.s_contact scon\n" + "   where sast.owner_accnt_id (+) = soex.par_row_id\n" +
                "     and soex.pr_addr_id = sadp.row_id(+)\n" + "     and sast.row_id = sasx.par_row_id(+)\n" +
                "     and soex.row_id = ssrq.cst_ou_id(+)\n" + "     and ssrq.owner_emp_id = suse.par_row_id(+)\n" +
                "     and ssrq.owner_emp_id = scon.par_row_id(+)\n" + "     and ssrq.integration_id = '" + customerId +
                "'" + "";
            stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(queryId);
            while (rs.next()) {
                String DOC_EJEC_RESPONSABLE = "";
                if (hasColumn(rs, "doc_ejec_responsable") && rs.getString("doc_ejec_responsable") != null) {
                    DOC_EJEC_RESPONSABLE = rs.getString("doc_ejec_responsable");
                }
                String TIPO_DOCUMENTO_CLI = "";
                if (hasColumn(rs, "tipo_documento_cli") && rs.getString("tipo_documento_cli") != null) {
                    TIPO_DOCUMENTO_CLI = rs.getString("tipo_documento_cli");
                }
                String NOMBRE_CLI = "";
                if (hasColumn(rs, "nombre_cli") && rs.getString("nombre_cli") != null) {
                    NOMBRE_CLI = rs.getString("nombre_cli");
                }
                String CIUDAD = "";
                if (hasColumn(rs, "ciudad") && rs.getString("ciudad") != null) {
                    CIUDAD = rs.getString("ciudad");
                }
                String IMEI = "";
                if (hasColumn(rs, "imei") && rs.getString("imei") != null) {
                    IMEI = rs.getString("imei");
                }
                java.util.Date CUSTOMER_SINCE = null;
                if (hasColumn(rs, "customer_since") && rs.getDate("customer_since") != null) {
                    CUSTOMER_SINCE = rs.getDate("customer_since");
                }
                array[0] = DOC_EJEC_RESPONSABLE;
                array[1] = TIPO_DOCUMENTO_CLI;
                array[2] = NOMBRE_CLI;
                array[3] = CIUDAD;
                array[4] = IMEI;

                SimpleDateFormat df = new SimpleDateFormat("yyyMMddHHmmss");
                if (CUSTOMER_SINCE != null) {
                    array[5] = df.format(CUSTOMER_SINCE);
                } else {
                    array[5] = "";
                }
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (connection != null) {
        } else {
        }
        return array;
    }

    public List<String[]> readSalesPreFile(String path) {
        List<String[]> params = new ArrayList();
        try {
            File folder = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setNamespaceAware(true);
            DocumentBuilder dBuilder;
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = null;
            String[] param = null;
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();

                for (File xmlFile : files) {
                    if (xmlFile.getName().contains("XML")) {
                        doc = dBuilder.parse(xmlFile);
                        doc.getDocumentElement().normalize();
                        String area =
                            doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                       "area").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                       "area").item(0).getTextContent() : "";
                        if (area.equalsIgnoreCase("venta")) {
                            String sucursalCode = "";
                            String clientCode = "";
                            String contractCode = "";
                            String MSISDN = "";
                            String IMSI = "";
                            String portability = "";
                            String origenOperator = "";
                            String portabilityOrigenPlan = "";
                            String productOffer = "";
                            String salesDate = "";
                            String precio = "";

                            Boolean isValid = false;

                            String salesCode =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "shoppingCartID").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "shoppingCartID").item(0).getTextContent() : "";

                            salesDate =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "createdDate").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "createdDate").item(0).getTextContent() : "";

                            NodeList productSpecCharacteristics =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductSpecCharacteristic").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductSpecCharacteristic") : null;
                            if (productSpecCharacteristics != null) {
                                for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                                    Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                                    NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                                    for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                        Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                        if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                            productSpecCharacteristicNode.getNodeName().split(":")[1].equals("name") &&
                                            productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("contractId")) {
                                            contractCode = getContractCodeValue(productSpecCharacteristicNodes);
                                            if (!contractCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!contractCode.isEmpty()) {
                                        break;
                                    }
                                }


                                for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                                    Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                                    NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                                    for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                        Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                        if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                            productSpecCharacteristicNode.getNodeName().split(":")[1].equals("name") &&
                                            productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("specificationSubSubtype")) {
                                            String value = getContractCodeValue(productSpecCharacteristicNodes);
                                            if (value.equalsIgnoreCase("card") || value.equalsIgnoreCase("bundle")) {
                                                isValid = true;
                                                productOffer =
                                                    productSpecCharacteristicNode.getParentNode().getParentNode().getParentNode().getParentNode().getChildNodes().item(0).getTextContent();
                                                if (!productOffer.isEmpty()) {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if (!productOffer.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList salesChannels =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "SalesChannel").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "SalesChannel") : null;
                            if (salesChannels != null) {
                                for (int i = 0; i < salesChannels.getLength(); i++) {
                                    Node salesChannel = salesChannels.item(i);
                                    NodeList salesChannelNodes = salesChannel.getChildNodes();

                                    for (int j = 0; j < salesChannelNodes.getLength(); j++) {
                                        Node salesChannelNode = salesChannelNodes.item(j);
                                        if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                            salesChannelNode.getNodeName().split(":")[1].equals("orderCommercialChannel")) {
                                            sucursalCode = salesChannelNode.getTextContent();
                                        }
                                        if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                            salesChannelNode.getNodeName().split(":")[1].equals("ID")) {
                                            if (!salesChannelNode.getTextContent().isEmpty()) {
                                                sucursalCode = salesChannelNode.getTextContent();
                                            }
                                            if (!sucursalCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!sucursalCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList productOfferingPrices =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductOfferingPrice").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductOfferingPrice") : null;
                            if (productOfferingPrices != null) {
                                for (int i = 0; i < productOfferingPrices.getLength(); i++) {
                                    Node productOfferingPrice = productOfferingPrices.item(i);
                                    NodeList productOfferingPriceNodes = productOfferingPrice.getChildNodes();
                                    String taxCode = "tax";
                                    for (int j = 0; j < productOfferingPriceNodes.getLength(); j++) {
                                        Node productOfferingPriceNode = productOfferingPriceNodes.item(j);
                                        if (productOfferingPriceNode.getNodeName().split(":").length > 1 &&
                                            productOfferingPriceNode.getNodeName().split(":")[1].equals("taxCode")) {
                                            taxCode = productOfferingPriceNode.getTextContent();
                                        }
                                        if (taxCode == null || taxCode.equals("")) {
                                            precio = getPriceValue(productOfferingPriceNodes);
                                        }
                                    }
                                    if (!sucursalCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList customerAccounts =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "CustomerAccount").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "CustomerAccount") : null;
                            if (customerAccounts != null) {
                                for (int i = 0; i < customerAccounts.getLength(); i++) {
                                    Node customerAccount = customerAccounts.item(i);
                                    NodeList customerAccountNodes = customerAccount.getChildNodes();

                                    for (int j = 0; j < customerAccountNodes.getLength(); j++) {
                                        Node customerAccountNode = customerAccountNodes.item(j);
                                        if (customerAccountNode.getNodeName().split(":").length > 1 &&
                                            customerAccountNode.getNodeName().split(":")[1].equals("ID")) {
                                            clientCode = customerAccountNode.getTextContent();
                                            if (!clientCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!clientCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList msisdnNodes =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "MSISDN").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "MSISDN").item(0).getChildNodes() : null;
                            if (msisdnNodes != null) {
                                for (int j = 0; j < msisdnNodes.getLength(); j++) {
                                    Node msisdnNode = msisdnNodes.item(j);
                                    if (msisdnNode.getNodeName().split(":").length > 1 &&
                                        msisdnNode.getNodeName().split(":")[1].equals("SN")) {
                                        MSISDN = msisdnNode.getTextContent();
                                    }
                                }
                            }

                            NodeList resources =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "resourceCharacteristics").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "resourceCharacteristics") : null;

                            if (resources != null) {
                                for (int i = 0; i < resources.getLength(); i++) {
                                    Node resource = resources.item(i);
                                    NodeList resourcesNodes = resource.getChildNodes();
                                    for (int j = 0; j < resourcesNodes.getLength(); j++) {
                                        Node resourcesNode = resourcesNodes.item(j);
                                        if (resourcesNode.getNodeName().split(":").length > 1 &&
                                            resourcesNode.getNodeName().split(":")[1].equals("name")) {
                                            String resourceName = resourcesNode.getTextContent();
                                            if (resourceName.contains("serialNumber")) {
                                                IMSI = getImsiValue(resourcesNodes);
                                                break;
                                            }
                                        }
                                    }
                                    if (!IMSI.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            Node imsiNodes =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ICCID").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ICCID").item(0) : null;
                            if (imsiNodes != null) {
                                IMSI = imsiNodes.getTextContent();
                            }

                            NodeList portabilityOrders =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "PortabilityOrder").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "PortabilityOrder") : null;
                            if (portabilityOrders != null) {
                                for (int i = 0; i < portabilityOrders.getLength(); i++) {
                                    Node portabilityOrder = portabilityOrders.item(i);
                                    NodeList portabilityOrderNodes = portabilityOrder.getChildNodes();

                                    for (int j = 0; j < portabilityOrderNodes.getLength(); j++) {
                                        Node portabilityOrderNode = portabilityOrderNodes.item(j);
                                        if (portabilityOrderNode.getNodeName().split(":").length > 1 &&
                                            portabilityOrderNode.getNodeName().split(":")[1].equals("subscriberType")) {
                                            portabilityOrigenPlan = portabilityOrderNode.getTextContent();
                                            if (!portabilityOrigenPlan.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!portabilityOrigenPlan.isEmpty()) {
                                        break;
                                    }
                                }


                                portability = !portabilityOrigenPlan.isEmpty() ? "0" : "1";

                                for (int i = 0; i < portabilityOrders.getLength(); i++) {
                                    Node portabilityOrder = portabilityOrders.item(i);
                                    NodeList portabilityOrderNodes = portabilityOrder.getChildNodes();

                                    for (int j = 0; j < portabilityOrderNodes.getLength(); j++) {
                                        Node portabilityOrderNode = portabilityOrderNodes.item(j);
                                        if (portabilityOrderNode.getNodeName().split(":").length > 1 &&
                                            portabilityOrderNode.getNodeName().split(":")[1].equals("originOperator")) {
                                            origenOperator = portabilityOrderNode.getTextContent();
                                            if (!origenOperator.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!origenOperator.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            String[] siebelData = readSiebelData(salesCode);

                            String rut = siebelData[0];
                            String documentType = siebelData[1];
                            String clientName = siebelData[2];
                            String clientCity = siebelData[3];
                            String IMEI = siebelData[4];
                            String customerSince = siebelData[5];

                            salesDate = salesDate.split("\\.")[0].replace("-", "").replace("T", "").replace(":", "");

                            param = new String[] {
                                salesCode, contractCode, sucursalCode, rut, clientCode, documentType, "", clientName,
                                clientCity, MSISDN, IMEI, IMSI, customerSince, portability, origenOperator,
                                portabilityOrigenPlan, productOffer, salesDate, precio
                            };
                            if (isValid) {
                                params.add(param);
                            }

                        }
                        xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() + File.separator +
                                                  "PROCESSED" + File.separator + xmlFile.getName()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return params;
    }

    public List<String[]> readSimPreFile(String path) {
        List<String[]> params = new ArrayList();
        try {
            File folder = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setNamespaceAware(true);
            DocumentBuilder dBuilder;
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = null;
            String[] param = null;
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();

                for (File xmlFile : files) {
                    if (xmlFile.getName().contains("XML")) {
                        doc = dBuilder.parse(xmlFile);
                        doc.getDocumentElement().normalize();
                        String area =
                            doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                       "area").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                       "area").item(0).getTextContent() : "";
                        if (area.equalsIgnoreCase("cambio")) {
                            String sucursalCode = "";
                            String clientCode = "";
                            String contractCode = "";
                            String MSISDN = "";
                            String IMSI = "";
                            String salesDate = "";
                            int isValid = 0;

                            String salesCode =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "shoppingCartID").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "shoppingCartID").item(0).getTextContent() : "";

                            salesDate =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "createdDate").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "createdDate").item(0).getTextContent() : "";

                            NodeList productSpecCharacteristics =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductSpecCharacteristic").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductSpecCharacteristic") : null;

                            if (productSpecCharacteristics != null) {
                                for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                                    Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                                    NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                                    for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                        Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                        if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                            productSpecCharacteristicNode.getNodeName().split(":")[1].equals("name") &&
                                            productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("contractId")) {
                                            contractCode = getContractCodeValue(productSpecCharacteristicNodes);
                                            if (!contractCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }


                                    if (!contractCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList salesChannels =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "SalesChannel").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "SalesChannel") : null;
                            if (salesChannels != null) {

                                for (int i = 0; i < salesChannels.getLength(); i++) {
                                    Node salesChannel = salesChannels.item(i);
                                    NodeList salesChannelNodes = salesChannel.getChildNodes();

                                    for (int j = 0; j < salesChannelNodes.getLength(); j++) {
                                        Node salesChannelNode = salesChannelNodes.item(j);
                                        if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                            salesChannelNode.getNodeName().split(":")[1].equals("orderCommercialChannel")) {
                                            sucursalCode = salesChannelNode.getTextContent();
                                        }
                                        if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                            salesChannelNode.getNodeName().split(":")[1].equals("ID")) {
                                            if (!salesChannelNode.getTextContent().isEmpty()) {
                                                sucursalCode = salesChannelNode.getTextContent();
                                            }
                                            if (!sucursalCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!sucursalCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }
                            NodeList customerAccounts =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "CustomerAccount").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "CustomerAccount") : null;
                            if (customerAccounts != null) {
                                for (int i = 0; i < customerAccounts.getLength(); i++) {
                                    Node customerAccount = customerAccounts.item(i);
                                    NodeList customerAccountNodes = customerAccount.getChildNodes();

                                    for (int j = 0; j < customerAccountNodes.getLength(); j++) {
                                        Node customerAccountNode = customerAccountNodes.item(j);
                                        if (customerAccountNode.getNodeName().split(":").length > 1 &&
                                            customerAccountNode.getNodeName().split(":")[1].equals("ID")) {
                                            clientCode = customerAccountNode.getTextContent();
                                            if (!clientCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!clientCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList msisdnNodes =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "MSISDN").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "MSISDN").item(0).getChildNodes() : null;
                            if (msisdnNodes != null) {
                                for (int j = 0; j < msisdnNodes.getLength(); j++) {
                                    Node msisdnNode = msisdnNodes.item(j);
                                    if (msisdnNode.getNodeName().split(":").length > 1 &&
                                        msisdnNode.getNodeName().split(":")[1].equals("SN")) {
                                        MSISDN = msisdnNode.getTextContent();
                                    }
                                }
                            }
                            NodeList resourcesSpec =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ResorceSpecification").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ResorceSpecification") : null;

                            if (resourcesSpec != null) {
                                for (int i = 0; i < resourcesSpec.getLength(); i++) {
                                    Node resource = resourcesSpec.item(i).getFirstChild();
                                    if (resource.getNodeName().split(":").length > 1 &&
                                        resource.getNodeName().split(":")[1].equals("ID")) {
                                        String resourceId = resource.getTextContent();
                                        if (resourceId.equals("PRS_SIM")) {
                                            isValid++;
                                            break;
                                        }
                                    }
                                }
                            }

                            NodeList resourceChar =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "resourceCharacteristics").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "resourceCharacteristics") : null;

                            if (resourceChar != null) {
                                String oldSerialNumber = "";
                                for (int i = 0; i < resourceChar.getLength(); i++) {
                                    Node resource = resourceChar.item(i);
                                    NodeList resourcesNodes = resource.getChildNodes();
                                    if (oldSerialNumber != null && !oldSerialNumber.equals("")) {
                                        break;
                                    }
                                    for (int j = 0; j < resourcesNodes.getLength(); j++) {
                                        Node resourcesNode = resourcesNodes.item(j);
                                        if (resourcesNode.getNodeName().split(":").length > 1 &&
                                            resourcesNode.getNodeName().split(":")[1].equals("name")) {
                                            String resourceName = resourcesNode.getTextContent();
                                            if (resourceName.contains("oldSerialNumber")) {
                                                oldSerialNumber = getImeiValue(resourcesNodes);
                                                if (oldSerialNumber != null && !oldSerialNumber.equals("")) {
                                                    isValid++;
                                                    break;
                                                }
                                            }
                                        }

                                    }
                                }

                                String serialNumber = "";
                                for (int i = 0; i < resourceChar.getLength(); i++) {
                                    Node resource = resourceChar.item(i);
                                    NodeList resourcesNodes = resource.getChildNodes();
                                    if (serialNumber != null && !serialNumber.equals("")) {
                                        break;
                                    }
                                    for (int j = 0; j < resourcesNodes.getLength(); j++) {
                                        Node resourcesNode = resourcesNodes.item(j);
                                        if (resourcesNode.getNodeName().split(":").length > 1 &&
                                            resourcesNode.getNodeName().split(":")[1].equals("name")) {
                                            String resourceName = resourcesNode.getTextContent();
                                            if (resourceName.contains("serialNumber")) {
                                                serialNumber = getImeiValue(resourcesNodes);
                                                if (serialNumber != null && !serialNumber.equals("")) {
                                                    isValid++;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                IMSI = serialNumber;
                            }

                            Node imsiNodes =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ICCID").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ICCID").item(0) : null;
                            if (imsiNodes != null) {
                                IMSI = imsiNodes.getTextContent();
                            }
                            String[] siebelData = readSiebelData(salesCode);

                            String rut = siebelData[0];
                            String clientName = siebelData[2];
                            String clientCity = siebelData[3];

                            salesDate = salesDate.split("\\.")[0].replace("-", "").replace("T", "").replace(":", "");

                            param = new String[] {
                                salesCode, contractCode, sucursalCode, rut, clientCode, clientName, clientCity, MSISDN,
                                IMSI, salesDate
                            };
                            if (isValid > 2) {
                                params.add(param);
                            }

                        }
                        xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() + File.separator +
                                                  "PROCESSED" + File.separator + xmlFile.getName()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return params;
    }

    public List<String[]> readEquiposPreFile(String path) {
        List<String[]> params = new ArrayList();
        try {
            File folder = new File(path);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setNamespaceAware(true);
            DocumentBuilder dBuilder;
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = null;
            String[] param = null;
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();

                String tipoRUC = "";

                for (File xmlFile : files) {
                    if (xmlFile.getName().contains("XML")) {
                        doc = dBuilder.parse(xmlFile);
                        doc.getDocumentElement().normalize();
                        String area =
                            doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                       "area").getLength() > 0 ?
                            doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                       "area").item(0).getTextContent() : "";
                        if (area.equalsIgnoreCase("venta")) {
                            String sucursalCode = "";
                            String clientCode = "";
                            String contractCode = "";
                            String MSISDN = "";
                            String IMEI = "";
                            String productOffer = "";
                            String salesDate = "";
                            Boolean isValid = false;
                            String precio = "";

                            String salesCode =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "shoppingCartID").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "shoppingCartID").item(0).getTextContent() : "";

                            salesDate =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "createdDate").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "createdDate").item(0).getTextContent() : "";
                            NodeList productSpecCharacteristics =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductSpecCharacteristic").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductSpecCharacteristic") : null;

                            if (productSpecCharacteristics != null) {

                                for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                                    Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                                    NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                                    for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                        Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                        if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                            productSpecCharacteristicNode.getNodeName().split(":")[1].equals("name") &&
                                            productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("contractId")) {
                                            contractCode = getContractCodeValue(productSpecCharacteristicNodes);
                                            if (!contractCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!contractCode.isEmpty()) {
                                        break;
                                    }
                                }

                                for (int i = 0; i < productSpecCharacteristics.getLength(); i++) {
                                    Node productSpecCharacteristic = productSpecCharacteristics.item(i);
                                    NodeList productSpecCharacteristicNodes = productSpecCharacteristic.getChildNodes();

                                    for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
                                        Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
                                        if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                                            productSpecCharacteristicNode.getNodeName().split(":")[1].equals("name") &&
                                            productSpecCharacteristicNode.getTextContent().equalsIgnoreCase("specificationSubSubtype")) {
                                            String value = getContractCodeValue(productSpecCharacteristicNodes);
                                            if (value.equalsIgnoreCase("cellphone")) {
                                                isValid = true;
                                                productOffer =
                                                    productSpecCharacteristicNode.getParentNode().getParentNode().getParentNode().getParentNode().getChildNodes().item(0).getTextContent();
                                                if (!productOffer.isEmpty()) {
                                                    break;
                                                }
                                            }
                                        }
                                        if (!productOffer.isEmpty()) {
                                            break;
                                        }
                                    }
                                }
                            }

                            NodeList salesChannels =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "SalesChannel").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "SalesChannel") : null;


                            if (salesChannels != null) {
                                for (int i = 0; i < salesChannels.getLength(); i++) {
                                    Node salesChannel = salesChannels.item(i);
                                    NodeList salesChannelNodes = salesChannel.getChildNodes();

                                    for (int j = 0; j < salesChannelNodes.getLength(); j++) {
                                        Node salesChannelNode = salesChannelNodes.item(j);
                                        if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                            salesChannelNode.getNodeName().split(":")[1].equals("orderCommercialChannel")) {
                                            sucursalCode = salesChannelNode.getTextContent();
                                        }
                                        if (salesChannelNode.getNodeName().split(":").length > 1 &&
                                            salesChannelNode.getNodeName().split(":")[1].equals("ID")) {
                                            if (!salesChannelNode.getTextContent().isEmpty()) {
                                                sucursalCode = salesChannelNode.getTextContent();
                                            }
                                            if (!sucursalCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!sucursalCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList productOfferingPrices =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductOfferingPrice").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "ProductOfferingPrice") : null;
                            if (productOfferingPrices != null) {
                                for (int i = 0; i < productOfferingPrices.getLength(); i++) {
                                    Node productOfferingPrice = productOfferingPrices.item(i);
                                    NodeList productOfferingPriceNodes = productOfferingPrice.getChildNodes();
                                    String taxCode = "tax";
                                    for (int j = 0; j < productOfferingPriceNodes.getLength(); j++) {
                                        Node productOfferingPriceNode = productOfferingPriceNodes.item(j);
                                        if (productOfferingPriceNode.getNodeName().split(":").length > 1 &&
                                            productOfferingPriceNode.getNodeName().split(":")[1].equals("taxCode")) {
                                            taxCode = productOfferingPriceNode.getTextContent();
                                        }
                                        if (taxCode == null || taxCode.equals("")) {
                                            precio = getPriceValue(productOfferingPriceNodes);
                                        }
                                    }
                                    if (!sucursalCode.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            NodeList customerAccounts =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "CustomerAccount").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "CustomerAccount") : null;

                            if (customerAccounts != null) {
                                for (int i = 0; i < customerAccounts.getLength(); i++) {
                                    Node customerAccount = customerAccounts.item(i);
                                    NodeList customerAccountNodes = customerAccount.getChildNodes();

                                    for (int j = 0; j < customerAccountNodes.getLength(); j++) {
                                        Node customerAccountNode = customerAccountNodes.item(j);
                                        if (customerAccountNode.getNodeName().split(":").length > 1 &&
                                            customerAccountNode.getNodeName().split(":")[1].equals("ID")) {
                                            clientCode = customerAccountNode.getTextContent();
                                            if (!clientCode.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                    if (!clientCode.isEmpty()) {
                                        break;
                                    }
                                }

                            }

                            NodeList msisdnNodes =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "MSISDN").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "MSISDN").item(0).getChildNodes() : null;

                            if (msisdnNodes != null) {
                                for (int j = 0; j < msisdnNodes.getLength(); j++) {
                                    Node msisdnNode = msisdnNodes.item(j);
                                    if (msisdnNode.getNodeName().split(":").length > 1 &&
                                        msisdnNode.getNodeName().split(":")[1].equals("SN")) {
                                        MSISDN = msisdnNode.getTextContent();
                                    }
                                }
                            }

                            NodeList resources =
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "resourceCharacteristics").getLength() > 0 ?
                                doc.getElementsByTagNameNS("http://www.entel.cl/CSM/RA/CHL/ODI/ODI/PublishProductOrder/v1",
                                                           "resourceCharacteristics") : null;

                            if (resources != null) {
                                for (int i = 0; i < resources.getLength(); i++) {
                                    Node resource = resources.item(i);
                                    NodeList resourcesNodes = resource.getChildNodes();
                                    for (int j = 0; j < resourcesNodes.getLength(); j++) {
                                        Node resourcesNode = resourcesNodes.item(j);
                                        if (resourcesNode.getNodeName().split(":").length > 1 &&
                                            resourcesNode.getNodeName().split(":")[1].equals("name")) {
                                            String resourceName = resourcesNode.getTextContent();
                                            if (resourceName.contains("imei")) {
                                                IMEI = getImeiValue(resourcesNodes);
                                                break;
                                            }
                                        }
                                    }
                                    if (!IMEI.isEmpty()) {
                                        break;
                                    }
                                }
                            }

                            String[] siebelData = readSiebelData(salesCode);

                            String rut = siebelData[0];
                            String clientName = siebelData[2];
                            String clientCity = siebelData[3];
                            String docType = siebelData[1];
                            if (IMEI == null || IMEI.equals("")) {
                                IMEI = siebelData[4];
                            }
                            String customerSince = siebelData[5];

                            if (contractCode.isEmpty()) {
                                contractCode = "NA";
                            }
                            salesDate = salesDate.split("\\.")[0].replace("-", "").replace("T", "").replace(":", "");

                            param = new String[] {
                                salesCode, contractCode, sucursalCode, rut, clientCode, clientName, clientCity, MSISDN,
                                IMEI, salesDate, productOffer, precio
                            };
                            if (isValid) {
                                params.add(param);
                            }
                        }
                        xmlFile.renameTo(new File(xmlFile.getParentFile().getParentFile().getPath() + File.separator +
                                                  "PROCESSED" + File.separator + xmlFile.getName()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return params;
    }

    public String getImeiValue(NodeList resourceCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < resourceCharacteristicNodes.getLength(); j++) {
            Node resourceCharacteristicNode = resourceCharacteristicNodes.item(j);
            if (resourceCharacteristicNode.getNodeName().split(":").length > 1 &&
                resourceCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = resourceCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getImsiValue(NodeList resourceCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < resourceCharacteristicNodes.getLength(); j++) {
            Node resourceCharacteristicNode = resourceCharacteristicNodes.item(j);
            if (resourceCharacteristicNode.getNodeName().split(":").length > 1 &&
                resourceCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = resourceCharacteristicNode.getTextContent();
            }
        }
        return value;
    }

    public String getContractCodeValue(NodeList productSpecCharacteristicNodes) {
        String value = "";
        for (int j = 0; j < productSpecCharacteristicNodes.getLength(); j++) {
            Node productSpecCharacteristicNode = productSpecCharacteristicNodes.item(j);
            if (productSpecCharacteristicNode.getNodeName().split(":").length > 1 &&
                productSpecCharacteristicNode.getNodeName().split(":")[1].equals("value")) {
                value = productSpecCharacteristicNode.getTextContent();
            }
        }
        return value;
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

    public String transformaFecha(String fechaXML, String formato) {

        if (!fechaXML.isEmpty()) {
            FastDateFormat formatoXML = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            FastDateFormat formatoEntrada = FastDateFormat.getInstance(formato);
            try {
                Date fechaXMLDate = formatoXML.parse(fechaXML);
                return formatoEntrada.format(fechaXMLDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        return "";
    }

    public String getConnectionURI() {
        return connectionURI;
    }

    public void setConnectionURI(String connectionURI) {
        this.connectionURI = connectionURI;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }
}
