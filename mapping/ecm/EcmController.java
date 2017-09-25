package mapping.ecm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import main.tde.java.Common;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class EcmController {

    private Common common;
    private static String sapFilePathOk = "";

    public EcmController() {
        super();
        common = new Common();
    }

    public String mappFileToXML() {
        String result = "";
        String sapFilePath = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\sap_file.txt";
        String ecmXMLPath = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\";

        List<String[]> params = new ArrayList();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/ecm.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\ecm.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                sapFilePath = prop.getProperty("sapFilePath");
                ecmXMLPath = prop.getProperty("ecmXMLPath");
                sapFilePathOk = prop.getProperty("sapFileOk");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        params = common.readSapFile(sapFilePath);

        File folder = new File(sapFilePath);
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            for (File file : files) {
                if (file.getName().contains("txt")) {
                    file.renameTo(new File(sapFilePathOk + file.getName()));
                }
            }
        }

        try {
            for (String[] sapParams : params) {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                // root elements
                Document doc = docBuilder.newDocument();
                Element rootElement = doc.createElement("externalInterfaces.accessoryImport:itemList");
                rootElement.setAttribute("xmlns:externalInterfaces.accessoryImport",
                                         "externalInterfaces.accessoryImport");
                rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
                rootElement.setAttribute("xsi:schemaLocation",
                                         "externalInterfaces.accessoryImport externalInterfaces.accessoryImport.xsd");
                doc.appendChild(rootElement);

                // itemHdrDesc elements
                Element item = doc.createElement("item");

                Element name = doc.createElement("name");
                name.appendChild(doc.createTextNode(sapParams[1]));
                item.appendChild(name);

                Element skuNumber = doc.createElement("skuNumber");
                skuNumber.appendChild(doc.createTextNode(sapParams[0]));
                item.appendChild(skuNumber);

                Element category = doc.createElement("category");
                category.appendChild(doc.createTextNode(sapParams[1]));
                item.appendChild(category);

                Element compatibleItem = doc.createElement("compatibleItem");
                item.appendChild(compatibleItem);

                Element price = doc.createElement("price");
                item.appendChild(price);

                rootElement.appendChild(item);
                Date actualDate = new Date();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                // write the content into xml file
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                DOMSource source = new DOMSource(doc);
                StreamResult streamResult =
                    new StreamResult(new File(ecmXMLPath + "SAP_ECM_".concat(sdf.format(actualDate)) + ".XML"));

                transformer.transform(source, streamResult);
            }

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
        return result;
    }

    public String mapAFileToXML(String origin) {
        String result = "";
        String sapFilePath = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\sap_file.txt";
        String ecmXMLPath = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\";
        List<String[]> params = new ArrayList();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/ecm.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\ecm.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                sapFilePath = prop.getProperty(origin);
                ecmXMLPath = prop.getProperty("ecmXMLPath");
                sapFilePathOk = prop.getProperty("sapFileOk");

            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        params = common.readSapFile(sapFilePath);

        File folder = new File(sapFilePath);
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            for (File file : files) {
                if (file.getName().contains("txt")) {
                    file.renameTo(new File(sapFilePathOk + file.getName()));
                }
            }
        }
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            // root elements
            Document doc = docBuilder.newDocument();
            //Element rootElement = doc.createElement("itemList");
            Element rootElement = doc.createElement("externalInterfaces.accessoryImport:itemList");
            rootElement.setAttribute("xmlns:externalInterfaces.accessoryImport", "externalInterfaces.accessoryImport");
            rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
            rootElement.setAttribute("xsi:schemaLocation",
                                     "externalInterfaces.accessoryImport externalInterfaces.accessoryImport.xsd");
            doc.appendChild(rootElement);
            for (String[] sapParams : params) {
                // itemHdrDesc elements
                Element item = doc.createElement("item");

                Element name = doc.createElement("name");
                name.appendChild(doc.createTextNode(sapParams[1]));
                item.appendChild(name);

                Element skuNumber = doc.createElement("skuNumber");
                skuNumber.appendChild(doc.createTextNode(sapParams[0]));
                item.appendChild(skuNumber);

                Element category = doc.createElement("category");
                category.appendChild(doc.createTextNode(sapParams[1]));
                item.appendChild(category);

                Element compatibleItem = doc.createElement("compatibleItem");
                item.appendChild(compatibleItem);

                Element price = doc.createElement("price");
                item.appendChild(price);

                rootElement.appendChild(item);

            }
            Date actualDate = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            //transformer.setOutputProperty("omit-xml-declaration", "yes");
            DOMSource source = new DOMSource(doc);
            StreamResult streamResult =
                new StreamResult(new File(ecmXMLPath + "SAP_ECM_".concat(sdf.format(actualDate)) + ".xml"));

            transformer.transform(source, streamResult);

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
        return result;
    }

}
