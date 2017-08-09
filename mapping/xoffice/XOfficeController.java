package mapping.xoffice;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import main.tde.java.Common;

public class XOfficeController {

    private Common common;

    public XOfficeController() {
        super();
        common = new Common();
    }

    public String mapSAPToXOffice(String origin) {
        String sapFilePath = "";
        String xOfficePath = "";
        String country = "CL";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/xoffice.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\xoffice.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                sapFilePath = prop.getProperty(origin);
                xOfficePath = prop.getProperty("sapxOfficePath");
                country = prop.getProperty("country");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        String result = "";
        int count = 0;
        Date actualDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
        String fileToWrite = "ITEM_ALL_STORE_".concat(sdfForFile.format(actualDate).concat(".mnt"));

        BufferedWriter bw = null;
        FileWriter fw = null;

        List<String[]> params = new ArrayList();

        try {
            params = common.readSapFile(sapFilePath);

            fw = new FileWriter(xOfficePath + fileToWrite);
            bw = new BufferedWriter(fw);

            bw.write("<Header line_count=\"".concat(String.valueOf(params.size()).concat("\" download_id=\"" +
                                                                                         sdfForFile.format(actualDate) +
                                                                                         "\" application_date=\"".concat(sdf.format(actualDate).concat("\" target_org_node=\"*:*\" deployment_name=\"*:*\" download_time=\"IMMEDIATE\" apply_immediately=\"true\"/>\n")))));

            for (String[] sapParams : params) {
                if (sapParams.length > 5 && sapParams[5].equalsIgnoreCase("true")) {
                    bw.write("INSERT|ITEM|" + sapParams[0] + "|ITEM|||STANDARD|" + sapParams[1] + "|" + sapParams[1] +
                             "|" + sapParams[2] + "||" + sapParams[3] + "|" + sapParams[4] +
                             "||||||||EA|||||||CL|||||||||||||||||||1|||||||||||||||||||||||||||||||||||||||*|*||||1|||||||\n");
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {

                ex.printStackTrace();
            }
        }
        return result = "";
    }

    public String mapASAPToXOffice() {
        String sapFilePath = "";
        String xOfficePath = "";
        String country = "CL";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/xoffice.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\xoffice.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                sapFilePath = prop.getProperty("sapFilePath");
                xOfficePath = prop.getProperty("xOfficePath");
                country = prop.getProperty("country");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        String result = "";
        int count = 0;
        Date actualDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
        String fileToWrite = "ITEM_ALL_STORE_".concat(sdfForFile.format(actualDate).concat(".mnt"));

        BufferedReader br = null;
        FileReader fr = null;
        BufferedWriter bw = null;
        FileWriter fw = null;

        try {
            fr = new FileReader(sapFilePath);
            br = new BufferedReader(fr);
            fw = new FileWriter(xOfficePath + fileToWrite);
            bw = new BufferedWriter(fw);

            String sCurrentLine;

            br = new BufferedReader(new FileReader(sapFilePath));
            bw.write("<Header line_count=\"".concat(String.valueOf(countLines(fr)).concat("\" download_id=\"ITEM-CODIGO\" application_date=\"".concat(sdf.format(actualDate).concat("\" target_org_node=\"*:*\" deployment_name=\"*:*\" download_time=\"IMMEDIATE\" apply_immediately=\"true\"/>\n")))));

            while ((sCurrentLine = br.readLine()) != null) {
                if (count > 0) {
                    if (sCurrentLine.length() > 0 && sCurrentLine.split("\\|")[5].equalsIgnoreCase("true")) {
                        String[] params = sCurrentLine.split("//|");
                        bw.write("INSERT|ITEM|" + params[0] + "|ITEM|||STANDARD|" + params[1] + "|" + params[1] +
                                 "|||||||||||EA|1||||||" + country +
                                 "||||||||||||||||||1|||||||||||||||||||||||||||||||||||||||*|*|||||||||||\n");
                    }
                } else {
                    count++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {

                ex.printStackTrace();
            }
        }
        return result = "";
    }

    public String mapXmlToXOffice() {
        String offeringXMLPath = "";
        String xOfficePath = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/xoffice.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\xoffice.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                offeringXMLPath = prop.getProperty("offeringXMLPath");
                xOfficePath = prop.getProperty("xOfficePath");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        String result = "";
        Date actualDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMdd_HHmmssSSS");
        SimpleDateFormat sdfForHeader = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
        String fileToWrite = "ITEM_ALL_STORE_".concat(sdfForFile.format(actualDate).concat(".mnt"));

        BufferedWriter bw = null;
        FileWriter fw = null;
        String line = "";
        try {
            fw = new FileWriter(xOfficePath + fileToWrite);
            bw = new BufferedWriter(fw);

            List<String[]> params = common.readECMFile(offeringXMLPath);
            bw.write("<Header line_count=\"".concat("1").concat("\" download_id=ITEM-\"".concat(sdfForHeader.format(actualDate)).concat("\" application_date=\"".concat(sdf.format(actualDate).concat("\" target_org_node=\"*:*\" deployment_name=\"Envio de articulos\" download_time=\"IMMEDIATE\" apply_immediately=\"true\"/>\n")))));

            for (String[] sapParams : params) {
                //Hasta family
                line = "INSERT|ITEM|" + sapParams[1] + "|ITEM|||STANDARD|" + sapParams[8] + "|" + sapParams[7] + "|1||";
                switch (sapParams[3]) {
                case "Add-on":
                    line += "10|";
                    break;
                case "Equipment":
                    line += "2|";
                    break;
                case "Product":
                    line += "12|";
                    break;
                }
                switch (sapParams[3]) {
                case "Add-on":
                    switch (sapParams[6]) {
                    case "Mix":
                        line += "6|";
                        break;
                    }
                    break;
                case "Equipment":
                    switch (sapParams[6]) {
                    case "Bundle":
                        line += "3|";
                        break;
                    case "Modem":
                        line += "3|";
                        break;
                    case "Cellphone":
                        line += "3|";
                        break;
                    case "Card":
                        line += "5|";
                        break;
                    case "Accessory":
                        line += "4|";
                        break;
                    }
                    break;
                case "Product":
                    switch (sapParams[6]) {
                    case "Delivery":
                        line += "13|";
                        break;
                    }
                    break;
                }
                //Hasta Tax Group ID
                if (sapParams[5].length() > 2) {
                    line += "|||||||EA|1||||||" + sapParams[5].substring(0, 2);
                } else {
                    line += "|||||||EA|1||||||" + "CL";
                }

                //Hasta Serialized Item Flag
                line += "|||||||||||||||||||";
                switch (sapParams[3]) {
                case "Equipment":
                    switch (sapParams[6]) {
                    case "Card":
                        line += "1|";
                        break;
                    case "Cellphone":
                        line += "1|";
                        break;
                    default:
                        line += "0|";
                        break;
                    }
                    break;
                }
                line += "||||||||||||||||||||||||||||||||||||||*|*|||||||||||\n";
                bw.write(line);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null)
                    bw.close();
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {

                ex.printStackTrace();
            }
        }
        return result = "";
    }

    private static Integer countLines(FileReader fr) {

        Integer count = -1;
        BufferedReader br = null;

        try {
            br = new BufferedReader(fr);
            while (br.readLine() != null) {
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {

                if (br != null)
                    br.close();

            } catch (IOException ex) {

                ex.printStackTrace();

            }

        }
        return count;
    }
}
