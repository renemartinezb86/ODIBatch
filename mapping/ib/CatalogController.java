package mapping.ib;

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

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import main.tde.java.Common;

public class CatalogController {

    private Common common;

    public CatalogController() {
        super();
        common = new Common();
    }

    public String ecmProductToCatalog() {
        String offeringXMLPath = "";
        String xCatalogPath = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/catalog.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\catalog.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                offeringXMLPath = prop.getProperty("offeringXMLPath");
                xCatalogPath = prop.getProperty("xCatalogPath");
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
        SimpleDateFormat sdfForFile = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat sdfForHeader = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss");
        String fileToWrite = "IB_PRODUCTOS_".concat(sdfForFile.format(actualDate).concat(".csv"));

        BufferedWriter bw = null;
        FileWriter fw = null;
        String line = "";
        try {
            fw = new FileWriter(xCatalogPath + fileToWrite);
            bw = new BufferedWriter(fw);

            List<String[]> params = common.readCatalogFile(offeringXMLPath);
            bw.write("C�digo Product Offer;Nombre Product Offer;Descripci�n Product Offer;SubFamily;SubType;Subtechnology;Nombre del equipo;Modelo del Equipo;C�digo Material del Equipo;Cargo Fijo Neto;Fecha de Creaci�n;Fecha de Actualizaci�n");
            bw.newLine();

            for (String[] param : params) {
                line = "";
                for (int i = 0; i < param.length; i++) {
                    line += param[i] + ";";
                }
                if (!line.isEmpty()) {
                    line = line.substring(0, line.length() - 1);
                }
                bw.write(line);
                bw.newLine();
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
