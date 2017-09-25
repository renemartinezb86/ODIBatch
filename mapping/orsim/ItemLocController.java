package mapping.orsim;

import com.retek.rib.app.messaging.service.RibMessageVO;
import com.retek.rib.binding.injector.impl.ApplicationMessageInjectorRemote;
import com.retek.rib.binding.injector.impl.ApplicationMessageInjectorRemoteHome;

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
import java.nio.file.Files;
import java.nio.file.Paths;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import java.sql.Connection;

import java.sql.DriverManager;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.rmi.PortableRemoteObject;

import main.tde.java.Common;

public class ItemLocController {
    private static String orsimXMLPath = "C:\\Users\\luism\\Desktop\\TF\\JDBClient2\\xml\\";
    private static String sapFilePath = "C:\\Users\\luism\\Desktop\\TF\\JDBClient2\\xml\\sap";
    private static String offeringXMLPath =
        "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\ECM_PRODUCTOFFERING.XML";
    private String connectionURI = "jdbc:oracle:thin:@10.49.4.137:1550/ESIMI01.tdenopcl.internal";
    private String user = "ENTEL_LECTURA";
    private String pass = "entel123";
    private String country = "CH";
    private Common common;
    private static String sapFilePathOk = "";
    private static String sapFilePathWar = "";
    private static String sapFilePathError = "";

    public ItemLocController() {
        super();
        common = new Common();
    }

    public String mapWFileToXml() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemloc.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\itemloc.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                sapFilePath = prop.getProperty("sapPFilePath");
                sapFilePathOk = prop.getProperty("sapPFileOk");
                sapFilePathError = prop.getProperty("sapFileError");
                sapFilePathWar = prop.getProperty("sapFileWar");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
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

        Connection connection = null;
        ArrayList<String[]> ids = new ArrayList<>();
        try {
            List<String[]> readSAP = common.readSapFile(sapFilePath);
            /*
            File folder = new File(sapFilePath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File file : files) {
                    if (file.getName().contains("txt")) {
                        file.renameTo(new File(sapFilePathOk + file.getName()));
                    }
                }
            }*/
            connection = common.getConnection();
            Statement stmt = null;
            String query = "select * from ETL_WAREHOUSE_V";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String ID = rs.getString("ID");                
                ids.add(new String[] { ID, "" });
            }

            //Only for offline connections.
            /*int[] localIds = new int[] {
                4, 9, 10, 25, 37, 43, 45, 48, 53, 84, 88, 91, 96, 108, 115, 131, 157, 161, 187, 214, 222, 223, 332, 339,
                350, 357, 395, 413, 435, 487, 524, 605, 681, 801, 896, 898, 917, 943, 1164, 1165, 1174, 1182, 1268,
                1270, 1275, 1280, 1295, 1296, 1312, 1342, 1366, 1412, 1424, 1425, 1426, 1427, 1428, 1434, 1437, 1446,
                1447, 1486, 1505, 1548, 1551, 1559, 1567, 1571, 1572, 1578, 1646, 1712, 1713, 1772, 1775, 1776, 1809,
                1824, 1830, 1843, 1859, 1875, 1876, 1877, 1884, 1992, 1996, 3043, 3105, 3239, 3258, 3263, 3264, 3265,
                3266, 3267, 3268, 3274, 3275, 3276, 3277, 3278, 3280, 3286, 3287, 3288, 3293, 3294, 3305, 3309, 3315,
                3318, 3321, 3325, 3327, 3328, 3329, 3361, 3363, 3516, 3524, 3593, 3609, 3667, 3680, 3688, 3704, 3715,
                3755, 3797, 3798, 3804, 3805, 3806, 3807, 3809, 3810, 3812, 3814, 3815, 3819, 3828, 3831, 3833, 3834,
                3835, 3838, 3841, 3846, 3852, 3856, 3857, 3863, 3892, 3917, 3922, 3957, 3958, 4013, 4026, 4063, 4064,
                4065, 4066, 4067, 4069, 4070, 4071, 4072, 4073, 4077, 4079, 4095, 4096, 4097, 4098, 4099, 4100, 4101,
                4102, 4103, 4104, 4105, 4106, 4107, 4110, 4111, 4112, 4113, 4114, 4115, 4116, 4117, 4118, 4120, 4121,
                4122, 4123, 4124, 4125, 4126, 4127, 4128, 4129, 4130, 4131, 4132, 4133, 4134, 4135, 4136, 4137, 4138,
                4140, 4144, 4146, 4147, 4148, 4149, 4154, 4155, 4156, 4157, 4158, 4159, 4160, 4161, 4162, 4163, 4164,
                4165, 4166, 4167, 4168, 4169, 4170, 4171, 4172, 4173, 4174, 4175, 4176, 4177, 4179, 4180, 4181, 4185,
                4186, 4187, 4188, 4189, 4190, 4191, 4192, 4193, 4194, 4195, 4197, 4198, 4199, 4200, 4201, 4281, 4294,
                4295, 4296, 4297, 4298, 4305, 4306, 4309, 4313, 4324, 4388, 4398, 4399, 4400, 4401, 4410, 4411
            };
            int[] localIds = new int[] { 43 };
            for (int i = 0; i < localIds.length; i++) {
                String ID = localIds[i] + "";
                String description = "";
                ids.add(new String[] { ID, description });
            }*/

            for (String[] strings : readSAP) {
                try {
                    String code = strings[0];
                    String name = strings[1];
                    String flag = strings[5];
                    String description = strings[1];
                    for (String[] id : ids) {
                        ArrayList<String[]> listIds = new ArrayList<>();
                        listIds.add(id);
                        Element doc = ItemLocController.generateXML(code, name, description, listIds, false);
                        printXML(doc, code + "_" + id[0]+"_W");

                        //EJB client integration.
                        odiInvokeEJBItemLoc(orsimXMLPath + code + "_" + id[0]+"_W" + ".XML",
                                            orsimXMLPath + code + "_" + id[0] + "RSP_W.XML", flag);
                    }

                } catch (ParserConfigurationException e) {
                    e.printStackTrace();
                } catch (TransformerException e) {
                    e.printStackTrace();
                }
            }


        } catch (Exception e) {

            e.printStackTrace();
            return "ERROR";

        }
        if (connection != null) {
        } else {
        }
        return result;
    }

    public String mapFileToXml() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemloc.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\itemloc.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                sapFilePath = prop.getProperty("sapPFilePath");
                sapFilePathOk = prop.getProperty("sapPFileOk");
                sapFilePathError = prop.getProperty("sapFileError");
                sapFilePathWar = prop.getProperty("sapFileWar");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
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

        Connection connection = null;
        ArrayList<String[]> ids = new ArrayList<>();
        try {
            List<String[]> readSAP = common.readSapFile(sapFilePath);
            File folder = new File(sapFilePath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File file : files) {
                    if (file.getName().contains("txt")) {
                        file.renameTo(new File(sapFilePathOk + file.getName()));
                    }
                }
            }
            connection = common.getConnection();
            Statement stmt = null;
            String query = "select * from ETL_STORE_V";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String ID = rs.getString("ID");
                String description = "";
                if (hasColumn(rs, "DESCRIPTION")) {
                    description = rs.getString("DESCRIPTION");
                }
                ids.add(new String[] { ID, description });
            }

            //Only for offline connections.
            /*int[] localIds = new int[] {
                4, 9, 10, 25, 37, 43, 45, 48, 53, 84, 88, 91, 96, 108, 115, 131, 157, 161, 187, 214, 222, 223, 332, 339,
                350, 357, 395, 413, 435, 487, 524, 605, 681, 801, 896, 898, 917, 943, 1164, 1165, 1174, 1182, 1268,
                1270, 1275, 1280, 1295, 1296, 1312, 1342, 1366, 1412, 1424, 1425, 1426, 1427, 1428, 1434, 1437, 1446,
                1447, 1486, 1505, 1548, 1551, 1559, 1567, 1571, 1572, 1578, 1646, 1712, 1713, 1772, 1775, 1776, 1809,
                1824, 1830, 1843, 1859, 1875, 1876, 1877, 1884, 1992, 1996, 3043, 3105, 3239, 3258, 3263, 3264, 3265,
                3266, 3267, 3268, 3274, 3275, 3276, 3277, 3278, 3280, 3286, 3287, 3288, 3293, 3294, 3305, 3309, 3315,
                3318, 3321, 3325, 3327, 3328, 3329, 3361, 3363, 3516, 3524, 3593, 3609, 3667, 3680, 3688, 3704, 3715,
                3755, 3797, 3798, 3804, 3805, 3806, 3807, 3809, 3810, 3812, 3814, 3815, 3819, 3828, 3831, 3833, 3834,
                3835, 3838, 3841, 3846, 3852, 3856, 3857, 3863, 3892, 3917, 3922, 3957, 3958, 4013, 4026, 4063, 4064,
                4065, 4066, 4067, 4069, 4070, 4071, 4072, 4073, 4077, 4079, 4095, 4096, 4097, 4098, 4099, 4100, 4101,
                4102, 4103, 4104, 4105, 4106, 4107, 4110, 4111, 4112, 4113, 4114, 4115, 4116, 4117, 4118, 4120, 4121,
                4122, 4123, 4124, 4125, 4126, 4127, 4128, 4129, 4130, 4131, 4132, 4133, 4134, 4135, 4136, 4137, 4138,
                4140, 4144, 4146, 4147, 4148, 4149, 4154, 4155, 4156, 4157, 4158, 4159, 4160, 4161, 4162, 4163, 4164,
                4165, 4166, 4167, 4168, 4169, 4170, 4171, 4172, 4173, 4174, 4175, 4176, 4177, 4179, 4180, 4181, 4185,
                4186, 4187, 4188, 4189, 4190, 4191, 4192, 4193, 4194, 4195, 4197, 4198, 4199, 4200, 4201, 4281, 4294,
                4295, 4296, 4297, 4298, 4305, 4306, 4309, 4313, 4324, 4388, 4398, 4399, 4400, 4401, 4410, 4411
            };
            int[] localIds = new int[] { 43 };
            for (int i = 0; i < localIds.length; i++) {
                String ID = localIds[i] + "";
                String description = "";
                ids.add(new String[] { ID, description });
            }*/

            for (String[] strings : readSAP) {
                try {
                    String code = strings[0];
                    String name = strings[1];
                    String flag = strings[5];
                    String description = strings[1];
                    for (String[] id : ids) {
                        ArrayList<String[]> listIds = new ArrayList<>();
                        listIds.add(id);
                        Element doc = ItemLocController.generateXML(code, name, description, listIds, true);
                        printXML(doc, code + "_" + id[0]);

                        //EJB client integration.
                        odiInvokeEJBItemLoc(orsimXMLPath + code + "_" + id[0] + ".XML",
                                            orsimXMLPath + code + "_" + id[0] + "RSP.XML", flag);
                    }

                } catch (ParserConfigurationException e) {
                    e.printStackTrace();
                } catch (TransformerException e) {
                    e.printStackTrace();
                }
            }


        } catch (Exception e) {

            e.printStackTrace();
            return "ERROR";

        }
        if (connection != null) {
        } else {
        }
        return result;
    }
    
    public String mapDBToXml() {
        String result = "";
        String EBSconnectionURI = "";
        String EBSuser = "";
        String EBSpass = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemloc.properties";
            //propFileName = "C:\\Users\\proyecto\\Documents\\Work\\ODI\\conf\\itemloc.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                sapFilePath = prop.getProperty("sapPFilePath");
                sapFilePathOk = prop.getProperty("sapPFileOk");
                sapFilePathError = prop.getProperty("sapFileError");
                sapFilePathWar = prop.getProperty("sapFileWar");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                EBSconnectionURI = prop.getProperty("EBSconnectionURI");
                EBSuser = prop.getProperty("EBSuser");
                EBSpass = prop.getProperty("EBSpass");
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }

        common = new Common(connectionURI, user, pass);

        Connection connection = null;
        ArrayList<String[]> ids = new ArrayList<>();
        try {
            List<String[]> readSAP = common.readEBSDB(EBSconnectionURI,EBSuser,EBSpass);

            File folder = new File(sapFilePath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File file : files) {
                    if (file.getName().contains("txt")) {
                        file.renameTo(new File(sapFilePathOk + file.getName()));
                    }
                }
            }
            connection = common.getConnection();
            Statement stmt = null;
            String query = "select * from ETL_STORE_V";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String ID = rs.getString("ID");
                String description = "";
                if (hasColumn(rs, "DESCRIPTION")) {
                    description = rs.getString("DESCRIPTION");
                }
                ids.add(new String[] { ID, description });
            }

            //Only for offline connections.
            /*int[] localIds = new int[] {
                4, 9, 10, 25, 37, 43, 45, 48, 53, 84, 88, 91, 96, 108, 115, 131, 157, 161, 187, 214, 222, 223, 332, 339,
                350, 357, 395, 413, 435, 487, 524, 605, 681, 801, 896, 898, 917, 943, 1164, 1165, 1174, 1182, 1268,
                1270, 1275, 1280, 1295, 1296, 1312, 1342, 1366, 1412, 1424, 1425, 1426, 1427, 1428, 1434, 1437, 1446,
                1447, 1486, 1505, 1548, 1551, 1559, 1567, 1571, 1572, 1578, 1646, 1712, 1713, 1772, 1775, 1776, 1809,
                1824, 1830, 1843, 1859, 1875, 1876, 1877, 1884, 1992, 1996, 3043, 3105, 3239, 3258, 3263, 3264, 3265,
                3266, 3267, 3268, 3274, 3275, 3276, 3277, 3278, 3280, 3286, 3287, 3288, 3293, 3294, 3305, 3309, 3315,
                3318, 3321, 3325, 3327, 3328, 3329, 3361, 3363, 3516, 3524, 3593, 3609, 3667, 3680, 3688, 3704, 3715,
                3755, 3797, 3798, 3804, 3805, 3806, 3807, 3809, 3810, 3812, 3814, 3815, 3819, 3828, 3831, 3833, 3834,
                3835, 3838, 3841, 3846, 3852, 3856, 3857, 3863, 3892, 3917, 3922, 3957, 3958, 4013, 4026, 4063, 4064,
                4065, 4066, 4067, 4069, 4070, 4071, 4072, 4073, 4077, 4079, 4095, 4096, 4097, 4098, 4099, 4100, 4101,
                4102, 4103, 4104, 4105, 4106, 4107, 4110, 4111, 4112, 4113, 4114, 4115, 4116, 4117, 4118, 4120, 4121,
                4122, 4123, 4124, 4125, 4126, 4127, 4128, 4129, 4130, 4131, 4132, 4133, 4134, 4135, 4136, 4137, 4138,
                4140, 4144, 4146, 4147, 4148, 4149, 4154, 4155, 4156, 4157, 4158, 4159, 4160, 4161, 4162, 4163, 4164,
                4165, 4166, 4167, 4168, 4169, 4170, 4171, 4172, 4173, 4174, 4175, 4176, 4177, 4179, 4180, 4181, 4185,
                4186, 4187, 4188, 4189, 4190, 4191, 4192, 4193, 4194, 4195, 4197, 4198, 4199, 4200, 4201, 4281, 4294,
                4295, 4296, 4297, 4298, 4305, 4306, 4309, 4313, 4324, 4388, 4398, 4399, 4400, 4401, 4410, 4411
            };
            int[] localIds = new int[] { 43 };
            for (int i = 0; i < localIds.length; i++) {
                String ID = localIds[i] + "";
                String description = "";
                ids.add(new String[] { ID, description });
            }*/

            for (String[] strings : readSAP) {
                try {
                    String code = strings[0];
                    String name = strings[1];
                    String flag = strings[5];
                    String description = strings[1];
                    for (String[] id : ids) {
                        ArrayList<String[]> listIds = new ArrayList<>();
                        listIds.add(id);
                        Element doc = ItemLocController.generateXML(code, name, description, listIds, true);
                        printXML(doc, code + "_" + id[0]);

                        //EJB client integration.
                        odiInvokeEJBItemLoc(orsimXMLPath + code + "_" + id[0] + ".XML",
                                            orsimXMLPath + code + "_" + id[0] + "RSP.XML", flag);
                    }

                } catch (ParserConfigurationException e) {
                    e.printStackTrace();
                } catch (TransformerException e) {
                    e.printStackTrace();
                }
            }


        } catch (Exception e) {

            e.printStackTrace();
            return "ERROR";

        }
        if (connection != null) {
        } else {
        }
        return result;
    }

    public String mapAFileToXml() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemloc.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\itemloc.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                sapFilePath = prop.getProperty("sapFilePath");
                sapFilePathOk = prop.getProperty("sapFileOk");
                sapFilePathError = prop.getProperty("sapFileError");
                sapFilePathWar = prop.getProperty("sapFileWar");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
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

        Connection connection = null;
        ArrayList<String[]> ids = new ArrayList<>();
        try {
            List<String[]> readSAP = common.readSapFile(sapFilePath);

            connection = common.getConnection();
            Statement stmt = null;
            String query = "select * from ETL_STORE_V";
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                String ID = rs.getString("ID");
                String description = "";
                if (hasColumn(rs, "DESCRIPTION")) {
                    description = rs.getString("DESCRIPTION");
                }
                ids.add(new String[] { ID, description });
            }

            //Only for offline connections.
            /*int[] localIds = new int[] {
                4, 9, 10, 25, 37, 43, 45, 48, 53, 84, 88, 91, 96, 108, 115, 131, 157, 161, 187, 214, 222, 223, 332, 339,
                350, 357, 395, 413, 435, 487, 524, 605, 681, 801, 896, 898, 917, 943, 1164, 1165, 1174, 1182, 1268,
                1270, 1275, 1280, 1295, 1296, 1312, 1342, 1366, 1412, 1424, 1425, 1426, 1427, 1428, 1434, 1437, 1446,
                1447, 1486, 1505, 1548, 1551, 1559, 1567, 1571, 1572, 1578, 1646, 1712, 1713, 1772, 1775, 1776, 1809,
                1824, 1830, 1843, 1859, 1875, 1876, 1877, 1884, 1992, 1996, 3043, 3105, 3239, 3258, 3263, 3264, 3265,
                3266, 3267, 3268, 3274, 3275, 3276, 3277, 3278, 3280, 3286, 3287, 3288, 3293, 3294, 3305, 3309, 3315,
                3318, 3321, 3325, 3327, 3328, 3329, 3361, 3363, 3516, 3524, 3593, 3609, 3667, 3680, 3688, 3704, 3715,
                3755, 3797, 3798, 3804, 3805, 3806, 3807, 3809, 3810, 3812, 3814, 3815, 3819, 3828, 3831, 3833, 3834,
                3835, 3838, 3841, 3846, 3852, 3856, 3857, 3863, 3892, 3917, 3922, 3957, 3958, 4013, 4026, 4063, 4064,
                4065, 4066, 4067, 4069, 4070, 4071, 4072, 4073, 4077, 4079, 4095, 4096, 4097, 4098, 4099, 4100, 4101,
                4102, 4103, 4104, 4105, 4106, 4107, 4110, 4111, 4112, 4113, 4114, 4115, 4116, 4117, 4118, 4120, 4121,
                4122, 4123, 4124, 4125, 4126, 4127, 4128, 4129, 4130, 4131, 4132, 4133, 4134, 4135, 4136, 4137, 4138,
                4140, 4144, 4146, 4147, 4148, 4149, 4154, 4155, 4156, 4157, 4158, 4159, 4160, 4161, 4162, 4163, 4164,
                4165, 4166, 4167, 4168, 4169, 4170, 4171, 4172, 4173, 4174, 4175, 4176, 4177, 4179, 4180, 4181, 4185,
                4186, 4187, 4188, 4189, 4190, 4191, 4192, 4193, 4194, 4195, 4197, 4198, 4199, 4200, 4201, 4281, 4294,
                4295, 4296, 4297, 4298, 4305, 4306, 4309, 4313, 4324, 4388, 4398, 4399, 4400, 4401, 4410, 4411
            };
            for (int i = 0; i < localIds.length; i++) {
                String ID = localIds[i] + "";
                String description = "";
                ids.add(new String[] { ID, description });
            }*/


            for (String[] strings : readSAP) {
                try {
                    String code = strings[0];
                    String name = strings[1];
                    String flag = strings[5];
                    String description = strings[1];
                    for (String[] id : ids) {
                        ArrayList<String[]> listIds = new ArrayList<>();
                        listIds.add(id);
                        Element doc = ItemLocController.generateAXML(code, name, description, listIds);
                        printXML(doc, code + "_" + id[0]);

                        //EJB client integration.
                        odiInvokeEJBItemLoc(orsimXMLPath + code + "_" + id[0] + ".XML",
                                            orsimXMLPath + code + "_" + id[0] + "RSP.XML", flag);
                    }

                } catch (ParserConfigurationException e) {
                    e.printStackTrace();
                } catch (TransformerException e) {
                    e.printStackTrace();
                }
            }


        } catch (Exception e) {

            e.printStackTrace();
            return "ERROR";
        }
        if (connection != null) {
        } else {
        }
        return result;
    }

    public String mapXmlToXml() {
        String result = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemloc.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\itemloc.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                offeringXMLPath = prop.getProperty("offeringXMLPath");
                sapFilePath = prop.getProperty("sapFilePath");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
                connectionURI = prop.getProperty("connectionURI");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Class.forName("oracle.jdbc.OracleDriver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return "ERROR";
        }

        Connection connection = null;
        try {
            List<String[]> readSAP = common.readECMFile(offeringXMLPath);
            ArrayList<String[]> ids = new ArrayList<>();
            /*connection = DriverManager.getConnection(connectionURI, user, pass);
            Statement stmt = null;
            String query = "select * from ETL_STORE_V";

            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String ID = rs.getString("ID");
                ids.add(new String[] { ID });
            }*/

            int[] localIds = new int[] {
                4, 9, 10, 25, 37, 43, 45, 48, 53, 84, 88, 91, 96, 108, 115, 131, 157, 161, 187, 214, 222, 223, 332, 339,
                350, 357, 395, 413, 435, 487, 524, 605, 681, 801, 896, 898, 917, 943, 1164, 1165, 1174, 1182, 1268,
                1270, 1275, 1280, 1295, 1296, 1312, 1342, 1366, 1412, 1424, 1425, 1426, 1427, 1428, 1434, 1437, 1446,
                1447, 1486, 1505, 1548, 1551, 1559, 1567, 1571, 1572, 1578, 1646, 1712, 1713, 1772, 1775, 1776, 1809,
                1824, 1830, 1843, 1859, 1875, 1876, 1877, 1884, 1992, 1996, 3043, 3105, 3239, 3258, 3263, 3264, 3265,
                3266, 3267, 3268, 3274, 3275, 3276, 3277, 3278, 3280, 3286, 3287, 3288, 3293, 3294, 3305, 3309, 3315,
                3318, 3321, 3325, 3327, 3328, 3329, 3361, 3363, 3516, 3524, 3593, 3609, 3667, 3680, 3688, 3704, 3715,
                3755, 3797, 3798, 3804, 3805, 3806, 3807, 3809, 3810, 3812, 3814, 3815, 3819, 3828, 3831, 3833, 3834,
                3835, 3838, 3841, 3846, 3852, 3856, 3857, 3863, 3892, 3917, 3922, 3957, 3958, 4013, 4026, 4063, 4064,
                4065, 4066, 4067, 4069, 4070, 4071, 4072, 4073, 4077, 4079, 4095, 4096, 4097, 4098, 4099, 4100, 4101,
                4102, 4103, 4104, 4105, 4106, 4107, 4110, 4111, 4112, 4113, 4114, 4115, 4116, 4117, 4118, 4120, 4121,
                4122, 4123, 4124, 4125, 4126, 4127, 4128, 4129, 4130, 4131, 4132, 4133, 4134, 4135, 4136, 4137, 4138,
                4140, 4144, 4146, 4147, 4148, 4149, 4154, 4155, 4156, 4157, 4158, 4159, 4160, 4161, 4162, 4163, 4164,
                4165, 4166, 4167, 4168, 4169, 4170, 4171, 4172, 4173, 4174, 4175, 4176, 4177, 4179, 4180, 4181, 4185,
                4186, 4187, 4188, 4189, 4190, 4191, 4192, 4193, 4194, 4195, 4197, 4198, 4199, 4200, 4201, 4281, 4294,
                4295, 4296, 4297, 4298, 4305, 4306, 4309, 4313, 4324, 4388, 4398, 4399, 4400, 4401, 4410, 4411
            };
            for (int i = 0; i < localIds.length; i++) {
                String ID = localIds[i] + "";
                String description = "";
                ids.add(new String[] { ID, description });
            }

            for (String[] strings : readSAP) {
                try {
                    String code = strings[1];
                    String name = strings[8];
                    String flag = strings[5];
                    String description = strings[7];
                    for (String[] id : ids) {
                        ArrayList<String[]> listIds = new ArrayList<>();
                        listIds.add(id);
                        Element doc = ItemLocController.generateXML(code, name, description, listIds, true);
                        printXML(doc, code + "_" + id[0]);

                        //EJB client integration.
                        odiInvokeEJBItemLoc(orsimXMLPath + code + "_" + id[0] + ".XML",
                                            orsimXMLPath + code + "_" + id[0] + "RSP.XML", flag);
                    }
                } catch (ParserConfigurationException e) {
                    e.printStackTrace();
                } catch (TransformerException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
            return "ERROR";
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

    private static List<String[]> readSAP() {
        List<String[]> sapParams = new ArrayList<>();
        int count = 0;
        String sCurrentLine;
        FileInputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;

        try {
            fileInputStream = new FileInputStream(sapFilePath);
            inputStreamReader = new InputStreamReader(fileInputStream);
            bufferedReader = new BufferedReader(inputStreamReader);
            while ((sCurrentLine = bufferedReader.readLine()) != null) {
                if (count > 0) {
                    sapParams.add(sCurrentLine.split("\\|"));
                } else {
                    count++;
                }
            }
            bufferedReader.close();
            inputStreamReader.close();
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sapParams;
    }

    private static void printXML(Element doc, String name) throws TransformerException {
        // write the content into xml file
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        transformer.setOutputProperty("omit-xml-declaration", "yes");
        DOMSource source = new DOMSource(doc);
        StreamResult streamResult = new StreamResult(new File(orsimXMLPath + "" + name + ".XML"));

        StreamResult result = new StreamResult(System.out);

        transformer.transform(source, streamResult);
    }

    public static Element generateXML(String codeSap, String name, String description,
                                      ArrayList<String[]> ids,boolean store) throws ParserConfigurationException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        // root elements
        Document doc = docBuilder.newDocument();
        Element rootElement = doc.createElement("ItemLocDesc");
        rootElement.setAttribute("xmlns", "http://www.oracle.com/retail/integration/base/bo/ItemLocDesc/v1");
        rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
        rootElement.setAttribute("xsi:schemaLocation",
                                 "http://www.oracle.com/retail/integration/base/bo/ItemLocDesc/v1 http://www.oracle.com/retail/integration/base/bo/ItemLocDesc/v1/ItemLocDesc.xsd ");

        //item
        Element item = doc.createElement("item");
        item.appendChild(doc.createTextNode(codeSap));
        rootElement.appendChild(item);

        //ItemLocPhys
        for (String[] s : ids) {
            Element itemLocPhys = getItemLocPhys(doc, s, name, description, store);
            rootElement.appendChild(itemLocPhys);
        }

        doc.appendChild(rootElement);
        return rootElement;
    }


    public static Element generateAXML(String codeSap, String name, String description,
                                       ArrayList<String[]> ids) throws ParserConfigurationException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        // root elements
        Document doc = docBuilder.newDocument();
        Element rootElement = doc.createElement("ItemLocDesc");
        rootElement.setAttribute("xmlns", "http://www.oracle.com/retail/integration/base/bo/ItemLocDesc/v1");
        rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
        rootElement.setAttribute("xsi:schemaLocation",
                                 "http://www.oracle.com/retail/integration/base/bo/ItemLocDesc/v1 http://www.oracle.com/retail/integration/base/bo/ItemLocDesc/v1/ItemLocDesc.xsd ");

        //item
        Element item = doc.createElement("item");
        item.appendChild(doc.createTextNode(codeSap));
        rootElement.appendChild(item);

        //ItemLocPhys
        for (String[] s : ids) {
            Element itemLocPhys = getAItemLocPhys(doc, s, name, description);
            rootElement.appendChild(itemLocPhys);
        }

        doc.appendChild(rootElement);
        return rootElement;
    }

    private static Element getItemLocVirt(String id, String name, String description, Document doc, boolean store) {
        Element ItemLocVirt = doc.createElement("ItemLocVirt");

        //loc
        Element loc = doc.createElement("loc");
        loc.appendChild(doc.createTextNode(id));
        ItemLocVirt.appendChild(loc);

        //loc_type2
        Element loc_type2 = doc.createElement("loc_type");
        loc_type2.appendChild(store ? doc.createTextNode("S") : doc.createTextNode("W"));
        ItemLocVirt.appendChild(loc_type2);

        //local_item_desc
        Element local_item_desc = doc.createElement("local_item_desc");
        local_item_desc.appendChild(doc.createTextNode(description));
        ItemLocVirt.appendChild(local_item_desc);

        //local_short_desc
        Element local_short_desc = doc.createElement("local_short_desc");
        local_short_desc.appendChild(doc.createTextNode(name));
        ItemLocVirt.appendChild(local_short_desc);

        //status
        Element status = doc.createElement("status");
        status.appendChild(doc.createTextNode("A"));
        ItemLocVirt.appendChild(status);

        //primary_supp
        Element primary_supp = doc.createElement("primary_supp");
        primary_supp.appendChild(doc.createTextNode("1000"));
        ItemLocVirt.appendChild(primary_supp);

        //taxable_ind
        Element taxable_ind = doc.createElement("taxable_ind");
        taxable_ind.appendChild(doc.createTextNode("Y"));
        ItemLocVirt.appendChild(taxable_ind);

        //selling_unit_retail
        Element selling_unit_retail = doc.createElement("selling_unit_retail");
        selling_unit_retail.appendChild(doc.createTextNode("1"));
        ItemLocVirt.appendChild(selling_unit_retail);

        //selling_uom
        Element selling_uom = doc.createElement("selling_uom");
        selling_uom.appendChild(doc.createTextNode("EA"));
        ItemLocVirt.appendChild(selling_uom);

        //store_price_ind
        Element store_price_ind = doc.createElement("store_price_ind");
        store_price_ind.appendChild(doc.createTextNode("N"));
        ItemLocVirt.appendChild(store_price_ind);

        //purchase_type
        Element purchase_type = doc.createElement("purchase_type");
        purchase_type.appendChild(doc.createTextNode("N"));
        ItemLocVirt.appendChild(purchase_type);

        //uin_type
        Element uin_type = doc.createElement("uin_type");
        uin_type.appendChild(doc.createTextNode("1"));
        ItemLocVirt.appendChild(uin_type);

        //uin_label
        Element uin_label = doc.createElement("uin_label");
        uin_label.appendChild(doc.createTextNode("IMEI"));
        ItemLocVirt.appendChild(uin_label);

        //capture_time
        Element capture_time = doc.createElement("capture_time");
        capture_time.appendChild(doc.createTextNode("SR"));
        ItemLocVirt.appendChild(capture_time);

        //ext_uin_ind
        Element ext_uin_ind = doc.createElement("ext_uin_ind");
        ext_uin_ind.appendChild(doc.createTextNode("N"));
        ItemLocVirt.appendChild(ext_uin_ind);

        //ranged_ind
        Element ranged_ind = doc.createElement("ranged_ind");
        ranged_ind.appendChild(doc.createTextNode("Y"));
        ItemLocVirt.appendChild(ranged_ind);

        return ItemLocVirt;
    }

    private static Element getAItemLocVirt(String id, String name, String description, Document doc) {
        Element ItemLocVirt = doc.createElement("ItemLocVirt");

        //loc
        Element loc = doc.createElement("loc");
        loc.appendChild(doc.createTextNode(id));
        ItemLocVirt.appendChild(loc);

        //loc_type2
        Element loc_type2 = doc.createElement("loc_type");
        loc_type2.appendChild(doc.createTextNode("S"));
        ItemLocVirt.appendChild(loc_type2);

        //local_item_desc
        Element local_item_desc = doc.createElement("local_item_desc");
        local_item_desc.appendChild(doc.createTextNode(description));
        ItemLocVirt.appendChild(local_item_desc);

        //local_short_desc
        Element local_short_desc = doc.createElement("local_short_desc");
        local_short_desc.appendChild(doc.createTextNode(name));
        ItemLocVirt.appendChild(local_short_desc);

        //status
        Element status = doc.createElement("status");
        status.appendChild(doc.createTextNode("A"));
        ItemLocVirt.appendChild(status);

        //primary_supp
        Element primary_supp = doc.createElement("primary_supp");
        primary_supp.appendChild(doc.createTextNode("1000"));
        ItemLocVirt.appendChild(primary_supp);

        //taxable_ind
        Element taxable_ind = doc.createElement("taxable_ind");
        taxable_ind.appendChild(doc.createTextNode("Y"));
        ItemLocVirt.appendChild(taxable_ind);

        //selling_unit_retail
        Element selling_unit_retail = doc.createElement("selling_unit_retail");
        selling_unit_retail.appendChild(doc.createTextNode("1"));
        ItemLocVirt.appendChild(selling_unit_retail);

        //selling_uom
        Element selling_uom = doc.createElement("selling_uom");
        selling_uom.appendChild(doc.createTextNode("EA"));
        ItemLocVirt.appendChild(selling_uom);

        //store_price_ind
        Element store_price_ind = doc.createElement("store_price_ind");
        store_price_ind.appendChild(doc.createTextNode("N"));
        ItemLocVirt.appendChild(store_price_ind);

        //purchase_type
        Element purchase_type = doc.createElement("purchase_type");
        purchase_type.appendChild(doc.createTextNode("N"));
        ItemLocVirt.appendChild(purchase_type);

        //ranged_ind
        Element ranged_ind = doc.createElement("ranged_ind");
        ranged_ind.appendChild(doc.createTextNode("Y"));
        ItemLocVirt.appendChild(ranged_ind);

        return ItemLocVirt;
    }


    public static Element getItemLocPhys(Document doc, String[] id, String name, String description, boolean store) {
        Element itemLocPhys = doc.createElement("ItemLocPhys");

        //physical_loc
        Element physical_loc = doc.createElement("physical_loc");
        physical_loc.appendChild(doc.createTextNode(id[0]));
        itemLocPhys.appendChild(physical_loc);

        //loc_type
        Element loc_type = doc.createElement("loc_type");
        loc_type.appendChild(store ? doc.createTextNode("S") : doc.createTextNode("W"));
        itemLocPhys.appendChild(loc_type);

        //store_type
        Element store_type = doc.createElement("store_type");
        store_type.appendChild(doc.createTextNode("C"));
        itemLocPhys.appendChild(store_type);

        //stockholding_ind
        Element stockholding_ind = doc.createElement("stockholding_ind");
        stockholding_ind.appendChild(doc.createTextNode("Y"));
        itemLocPhys.appendChild(stockholding_ind);

        //ItemLocVirt
        Element ItemLocVirt = getItemLocVirt(id[0], name, description, doc, store);

        itemLocPhys.appendChild(ItemLocVirt);
        return itemLocPhys;
    }


    public static Element getAItemLocPhys(Document doc, String[] id, String name, String description) {
        Element itemLocPhys = doc.createElement("ItemLocPhys");

        //physical_loc
        Element physical_loc = doc.createElement("physical_loc");
        physical_loc.appendChild(doc.createTextNode(id[0]));
        itemLocPhys.appendChild(physical_loc);

        //loc_type
        Element loc_type = doc.createElement("loc_type");
        loc_type.appendChild(doc.createTextNode("S"));
        itemLocPhys.appendChild(loc_type);

        //store_type
        Element store_type = doc.createElement("store_type");
        store_type.appendChild(doc.createTextNode("C"));
        itemLocPhys.appendChild(store_type);

        //stockholding_ind
        Element stockholding_ind = doc.createElement("stockholding_ind");
        stockholding_ind.appendChild(doc.createTextNode("Y"));
        itemLocPhys.appendChild(stockholding_ind);

        //ItemLocVirt
        Element ItemLocVirt = getAItemLocVirt(id[0], name, description, doc);

        itemLocPhys.appendChild(ItemLocVirt);
        return itemLocPhys;
    }


    public String odiInvokeEJBItemLoc(String reqPath, String rspPath, String flag) {
        String result = "";
        String user = "entSIMuser";
        String pass = "ents1mUSER";
        String ctx = "weblogic.jndi.WLInitialContextFactory";
        String url = "t3://10.49.4.125:7010";
        String type = "ITEMCRE";
        String family = "ITEMS";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/config.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\config.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                ctx = prop.getProperty("ctx");
                url = prop.getProperty("url");
                type = prop.getProperty("locType");
                family = prop.getProperty("locFamily");
            }
            byte[] encoded;
            String xml = "";
            File folder = new File(reqPath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();
                for (File fXmlFile : files) {
                    encoded = Files.readAllBytes(Paths.get(fXmlFile.getPath()));
                    xml = new String(encoded, StandardCharsets.UTF_8);
                    result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
                    Files.write(Paths.get(rspPath), Arrays.asList(new String[] { result }), StandardCharsets.UTF_8);
                }
            } else {
                encoded = Files.readAllBytes(Paths.get(reqPath));
                xml = new String(encoded, StandardCharsets.UTF_8);
                if (flag.equalsIgnoreCase("true")) {
                    result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
                } else {
                    result = staticInvokeEJB(ctx, url, user, pass, family, "ITEMLOCMOD", xml);
                }
                Files.write(Paths.get(rspPath), Arrays.asList(new String[] { result }), StandardCharsets.UTF_8);
            }

        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            try {
                Files.write(Paths.get(rspPath),
                            Arrays.asList(new String[] { "Result: ERROR, Descripcion: ".concat(e.getMessage()) }),
                            StandardCharsets.UTF_8);
            } catch (IOException f) {
                f.printStackTrace();
            }
        }
        return result;
    }

    public String staticInvokeEJB(String pContext, String pUrl, String pUser, String pPass, String family, String type,
                                  String xml) {
        Properties prop = new Properties();
        prop.put("java.naming.factory.initial", pContext);
        prop.put("java.naming.provider.url", pUrl);
        prop.put("java.naming.security.principal", pUser);
        prop.put("java.naming.security.credentials", pPass);
        String className = "";
        try {
            System.out.println("InitialContext");
            Context ctx = new InitialContext(prop);
            System.out.println("injector");
            Object injector = ctx.lookup("com/retek/rib/binding/injector/ApplicationMessageInjector");
            className = injector.getClass().toString();
            System.out.println("ApplicationMessageInjectorRemoteHome");
            ApplicationMessageInjectorRemoteHome injectorEJBHome =
                (ApplicationMessageInjectorRemoteHome) PortableRemoteObject.narrow(injector,
                                                                                   ApplicationMessageInjectorRemoteHome.class);
            System.out.println("injectorEJBRemote");
            ApplicationMessageInjectorRemote injectorEJBRemote = injectorEJBHome.create();
            System.out.println("ribMessagesMessageVOs");
            RibMessageVO[] ribMessagesMessageVOs = new RibMessageVO[1];
            System.out.println("ribMessageVO");
            RibMessageVO ribMessageVO = new RibMessageVO();
            ribMessageVO.setFamily(family);
            ribMessageVO.setType(type);
            ribMessageVO.setPayload(null);
            ribMessageVO.setPayloadXml(xml);
            ribMessagesMessageVOs[0] = ribMessageVO;
            System.out.println("inject");
            injectorEJBRemote.inject(ribMessagesMessageVOs);
            return "Result: OK, Descripcion: Ejecucion exitosa";
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "Result: ERROR, Descripcion: " + e.getMessage();
        }
    }


}

