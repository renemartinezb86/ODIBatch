package mapping.rtd;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Types;

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

import mapping.ecm.EcmController;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class RtdController {
    private Common common;

    public RtdController() {
        super();
        common = new Common();
    }

    public String mappXmlToDb() {
        String result = "";
        String connectionURI = "jdbc:oracle:thin:@10.49.4.97:1530/OCRMD01.tdenopcl.internal";
        String user = "RTD";
        String pass = "RTD";
        String offeringXMLPath = "";
        String rtdXMLPath = "";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/rtd.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\rtd.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                offeringXMLPath = prop.getProperty("offeringXMLPath");
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
        List<String[]> params = common.readECMFile(offeringXMLPath);
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            for (String[] sapParams : params) {
                connection = DriverManager.getConnection(connectionURI, user, pass);
                String query =
                    "Insert into PRODUCTOS (ID_PRODUCT,CATEGORY,MARKETING_PRIORITY,NAME,POPULARITY_RANK,PRODUCT_LINE,TYPE,UNIT_PRICE,ID_PURCHASE,MODEL,MODEL_CATEGORY) values (?,?,?,?,?,?,?,?,?,?,?)";
                pstmt = connection.prepareStatement(query); // create a statement
                pstmt.setString(1, sapParams[1]);
                pstmt.setString(2, sapParams[3]);
                pstmt.setNull(3, Types.NVARCHAR);
                pstmt.setString(4, sapParams[8]);
                pstmt.setNull(5, Types.NVARCHAR);
                pstmt.setString(6, sapParams[2]);
                pstmt.setString(7, sapParams[6]);
                if (sapParams[9].equals("null")) {
                    pstmt.setNull(8, Types.FLOAT);
                } else {
                    pstmt.setString(8, sapParams[9].replace('.', ','));
                }
                pstmt.setNull(9, Types.NVARCHAR);
                pstmt.setString(10, sapParams[10]);
                pstmt.setString(11, sapParams[11]);
                pstmt.executeUpdate();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
