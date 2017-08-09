package t3.simple.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.retek.rib.app.messaging.service.RibMessageVO;
import com.retek.rib.binding.injector.impl.ApplicationMessageInjectorRemote;
import com.retek.rib.binding.injector.impl.ApplicationMessageInjectorRemoteHome;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.Arrays;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.rmi.PortableRemoteObject;

public class T3Client {

    public T3Client() {
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

    public String odiInvokeEJBItemCre() {
        String result = "";
        String reqPath = "/home/odi/.odi/oracledi/files/ITEMS_ITEMCRE.XML";
        String rspPath = "/home/odi/.odi/oracledi/files/ITEMCRE_RSP.xml";
        //String reqPath = "/home/odi/.odi/oracledi/files/ITEMLOC_ITEMLOCCRE.XML";
        //String rspPath = "/home/odi/.odi/oracledi/files/ITEMLOCCRE_RSP.xml";
        String user = "entSIMuser";
        String pass = "ents1mUSER";
        String ctx = "weblogic.jndi.WLInitialContextFactory";
        String url = "t3://10.49.4.125:7010";
        String type = "ITEMCRE";
        String family = "ITEMS";
        //String type = "ITEMLOCCRE";
        //String family = "ITEMLOC";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/config.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\config.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                reqPath = prop.getProperty("creReqPath");
                rspPath = prop.getProperty("creRspPath");
                //reqPath = prop.getProperty("locReqPath");
                //rspPath = prop.getProperty("locRspPath");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                ctx = prop.getProperty("ctx");
                url = prop.getProperty("url");
                type = prop.getProperty("creType");
                family = prop.getProperty("creFamily");
                //type = prop.getProperty("locType");
                //family = prop.getProperty("locFamily");
            }
            byte[] encoded;
            String xml = "";
            File folder = new File(reqPath);
            if (folder.isDirectory()) {
                File[] files = folder.listFiles();

                String tipoRUC = "";

                for (File fXmlFile : files) {
                    encoded = Files.readAllBytes(Paths.get(fXmlFile.getPath()));
                    xml = new String(encoded, StandardCharsets.UTF_8);
                    result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
                    Files.write(Paths.get(rspPath), Arrays.asList(new String[] { result }), StandardCharsets.UTF_8,
                                StandardOpenOption.APPEND);
                }
            } else {
                encoded = Files.readAllBytes(Paths.get(reqPath));
                xml = new String(encoded, StandardCharsets.UTF_8);
                result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
                Files.write(Paths.get(rspPath), Arrays.asList(new String[] { result }), StandardCharsets.UTF_8,
                            StandardOpenOption.APPEND);
            }


        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            try {
                Files.write(Paths.get(rspPath),
                            Arrays.asList(new String[] { "Result: ERROR, Descripcion: ".concat(e.getMessage()) }),
                            StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            } catch (IOException f) {
                f.printStackTrace();
            }
        }
        return result;
    }

    public String odiInvokeEJBItemLoc() {
        String result = "";
        //String reqPath = "/home/odi/.odi/oracledi/files/ITEMS_ITEMCRE.XML";
        //String rspPath = "/home/odi/.odi/oracledi/files/ITEMCRE_RSP.xml";
        String reqPath = "/home/odi/.odi/oracledi/files/ITEMLOC_ITEMLOCCRE.XML";
        String rspPath = "/home/odi/.odi/oracledi/files/ITEMLOCCRE_RSP.xml";
        /*String path =
            "C:\\Users\\Tutorial\\Documents\\Work\\Bluu\\Proyecto\\ODI\\ITEMS_ITEMCRE\\ITEMS_ITEMCRE\\ITEMS_ITEMCRE.XML";
        String rspPath =
            "C:\\Users\\Tutorial\\Documents\\Work\\Bluu\\Proyecto\\ODI\\ITEMS_ITEMCRE\\ITEMS_ITEMCRE\\response.xml";*/
        String user = "entSIMuser";
        String pass = "ents1mUSER";
        String ctx = "weblogic.jndi.WLInitialContextFactory";
        String url = "t3://10.49.4.125:7010";
        String type = "ITEMCRE";
        String family = "ITEMS";
        //String type = "ITEMLOCCRE";
        //String family = "ITEMLOC";
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/config.properties";
            //propFileName = "D:\\Work\\ODI\\conf\\config.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                //reqPath = prop.getProperty("creReqPath");
                //rspPath = prop.getProperty("creRspPath");
                reqPath = prop.getProperty("locReqPath");
                rspPath = prop.getProperty("locRspPath");
                user = prop.getProperty("user");
                pass = prop.getProperty("pass");
                ctx = prop.getProperty("ctx");
                url = prop.getProperty("url");
                //type = prop.getProperty("creType");
                //family = prop.getProperty("creFamily");
                type = prop.getProperty("locType");
                family = prop.getProperty("locFamily");
            }

            byte[] encoded;
            String xml = "";

            if (reqPath.contains(",")) {
                String[] reqs = reqPath.split(",");
                for (int i = 0; i < reqs.length; i++) {
                    String req = reqs[i];
                    encoded = Files.readAllBytes(Paths.get(req));
                    xml = new String(encoded, StandardCharsets.UTF_8);
                    result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
                    Files.write(Paths.get(rspPath), Arrays.asList(new String[] { result }), StandardCharsets.UTF_8,
                                StandardOpenOption.APPEND);
                }
            } else {
                encoded = Files.readAllBytes(Paths.get(reqPath));
                xml = new String(encoded, StandardCharsets.UTF_8);
                result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
                Files.write(Paths.get(rspPath), Arrays.asList(new String[] { result }), StandardCharsets.UTF_8,
                            StandardOpenOption.APPEND);
            }

        } catch (IOException e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            try {
                Files.write(Paths.get(rspPath),
                            Arrays.asList(new String[] { "Result: ERROR, Descripcion: ".concat(e.getMessage()) }),
                            StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            } catch (IOException f) {
                f.printStackTrace();
            }
        }
        return result;
    }

    public static void main(String[] args) {
        //String result = mockInvokeEJB();
        T3Client eJBClient = new T3Client();
        String result = "";
        if (args.length > 0) {
            switch (args[0]) {
            case "-l":
                result = eJBClient.odiInvokeEJBItemLoc();
                break;
            case "-c":
                result = eJBClient.odiInvokeEJBItemCre();
                break;
            default:
                result = eJBClient.odiInvokeEJBItemCre();
                break;
            }
        } else {
            //result = eJBClient.odiInvokeEJBItemCre();
            result = eJBClient.odiInvokeEJBItemLoc();
        }
        System.out.println("-------------");
        System.out.println(result);
        System.out.println("-------------");
    }
}
