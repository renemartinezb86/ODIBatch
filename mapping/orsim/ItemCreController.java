package mapping.orsim;

import com.retek.rib.app.messaging.service.RibMessageVO;
import com.retek.rib.binding.injector.impl.ApplicationMessageInjectorRemote;
import com.retek.rib.binding.injector.impl.ApplicationMessageInjectorRemoteHome;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.File;

import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.rmi.PortableRemoteObject;

import javax.xml.transform.OutputKeys;

import main.tde.java.Common;

public class ItemCreController {
    private Common common;

    public ItemCreController() {
        super();
        common = new Common();
    }

    public String mapAFileToXml() {
        String result = "";
        String country = "CH";
        String sapFilePath = "C:\\JDeveloper\\WebClient\\\\SimpleORSIMIntegration\\deploy\\sap_file.txt";
        String orsimXMLPath = "C:\\JDeveloper\\WebClient\\\\SimpleORSIMIntegration\\deploy\\ITEMS_ITEMCRE";
        List<String[]> params = new ArrayList();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemcre.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\itemcre.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                sapFilePath = prop.getProperty("sapFilePath");
                orsimXMLPath = prop.getProperty("orsimXMLPath");

                params = common.readSapFile(sapFilePath);

            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        try {
            for (String[] sapParams : params) {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                // root elements
                Document doc = docBuilder.newDocument();
                Element rootElement = doc.createElement("ItemDesc");
                rootElement.setAttribute("xmlns", "http://www.oracle.com/retail/integration/base/bo/ItemDesc/v1");
                rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
                doc.appendChild(rootElement);

                // itemHdrDesc elements
                Element itemHdrDesc = doc.createElement("ns1:ItemHdrDesc");
                itemHdrDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemHdrDesc/v1");

                Element item = doc.createElement("ns1:item");
                item.appendChild(doc.createTextNode(sapParams[0]));
                itemHdrDesc.appendChild(item);

                Element item_number_type = doc.createElement("ns1:item_number_type");
                item_number_type.appendChild(doc.createTextNode("0"));
                itemHdrDesc.appendChild(item_number_type);

                Element pack_ind = doc.createElement("ns1:pack_ind");
                pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(pack_ind);

                Element item_level = doc.createElement("ns1:item_level");
                item_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(item_level);

                Element tran_level = doc.createElement("ns1:tran_level");
                tran_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(tran_level);

                Element dept = doc.createElement("ns1:dept");
                dept.appendChild(doc.createTextNode(sapParams[2]));
                itemHdrDesc.appendChild(dept);

                Element eclass = doc.createElement("ns1:class");
                eclass.appendChild(doc.createTextNode(sapParams[3]));
                itemHdrDesc.appendChild(eclass);

                Element subclass = doc.createElement("ns1:subclass");
                subclass.appendChild(doc.createTextNode(sapParams[4]));
                itemHdrDesc.appendChild(subclass);

                Element status = doc.createElement("ns1:status");
                status.appendChild(doc.createTextNode("A"));
                itemHdrDesc.appendChild(status);

                Element item_desc = doc.createElement("ns1:item_desc");
                item_desc.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(item_desc);

                Element short_desc = doc.createElement("ns1:short_desc");
                short_desc.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(short_desc);

                Element primary_ref_item_ind = doc.createElement("ns1:primary_ref_item_ind");
                primary_ref_item_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(primary_ref_item_ind);

                Element standard_uom = doc.createElement("ns1:standard_uom");
                standard_uom.appendChild(doc.createTextNode("EA"));
                itemHdrDesc.appendChild(standard_uom);

                Element simple_pack_ind = doc.createElement("ns1:simple_pack_ind");
                simple_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(simple_pack_ind);

                Element sellable_ind = doc.createElement("ns1:sellable_ind");
                sellable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(sellable_ind);

                Element orderable_ind = doc.createElement("ns1:orderable_ind");
                orderable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(orderable_ind);

                Element inventory_ind = doc.createElement("ns1:inventory_ind");
                inventory_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(inventory_ind);

                Element notional_pack_ind = doc.createElement("ns1:notional_pack_ind");
                notional_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(notional_pack_ind);

                Element soh_inquiry_at_pack_ind = doc.createElement("ns1:soh_inquiry_at_pack_ind");
                soh_inquiry_at_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(soh_inquiry_at_pack_ind);

                Element purchase_type = doc.createElement("ns1:purchase_type");
                purchase_type.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(purchase_type);

                rootElement.appendChild(itemHdrDesc);

                // ItemSupDesc elements
                Element itemSupDesc = doc.createElement("ns1:ItemSupDesc");
                itemSupDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemSupDesc/v1");

                Element items = doc.createElement("ns1:item");
                items.appendChild(doc.createTextNode(sapParams[0]));
                itemSupDesc.appendChild(items);

                Element supplier = doc.createElement("ns1:supplier");
                supplier.appendChild(doc.createTextNode("1000"));
                itemSupDesc.appendChild(supplier);

                Element primary_supp_ind = doc.createElement("ns1:primary_supp_ind");
                primary_supp_ind.appendChild(doc.createTextNode("Y"));
                itemSupDesc.appendChild(primary_supp_ind);

                rootElement.appendChild(itemSupDesc);

                // itemSupCtyDesc elements
                Element itemSupCtyDesc = doc.createElement("ns1:ItemSupCtyDesc");
                itemSupCtyDesc.setAttribute("xmlns:ns1",
                                            "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyDesc/v1");

                Element itemsc = doc.createElement("ns1:item");
                itemsc.appendChild(doc.createTextNode(sapParams[0]));
                itemSupCtyDesc.appendChild(itemsc);

                Element supplierc = doc.createElement("ns1:supplier");
                supplierc.appendChild(doc.createTextNode("1000"));
                itemSupCtyDesc.appendChild(supplierc);

                Element origin_country_id = doc.createElement("ns1:origin_country_id");
                origin_country_id.appendChild(doc.createTextNode(country));
                itemSupCtyDesc.appendChild(origin_country_id);

                Element primary_supp_inds = doc.createElement("ns1:primary_supp_ind");
                primary_supp_inds.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_supp_inds);

                Element primary_country_ind = doc.createElement("ns1:primary_country_ind");
                primary_country_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_country_ind);

                Element unit_cost = doc.createElement("ns1:unit_cost");
                unit_cost.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(unit_cost);

                Element supp_pack_size = doc.createElement("ns1:supp_pack_size");
                supp_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(supp_pack_size);

                Element inner_pack_size = doc.createElement("ns1:inner_pack_size");
                inner_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(inner_pack_size);

                Element round_lvl = doc.createElement("ns1:round_lvl");
                round_lvl.appendChild(doc.createTextNode("C"));
                itemSupCtyDesc.appendChild(round_lvl);

                Element ti = doc.createElement("ns1:ti");
                ti.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(ti);

                Element hi = doc.createElement("ns1:hi");
                hi.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(hi);

                rootElement.appendChild(itemSupCtyDesc);

                // itemSupCtyMfrDesc elements
                Element itemSupCtyMfrDesc = doc.createElement("ns1:ItemSupCtyMfrDesc");
                itemSupCtyMfrDesc.setAttribute("xmlns:ns1",
                                               "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyMfrDesc/v1");

                Element itemscm = doc.createElement("ns1:item");
                itemscm.appendChild(doc.createTextNode(sapParams[0]));
                itemSupCtyMfrDesc.appendChild(itemscm);

                Element suppliercm = doc.createElement("ns1:supplier");
                suppliercm.appendChild(doc.createTextNode("1000"));
                itemSupCtyMfrDesc.appendChild(suppliercm);

                Element manufacturer_ctry_id = doc.createElement("ns1:manufacturer_ctry_id");
                manufacturer_ctry_id.appendChild(doc.createTextNode("CL"));
                itemSupCtyMfrDesc.appendChild(manufacturer_ctry_id);

                Element primary_manufacturer_ctry_ind = doc.createElement("ns1:primary_manufacturer_ctry_ind");
                primary_manufacturer_ctry_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyMfrDesc.appendChild(primary_manufacturer_ctry_ind);

                rootElement.appendChild(itemSupCtyMfrDesc);

                // write the content into xml file
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                transformer.setOutputProperty("omit-xml-declaration", "yes");
                DOMSource source = new DOMSource(doc);
                StreamResult streamResult = new StreamResult(new File(orsimXMLPath + "_" + sapParams[0] + ".XML"));

                transformer.transform(source, streamResult);

                //EJB client integration.
                odiInvokeEJBItemCre(orsimXMLPath + "_" + sapParams[0] + ".XML",
                                    orsimXMLPath + "_" + sapParams[0] + "RSP.XML");


            }

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
        return result;
    }

    public String mapFileToXml() {
        String result = "";
        String country = "CH";
        String sapFilePath = "C:\\JDeveloper\\WebClient\\\\SimpleORSIMIntegration\\deploy\\sap_file.txt";
        String orsimXMLPath = "C:\\JDeveloper\\WebClient\\\\SimpleORSIMIntegration\\deploy\\ITEMS_ITEMCRE";
        List<String[]> params = new ArrayList();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemcre.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\itemcre.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                sapFilePath = prop.getProperty("sapPFilePath");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
                params = common.readSapFile(sapFilePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        try {
            for (String[] sapParams : params) {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                // root elements
                Document doc = docBuilder.newDocument();
                Element rootElement = doc.createElement("ItemDesc");
                rootElement.setAttribute("xmlns", "http://www.oracle.com/retail/integration/base/bo/ItemDesc/v1");
                rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
                doc.appendChild(rootElement);

                // itemHdrDesc elements
                Element itemHdrDesc = doc.createElement("ns1:ItemHdrDesc");
                itemHdrDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemHdrDesc/v1");

                Element item = doc.createElement("ns1:item");
                item.appendChild(doc.createTextNode(sapParams[0]));
                itemHdrDesc.appendChild(item);

                Element item_number_type = doc.createElement("ns1:item_number_type");
                item_number_type.appendChild(doc.createTextNode("0"));
                itemHdrDesc.appendChild(item_number_type);

                Element pack_ind = doc.createElement("ns1:pack_ind");
                pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(pack_ind);

                Element item_level = doc.createElement("ns1:item_level");
                item_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(item_level);

                Element tran_level = doc.createElement("ns1:tran_level");
                tran_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(tran_level);

                Element dept = doc.createElement("ns1:dept");
                dept.appendChild(doc.createTextNode(sapParams[2]));
                itemHdrDesc.appendChild(dept);

                Element eclass = doc.createElement("ns1:class");
                eclass.appendChild(doc.createTextNode(sapParams[3]));
                itemHdrDesc.appendChild(eclass);

                Element subclass = doc.createElement("ns1:subclass");
                subclass.appendChild(doc.createTextNode(sapParams[4]));
                itemHdrDesc.appendChild(subclass);

                Element status = doc.createElement("ns1:status");
                status.appendChild(doc.createTextNode("A"));
                itemHdrDesc.appendChild(status);

                Element item_desc = doc.createElement("ns1:item_desc");
                item_desc.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(item_desc);

                Element short_desc = doc.createElement("ns1:short_desc");
                short_desc.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(short_desc);

                Element primary_ref_item_ind = doc.createElement("ns1:primary_ref_item_ind");
                primary_ref_item_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(primary_ref_item_ind);

                Element standard_uom = doc.createElement("ns1:standard_uom");
                standard_uom.appendChild(doc.createTextNode("EA"));
                itemHdrDesc.appendChild(standard_uom);

                Element simple_pack_ind = doc.createElement("ns1:simple_pack_ind");
                simple_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(simple_pack_ind);

                Element sellable_ind = doc.createElement("ns1:sellable_ind");
                sellable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(sellable_ind);

                Element orderable_ind = doc.createElement("ns1:orderable_ind");
                orderable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(orderable_ind);

                Element inventory_ind = doc.createElement("ns1:inventory_ind");
                inventory_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(inventory_ind);

                Element notional_pack_ind = doc.createElement("ns1:notional_pack_ind");
                notional_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(notional_pack_ind);

                Element soh_inquiry_at_pack_ind = doc.createElement("ns1:soh_inquiry_at_pack_ind");
                soh_inquiry_at_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(soh_inquiry_at_pack_ind);

                Element purchase_type = doc.createElement("ns1:purchase_type");
                purchase_type.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(purchase_type);

                rootElement.appendChild(itemHdrDesc);

                // ItemSupDesc elements
                Element itemSupDesc = doc.createElement("ns1:ItemSupDesc");
                itemSupDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemSupDesc/v1");

                Element items = doc.createElement("ns1:item");
                items.appendChild(doc.createTextNode(sapParams[0]));
                itemSupDesc.appendChild(items);

                Element supplier = doc.createElement("ns1:supplier");
                supplier.appendChild(doc.createTextNode("1000"));
                itemSupDesc.appendChild(supplier);

                Element primary_supp_ind = doc.createElement("ns1:primary_supp_ind");
                primary_supp_ind.appendChild(doc.createTextNode("Y"));
                itemSupDesc.appendChild(primary_supp_ind);

                rootElement.appendChild(itemSupDesc);

                // itemSupCtyDesc elements
                Element itemSupCtyDesc = doc.createElement("ns1:ItemSupCtyDesc");
                itemSupCtyDesc.setAttribute("xmlns:ns1",
                                            "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyDesc/v1");

                Element itemsc = doc.createElement("ns1:item");
                itemsc.appendChild(doc.createTextNode(sapParams[0]));
                itemSupCtyDesc.appendChild(itemsc);

                Element supplierc = doc.createElement("ns1:supplier");
                supplierc.appendChild(doc.createTextNode("1000"));
                itemSupCtyDesc.appendChild(supplierc);

                Element origin_country_id = doc.createElement("ns1:origin_country_id");
                origin_country_id.appendChild(doc.createTextNode(country));
                itemSupCtyDesc.appendChild(origin_country_id);

                Element primary_supp_inds = doc.createElement("ns1:primary_supp_ind");
                primary_supp_inds.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_supp_inds);

                Element primary_country_ind = doc.createElement("ns1:primary_country_ind");
                primary_country_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_country_ind);

                Element unit_cost = doc.createElement("ns1:unit_cost");
                unit_cost.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(unit_cost);

                Element supp_pack_size = doc.createElement("ns1:supp_pack_size");
                supp_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(supp_pack_size);

                Element inner_pack_size = doc.createElement("ns1:inner_pack_size");
                inner_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(inner_pack_size);

                Element round_lvl = doc.createElement("ns1:round_lvl");
                round_lvl.appendChild(doc.createTextNode("C"));
                itemSupCtyDesc.appendChild(round_lvl);

                Element ti = doc.createElement("ns1:ti");
                ti.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(ti);

                Element hi = doc.createElement("ns1:hi");
                hi.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(hi);

                rootElement.appendChild(itemSupCtyDesc);

                // itemSupCtyMfrDesc elements
                Element itemSupCtyMfrDesc = doc.createElement("ns1:ItemSupCtyMfrDesc");
                itemSupCtyMfrDesc.setAttribute("xmlns:ns1",
                                               "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyMfrDesc/v1");

                Element itemscm = doc.createElement("ns1:item");
                itemscm.appendChild(doc.createTextNode(sapParams[0]));
                itemSupCtyMfrDesc.appendChild(itemscm);

                Element suppliercm = doc.createElement("ns1:supplier");
                suppliercm.appendChild(doc.createTextNode("1000"));
                itemSupCtyMfrDesc.appendChild(suppliercm);

                Element manufacturer_ctry_id = doc.createElement("ns1:manufacturer_ctry_id");
                manufacturer_ctry_id.appendChild(doc.createTextNode("CL"));
                itemSupCtyMfrDesc.appendChild(manufacturer_ctry_id);

                Element primary_manufacturer_ctry_ind = doc.createElement("ns1:primary_manufacturer_ctry_ind");
                primary_manufacturer_ctry_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyMfrDesc.appendChild(primary_manufacturer_ctry_ind);

                rootElement.appendChild(itemSupCtyMfrDesc);

                // write the content into xml file
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                transformer.setOutputProperty("omit-xml-declaration", "yes");
                DOMSource source = new DOMSource(doc);
                StreamResult streamResult = new StreamResult(new File(orsimXMLPath + "_" + sapParams[0] + ".XML"));

                transformer.transform(source, streamResult);

                //EJB client integration.
                odiInvokeEJBItemCre(orsimXMLPath + "_" + sapParams[0] + ".XML",
                                    orsimXMLPath + "_" + sapParams[0] + "RSP.XML");
            }
        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
        return result;
    }

    public String mapDBToXml() {
        String result = "";
        String EBSconnectionURI = "CH";
        String EBSuser = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\sap_file.txt";
        String EBSpass = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\ITEMS_ITEMCRE";
        String country = "";
        String orsimXMLPath = "";
        List<String[]> params = new ArrayList();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemcre.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\itemcre.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                EBSconnectionURI = prop.getProperty("EBSconnectionURI");
                EBSuser = prop.getProperty("EBSuser");
                EBSpass = prop.getProperty("EBSpass");
                country = prop.getProperty("country");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
                params = common.readEBSDB(EBSconnectionURI, EBSuser, EBSpass);
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        try {
            for (String[] sapParams : params) {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                // root elements
                Document doc = docBuilder.newDocument();
                Element rootElement = doc.createElement("ItemDesc");
                rootElement.setAttribute("xmlns", "http://www.oracle.com/retail/integration/base/bo/ItemDesc/v1");
                rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
                doc.appendChild(rootElement);

                // itemHdrDesc elements
                Element itemHdrDesc = doc.createElement("ns1:ItemHdrDesc");
                itemHdrDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemHdrDesc/v1");

                Element item = doc.createElement("ns1:item");
                item.appendChild(doc.createTextNode(sapParams[0]));
                itemHdrDesc.appendChild(item);

                Element item_number_type = doc.createElement("ns1:item_number_type");
                item_number_type.appendChild(doc.createTextNode("0"));
                itemHdrDesc.appendChild(item_number_type);

                Element pack_ind = doc.createElement("ns1:pack_ind");
                pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(pack_ind);

                Element item_level = doc.createElement("ns1:item_level");
                item_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(item_level);

                Element tran_level = doc.createElement("ns1:tran_level");
                tran_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(tran_level);

                Element dept = doc.createElement("ns1:dept");
                dept.appendChild(doc.createTextNode(sapParams[14]));
                itemHdrDesc.appendChild(dept);

                Element eclass = doc.createElement("ns1:class");
                eclass.appendChild(doc.createTextNode(sapParams[15]));
                itemHdrDesc.appendChild(eclass);

                Element subclass = doc.createElement("ns1:subclass");
                subclass.appendChild(doc.createTextNode(sapParams[16]));
                itemHdrDesc.appendChild(subclass);

                Element status = doc.createElement("ns1:status");
                status.appendChild(doc.createTextNode("A"));
                itemHdrDesc.appendChild(status);

                Element item_desc = doc.createElement("ns1:item_desc");
                item_desc.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(item_desc);

                Element short_desc = doc.createElement("ns1:short_desc");
                short_desc.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(short_desc);

                Element primary_ref_item_ind = doc.createElement("ns1:primary_ref_item_ind");
                primary_ref_item_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(primary_ref_item_ind);

                Element standard_uom = doc.createElement("ns1:standard_uom");
                standard_uom.appendChild(doc.createTextNode("EA"));
                itemHdrDesc.appendChild(standard_uom);

                Element simple_pack_ind = doc.createElement("ns1:simple_pack_ind");
                simple_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(simple_pack_ind);

                Element sellable_ind = doc.createElement("ns1:sellable_ind");
                sellable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(sellable_ind);

                Element orderable_ind = doc.createElement("ns1:orderable_ind");
                orderable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(orderable_ind);

                Element inventory_ind = doc.createElement("ns1:inventory_ind");
                inventory_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(inventory_ind);

                Element notional_pack_ind = doc.createElement("ns1:notional_pack_ind");
                notional_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(notional_pack_ind);

                Element soh_inquiry_at_pack_ind = doc.createElement("ns1:soh_inquiry_at_pack_ind");
                soh_inquiry_at_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(soh_inquiry_at_pack_ind);

                Element purchase_type = doc.createElement("ns1:purchase_type");
                purchase_type.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(purchase_type);

                rootElement.appendChild(itemHdrDesc);

                // ItemSupDesc elements
                Element itemSupDesc = doc.createElement("ns1:ItemSupDesc");
                itemSupDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemSupDesc/v1");

                Element items = doc.createElement("ns1:item");
                items.appendChild(doc.createTextNode(sapParams[0]));
                itemSupDesc.appendChild(items);

                Element supplier = doc.createElement("ns1:supplier");
                supplier.appendChild(doc.createTextNode("1000"));
                itemSupDesc.appendChild(supplier);

                Element primary_supp_ind = doc.createElement("ns1:primary_supp_ind");
                primary_supp_ind.appendChild(doc.createTextNode("Y"));
                itemSupDesc.appendChild(primary_supp_ind);

                rootElement.appendChild(itemSupDesc);

                // itemSupCtyDesc elements
                Element itemSupCtyDesc = doc.createElement("ns1:ItemSupCtyDesc");
                itemSupCtyDesc.setAttribute("xmlns:ns1",
                                            "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyDesc/v1");

                Element itemsc = doc.createElement("ns1:item");
                itemsc.appendChild(doc.createTextNode(sapParams[0]));
                itemSupCtyDesc.appendChild(itemsc);

                Element supplierc = doc.createElement("ns1:supplier");
                supplierc.appendChild(doc.createTextNode("1000"));
                itemSupCtyDesc.appendChild(supplierc);

                Element origin_country_id = doc.createElement("ns1:origin_country_id");
                origin_country_id.appendChild(doc.createTextNode(country));
                itemSupCtyDesc.appendChild(origin_country_id);

                Element primary_supp_inds = doc.createElement("ns1:primary_supp_ind");
                primary_supp_inds.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_supp_inds);

                Element primary_country_ind = doc.createElement("ns1:primary_country_ind");
                primary_country_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_country_ind);

                Element unit_cost = doc.createElement("ns1:unit_cost");
                unit_cost.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(unit_cost);

                Element supp_pack_size = doc.createElement("ns1:supp_pack_size");
                supp_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(supp_pack_size);

                Element inner_pack_size = doc.createElement("ns1:inner_pack_size");
                inner_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(inner_pack_size);

                Element round_lvl = doc.createElement("ns1:round_lvl");
                round_lvl.appendChild(doc.createTextNode("C"));
                itemSupCtyDesc.appendChild(round_lvl);

                Element ti = doc.createElement("ns1:ti");
                ti.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(ti);

                Element hi = doc.createElement("ns1:hi");
                hi.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(hi);

                rootElement.appendChild(itemSupCtyDesc);

                // itemSupCtyMfrDesc elements
                Element itemSupCtyMfrDesc = doc.createElement("ns1:ItemSupCtyMfrDesc");
                itemSupCtyMfrDesc.setAttribute("xmlns:ns1",
                                               "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyMfrDesc/v1");

                Element itemscm = doc.createElement("ns1:item");
                itemscm.appendChild(doc.createTextNode(sapParams[0]));
                itemSupCtyMfrDesc.appendChild(itemscm);

                Element suppliercm = doc.createElement("ns1:supplier");
                suppliercm.appendChild(doc.createTextNode("1000"));
                itemSupCtyMfrDesc.appendChild(suppliercm);

                Element manufacturer_ctry_id = doc.createElement("ns1:manufacturer_ctry_id");
                manufacturer_ctry_id.appendChild(doc.createTextNode("CL"));
                itemSupCtyMfrDesc.appendChild(manufacturer_ctry_id);

                Element primary_manufacturer_ctry_ind = doc.createElement("ns1:primary_manufacturer_ctry_ind");
                primary_manufacturer_ctry_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyMfrDesc.appendChild(primary_manufacturer_ctry_ind);

                rootElement.appendChild(itemSupCtyMfrDesc);

                // write the content into xml file
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                transformer.setOutputProperty("omit-xml-declaration", "yes");
                DOMSource source = new DOMSource(doc);
                StreamResult streamResult = new StreamResult(new File(orsimXMLPath + "_" + sapParams[0] + ".XML"));

                transformer.transform(source, streamResult);

                //EJB client integration.
                odiInvokeEJBItemCre(orsimXMLPath + "_" + sapParams[0] + ".XML",
                                    orsimXMLPath + "_" + sapParams[0] + "RSP.XML");


            }

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
        return result;
    }

    public String odiInvokeEJBItemCre(String reqPath, String rspPath) {
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
                type = prop.getProperty("creType");
                family = prop.getProperty("creFamily");
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
                result = staticInvokeEJB(ctx, url, user, pass, family, type, xml);
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

    public String mapXmlToXml() {

        String result = "";
        String country = "CH";
        String offeringXMLPath = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\";
        String orsimXMLPath = "C:\\JDeveloper\\WebClient\\SimpleORSIMIntegration\\deploy\\";
        List<String[]> params = new ArrayList();
        try {
            Properties prop = new Properties();
            String propFileName = "/u01/entel/jars/itemcre.properties";
            //propFileName = "C:\\Users\\Proyecto\\Documents\\JDeveloper\\ODI\\properties\\itemcre.properties";
            InputStream inputStream = new FileInputStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
                country = prop.getProperty("country");
                offeringXMLPath = prop.getProperty("offeringXMLPath");
                orsimXMLPath = prop.getProperty("orsimXMLPath");
                params = common.readECMFile(offeringXMLPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
        }
        try {
            for (String[] sapParams : params) {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                // root elements
                Document doc = docBuilder.newDocument();
                Element rootElement = doc.createElement("ItemDesc");
                rootElement.setAttribute("xmlns", "http://www.oracle.com/retail/integration/base/bo/ItemDesc/v1");
                rootElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
                doc.appendChild(rootElement);

                // itemHdrDesc elements
                Element itemHdrDesc = doc.createElement("ns1:ItemHdrDesc");
                itemHdrDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemHdrDesc/v1");

                Element item = doc.createElement("ns1:item");
                item.appendChild(doc.createTextNode(sapParams[1]));
                itemHdrDesc.appendChild(item);

                Element item_number_type = doc.createElement("ns1:item_number_type");
                item_number_type.appendChild(doc.createTextNode("0"));
                itemHdrDesc.appendChild(item_number_type);

                Element pack_ind = doc.createElement("ns1:pack_ind");
                pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(pack_ind);

                Element item_level = doc.createElement("ns1:item_level");
                item_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(item_level);

                Element tran_level = doc.createElement("ns1:tran_level");
                tran_level.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(tran_level);

                Element dept = doc.createElement("ns1:dept");
                dept.appendChild(doc.createTextNode("1"));
                itemHdrDesc.appendChild(dept);

                Element eclass = doc.createElement("ns1:class");
                switch (sapParams[3]) {
                case "Add-on":
                    eclass.appendChild(doc.createTextNode("10"));
                    break;
                case "Equipment":
                    eclass.appendChild(doc.createTextNode("2"));
                    break;
                case "Product":
                    eclass.appendChild(doc.createTextNode("12"));
                    break;
                }
                itemHdrDesc.appendChild(eclass);

                Element subclass = doc.createElement("ns1:subclass");
                switch (sapParams[3]) {
                case "Add-on":
                    switch (sapParams[6]) {
                    case "Mix":
                        subclass.appendChild(doc.createTextNode("6"));
                        break;
                    }
                    break;
                case "Equipment":
                    switch (sapParams[6]) {
                    case "Bundle":
                        subclass.appendChild(doc.createTextNode("3"));
                        break;
                    case "Modem":
                        subclass.appendChild(doc.createTextNode("3"));
                        break;
                    case "Cellphone":
                        subclass.appendChild(doc.createTextNode("3"));
                        break;
                    case "Card":
                        subclass.appendChild(doc.createTextNode("5"));
                        break;
                    case "Accessory":
                        subclass.appendChild(doc.createTextNode("4"));
                        break;
                    }
                    break;
                case "Product":
                    switch (sapParams[6]) {
                    case "Delivery":
                        subclass.appendChild(doc.createTextNode("13"));
                        break;
                    }
                    break;
                }
                itemHdrDesc.appendChild(subclass);

                Element status = doc.createElement("ns1:status");
                status.appendChild(doc.createTextNode("A"));
                itemHdrDesc.appendChild(status);

                Element item_desc = doc.createElement("ns1:item_desc");
                item_desc.appendChild(doc.createTextNode(sapParams[7]));
                itemHdrDesc.appendChild(item_desc);

                Element short_desc = doc.createElement("ns1:short_desc");
                short_desc.appendChild(doc.createTextNode(sapParams[8]));
                itemHdrDesc.appendChild(short_desc);

                Element primary_ref_item_ind = doc.createElement("ns1:primary_ref_item_ind");
                primary_ref_item_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(primary_ref_item_ind);

                Element standard_uom = doc.createElement("ns1:standard_uom");
                standard_uom.appendChild(doc.createTextNode("EA"));
                itemHdrDesc.appendChild(standard_uom);

                Element simple_pack_ind = doc.createElement("ns1:simple_pack_ind");
                simple_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(simple_pack_ind);

                Element sellable_ind = doc.createElement("ns1:sellable_ind");
                sellable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(sellable_ind);

                Element orderable_ind = doc.createElement("ns1:orderable_ind");
                orderable_ind.appendChild(doc.createTextNode("Y"));
                itemHdrDesc.appendChild(orderable_ind);

                Element inventory_ind = doc.createElement("ns1:inventory_ind");
                switch (sapParams[4]) {
                case "Rental":
                    inventory_ind.appendChild(doc.createTextNode("Y"));
                    break;
                case "Sale":
                    inventory_ind.appendChild(doc.createTextNode("Y"));
                    break;
                case "Service":
                    inventory_ind.appendChild(doc.createTextNode("N"));
                    break;
                }
                itemHdrDesc.appendChild(inventory_ind);

                Element notional_pack_ind = doc.createElement("ns1:notional_pack_ind");
                notional_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(notional_pack_ind);

                Element soh_inquiry_at_pack_ind = doc.createElement("ns1:soh_inquiry_at_pack_ind");
                soh_inquiry_at_pack_ind.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(soh_inquiry_at_pack_ind);

                Element purchase_type = doc.createElement("ns1:purchase_type");
                purchase_type.appendChild(doc.createTextNode("N"));
                itemHdrDesc.appendChild(purchase_type);

                rootElement.appendChild(itemHdrDesc);

                // ItemSupDesc elements
                Element itemSupDesc = doc.createElement("ns1:ItemSupDesc");
                itemSupDesc.setAttribute("xmlns:ns1",
                                         "http://www.oracle.com/retail/integration/base/bo/ItemSupDesc/v1");

                Element items = doc.createElement("ns1:item");
                items.appendChild(doc.createTextNode(sapParams[1]));
                itemSupDesc.appendChild(items);

                Element supplier = doc.createElement("ns1:supplier");
                supplier.appendChild(doc.createTextNode("1000"));
                itemSupDesc.appendChild(supplier);

                Element primary_supp_ind = doc.createElement("ns1:primary_supp_ind");
                primary_supp_ind.appendChild(doc.createTextNode("Y"));
                itemSupDesc.appendChild(primary_supp_ind);

                rootElement.appendChild(itemSupDesc);

                // itemSupCtyDesc elements
                Element itemSupCtyDesc = doc.createElement("ns1:ItemSupCtyDesc");
                itemSupCtyDesc.setAttribute("xmlns:ns1",
                                            "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyDesc/v1");

                Element itemsc = doc.createElement("ns1:item");
                itemsc.appendChild(doc.createTextNode(sapParams[1]));
                itemSupCtyDesc.appendChild(itemsc);

                Element supplierc = doc.createElement("ns1:supplier");
                supplierc.appendChild(doc.createTextNode("1000"));
                itemSupCtyDesc.appendChild(supplierc);

                Element origin_country_id = doc.createElement("ns1:origin_country_id");
                if (sapParams[5].length() > 2) {
                    origin_country_id.appendChild(doc.createTextNode(sapParams[5].substring(0, 2)));
                } else {
                    origin_country_id.appendChild(doc.createTextNode("CL"));
                }
                itemSupCtyDesc.appendChild(origin_country_id);

                Element primary_supp_inds = doc.createElement("ns1:primary_supp_ind");
                primary_supp_inds.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_supp_inds);

                Element primary_country_ind = doc.createElement("ns1:primary_country_ind");
                primary_country_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyDesc.appendChild(primary_country_ind);

                Element unit_cost = doc.createElement("ns1:unit_cost");
                unit_cost.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(unit_cost);

                Element supp_pack_size = doc.createElement("ns1:supp_pack_size");
                supp_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(supp_pack_size);

                Element inner_pack_size = doc.createElement("ns1:inner_pack_size");
                inner_pack_size.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(inner_pack_size);

                Element round_lvl = doc.createElement("ns1:round_lvl");
                round_lvl.appendChild(doc.createTextNode("C"));
                itemSupCtyDesc.appendChild(round_lvl);

                Element ti = doc.createElement("ns1:ti");
                ti.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(ti);

                Element hi = doc.createElement("ns1:hi");
                hi.appendChild(doc.createTextNode("1"));
                itemSupCtyDesc.appendChild(hi);

                rootElement.appendChild(itemSupCtyDesc);

                // itemSupCtyMfrDesc elements
                Element itemSupCtyMfrDesc = doc.createElement("ns1:ItemSupCtyMfrDesc");
                itemSupCtyMfrDesc.setAttribute("xmlns:ns1",
                                               "http://www.oracle.com/retail/integration/base/bo/ItemSupCtyMfrDesc/v1");

                Element itemscm = doc.createElement("ns1:item");
                itemscm.appendChild(doc.createTextNode(sapParams[1]));
                itemSupCtyMfrDesc.appendChild(itemscm);

                Element suppliercm = doc.createElement("ns1:supplier");
                suppliercm.appendChild(doc.createTextNode("1000"));
                itemSupCtyMfrDesc.appendChild(suppliercm);

                Element manufacturer_ctry_id = doc.createElement("ns1:manufacturer_ctry_id");
                if (sapParams[5].length() > 2) {
                    manufacturer_ctry_id.appendChild(doc.createTextNode(sapParams[5].substring(0, 2)));
                } else {
                    manufacturer_ctry_id.appendChild(doc.createTextNode("CL"));
                }
                itemSupCtyMfrDesc.appendChild(manufacturer_ctry_id);

                Element primary_manufacturer_ctry_ind = doc.createElement("ns1:primary_manufacturer_ctry_ind");
                primary_manufacturer_ctry_ind.appendChild(doc.createTextNode("Y"));
                itemSupCtyMfrDesc.appendChild(primary_manufacturer_ctry_ind);

                rootElement.appendChild(itemSupCtyMfrDesc);

                // write the content into xml file
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
                transformer.setOutputProperty(OutputKeys.METHOD, "xml");
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                transformer.setOutputProperty("omit-xml-declaration", "yes");
                DOMSource source = new DOMSource(doc);
                StreamResult streamResult = new StreamResult(new File(orsimXMLPath + "_" + sapParams[0] + ".XML"));

                transformer.transform(source, streamResult);
            }

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
        }
        return result;
    }

}
