package main.tde.java;

import mapping.rtd.RtdController;

import mapping.ecm.EcmController;

import mapping.ib.*;

import mapping.ib.SalesController;

import mapping.mig.CDMController;

import mapping.orsim.ItemCreController;

import mapping.orsim.ItemLocController;

import mapping.port.PortController;

import mapping.sap.SapController;

import mapping.xoffice.XOfficeController;

public class Main {

    public static void main(String[] args) {
        ItemCreController itemCreController = new ItemCreController();
        ItemLocController itemLocController = new ItemLocController();
        XOfficeController xOfficeController = new XOfficeController();
        EcmController ecmController = new EcmController();
        RtdController rtdController = new RtdController();
        PosController posController = new PosController();
        SalesController salesController = new SalesController();
        EquiposController equiposController = new EquiposController();
        SimController simController = new SimController();
        PortController portController = new PortController();
        CatalogController catalogController = new CatalogController();
        CDMController cDMController = new CDMController();
        ActivePreController activePreController = new ActivePreController();
        SapController sapController = new SapController();

        if (args.length > 0) {
            switch (args[0]) {
            case "-ebs":
                    //ETL_Accessory_EBS_to_CDM
                    itemCreController.mapDBToXml();
                    itemLocController.mapDBToXml();
                    xOfficeController.mapDBToXOffice();
                    break;
            case "-ecm":
                //ETL_ProductOffering_CDM_to_SIM_XOffice_RTD
                xOfficeController.mapXmlToXOffice();
                //rtdController.mappXmlToDb();
                itemCreController.mapXmlToXml();
                itemLocController.mapXmlToXml();
                break;
            case "-sap":
                //ETL_Accessory_CDM_to_XOffice_SIM_ECM
                //sapController.checkFailure();
                itemCreController.mapAFileToXml();
                itemLocController.mapAFileToXml();

                sapController.processFailure();
                xOfficeController.mapSAPToXOffice("sapFilePath");
                ecmController.mapAFileToXML("sapFilePath");
                break;
            case "-sfa":
                //ETL_Accessory_CDM_to_XOffice_SIM_ECM
                sapController.checkFailure();
                //sapController.processFailure();
                xOfficeController.mapSAPToXOffice("sapRecoveryPath");
                ecmController.mapAFileToXML("sapRecoveryPath");
                break;
            case "-spr":
                //ETL_Product_CDM_to_XOffice_SIM_ECM
                itemCreController.mapFileToXml();
                itemLocController.mapFileToXml();
                //xOfficeController.mapSAPToXOffice();
                //ecmController.mappFileToXML();
                break;
            case "-pos":
                //ETL_PuntosVenta_CDM_to_IB
                posController.mapPosToIB();
                break;
            case "-sal":
                //ETL_VentaPre_CDM_to_IB
                salesController.eocOrderToVentaPre();
                break;
            case "-equ":
                //ETL_EquipoPre_CDM_to_IB
                equiposController.eocOrderToEquiposPre();
                break;
            case "-sim":
                //ETL_SimCardPre_CDM_to_IB
                simController.eocOrderToSimPre();
                break;
            case "-pin":
                //ETL_PortInOrdersStatus_DB_to_ACPN
                portController.mapEocToPortIn();
                break;
            case "-pou":
                //ETL_PortOutOrdersStatus_DB_to_ACPN
                portController.mapEocToPortOut();
                break;
            case "-cat":
                //ETL_CatProd_CDM_to_IB_DTM
                catalogController.ecmProductToCatalog();
                break;
            case "-mig":
                //ETL_MigrationInfo_DB_to_CDM
                cDMController.mapDBtoFile();
                break;
            case "-act":
                //ETL_ActivePre_CDM_to_IB
                activePreController.eocOrderToActivePre();
                break;
            default:
                //SAP to ORSIM
                xOfficeController.mapSAPToXOffice("sapFilePath");
                ecmController.mapAFileToXML("sapFilePath");
                itemCreController.mapAFileToXml();
                itemLocController.mapAFileToXml();
                break;
            }
        } else {
            //ETL_Accessory_CDM_to_XOffice_SIM_ECM
            //activePreController.eocOrderToActivePre();
            equiposController.eocOrderToEquiposPre();
            simController.eocOrderToSimPre();
            salesController.eocOrderToVentaPre();
            activePreController.eocOrderToActivePre();
        }
    }
}
