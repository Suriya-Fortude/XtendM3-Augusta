/**
 * README
 * This extension is being used to update order number in EXTSTH and EXTSTL table
 *
 * Name: EXT001MI.UpdPlayerOrder
 * Description: Update Order number in EXTSTH and EXTSTL tables
 * Date	      Changed By                      Description
 *20230403  SuriyaN@fortude.co     Update Order number in EXTSTH and EXTSTL tables */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdPlayerOrder extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller

  private String iDIVI, iORNR, iORNO
  private int iCONO
  private boolean validInput = true

  public UpdPlayerOrder(MIAPI mi, DatabaseAPI database, ProgramAPI program,MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
  }
  /**
   ** Main function
   * @param
   * @return
   */
  public void main() {
    iDIVI = (mi.inData.get("DIVI") == null || mi.inData.get("DIVI").trim().isEmpty()) ? program.LDAZD.DIVI : mi.inData.get("DIVI")
    iORNR = (mi.inData.get("ORNR") == null || mi.inData.get("ORNR").trim().isEmpty()) ? "" : mi.inData.get("ORNR")
    iORNO = (mi.inData.get("ORNO") == null || mi.inData.get("ORNO").trim().isEmpty()) ? "" : mi.inData.get("ORNO")
    iCONO = (mi.inData.get("CONO") == null || mi.inData.get("CONO").trim().isEmpty()) ? program.LDAZD.CONO as Integer : mi.inData.get("CONO") as Integer
    validateInput()
    if(validInput)
    {
      listPlayerHeaderDetails()
    }
  }

  /**
   *Validate Records
   * @params
   * @return
   */
  public validateInput() {

    //Validate Company Number
    def params = ["CONO": iCONO.toString().trim()]
    def callback = {
      Map < String,
        String > response ->
        if (response.CONO == null) {
          mi.error("Invalid Company Number " + iCONO)
          validInput = false
          return false
        }
    }
    miCaller.call("MNS095MI", "Get", params, callback)

    //Validate Temporary Order Number
    DBAction query = database.table("OXHEAD").index("00").build()
    DBContainer container = query.getContainer()
    container.set("OACONO", iCONO)
    container.set("OAORNO", iORNR)

    if(!query.read(container))
    {
      mi.error("Temporary Order Number not found "+iORNR)
      validInput = false
      return false
    }


    //Validate Final order Number
    params = ["CONO": iCONO.toString().trim(),"ORNO":iORNO.toString().trim()]
    callback = {
      Map < String,
        String > response ->
        if (response.ORNO == null) {
          mi.error("Invalid Order Number " + iORNO)
          validInput = false
          return false
        }
    }
    miCaller.call("OIS100MI", "GetHead", params, callback)

  }

  /**
   *List records from EXTSTH table
   * @params
   * @return
   */
  public listPlayerHeaderDetails() {
    DBAction query = database.table("EXTSTH").index("10").selection("EXPLID").build()
    DBContainer container = query.getContainer()
    container.set("EXORNR", iORNR)
    container.set("EXCONO", iCONO)
    container.set("EXDIVI", iDIVI)
    query.readAll(container, 3, resultset)

  }

  Closure < ? > resultset = {
    DBContainer container ->
      String PLID = container.get("EXPLID")

      def  params = ["CONO": iCONO.toString().trim(),"DIVI":iDIVI.toString().trim(),"PLID":PLID.toString().trim()
                     ,"ORNR":iORNR.toString().trim(),"ORNO":iORNO.toString().trim()]
      def callback = {
        Map < String,
          String > response ->
          if(response.errorMessage!=null)
          {
            mi.error(response.errorMessage)
          }
      }

      miCaller.call("EXT001MI", "UpdPlayerHead", params,callback)
      listLineDetails(PLID)
  }

  /**
   *List records to EXTSTL table
   * @params String PLID
   * @return
   */
  public listLineDetails(String PLID) {
    DBAction query = database.table("EXTSTL").index("00").selection("EXCONO","EXDIVI","EXPLID","EXORNR").build()
    DBContainer container = query.getContainer()
    container.set("EXPLID", PLID)
    container.set("EXCONO", iCONO)
    container.set("EXDIVI", iDIVI)
    query.readAll(container, 3, resultset_LstLineDetails)
  }

  Closure < ? > resultset_LstLineDetails = {
    DBContainer container ->
      String CONO = container.get("EXCONO")
      String DIVI = container.get("EXDIVI")
      String PLID = container.get("EXPLID")
      String ORNR = container.get("EXORNR")
      params = ["CONO": CONO.toString().trim(),"DIVI":DIVI.toString().trim(),"ORNR":ORNR.toString().trim(),
                "PLID":PLID.toString().trim(),"ORNO":iORNO.toString().trim()]

      miCaller.call("EXT001MI", "UpdPlayerLine", params,callback)
  }
}
