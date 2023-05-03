/**
 * README
 * This extension is being used to list data for lightning pick system. Lightning Pick is a pick-to-light system used for
 * picking ordered items from shelves and packing them as one delivery for an order.
 *
 * Name: EXT001MI.LstLPData
 * Description: List details of items to be picked.
 * Date	      Changed By                   Description
 * 20230130	  SuriyaN@fortude.co         Initial development from MAK AGS001MI.LstLPData
 *
 */
import java.util.HashMap

public class LstLPData extends ExtendM3Transaction {
  private final MIAPI mi
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller

  public LstLPData(MIAPI mi, ProgramAPI program, DatabaseAPI database, MICallerAPI miCaller) {
    this.mi = mi
    this.program = program
    this.database = database
    this.miCaller = miCaller
  }

  //Global Variables
  private int iCONO, iDLIX, maxPageSize = 10000
  private String iDIVI,tempORNO="",userDefinedField3 = ""


  HashMap < String, HashMap < String, String >> orderHeader = new HashMap < > ()
  HashMap < String, String > orderDetails = new HashMap < String, String > ()
  HashMap < String, HashMap < String, String >> allocationHeader = new HashMap <  > ()
  HashMap < String, String > allocationDeatils = new HashMap < String, String > ()

  public void main() {
    iCONO = (mi.inData.get("CONO") == null || mi.inData.get("CONO").trim().isEmpty()) ? program.LDAZD.CONO as Integer : mi.inData.get("CONO") as Integer
    iDIVI = (mi.inData.get("DIVI") == null || mi.inData.get("DIVI").trim().isEmpty()) ? program.LDAZD.DIVI : mi.inData.get("DIVI")
    iDLIX = (mi.inData.get("DLIX") == null || mi.inData.get("DLIX").trim().isEmpty()) ? 0 : mi.inData.get("DLIX") as Integer


    boolean validInput = validateInput(iCONO, iDLIX) //Check if inputs are valid.
    if (validInput) {
      DBAction query = database.table("MHDISL").index("00").selection("URITNO", "URRIDL", "URRIDN").build() //Get delivery related information
      DBContainer container = query.getContainer()
      container.set("URCONO", iCONO as Integer)
      container.set("URDLIX", iDLIX as Integer)
      query.readAll(container, 2,maxPageSize, deliveryLinesDetails)
      setOutput()
    } else {
      return
    }
  }
  Closure<?> deliveryLinesDetails = {
    DBContainer container ->
      String RIDN = container.get("URRIDN")
      String RIDL = container.get("URRIDL")
      String ITNO = container.get("URITNO")
      String ORNR = getTemporaryOrderNumber(RIDN)
      if(ORNR!=null&&!ORNR.trim().isEmpty()) {
        getPlayerLineDetails(ORNR, RIDL, ITNO)
      }else
      {
        mi.error("Temporary Order not found for the order " + RIDN)
        return false
      }
  }

  /**
   *Validate inputs
   * @params int CONO , int DLIX
   * @return boolean
   */
  private boolean validateInput(int iCONO, int iDLIX) {
    //Validate Company Number
    def params = ["CONO": iCONO.toString().trim()]
    def callback = {
      Map < String,
        String > response ->
        if (response.CONO == null) {
          mi.error("Invalid Company Number " + iCONO)
          return false
        }
    }
    miCaller.call("MNS095MI", "Get", params, callback)

    //Validate Delivery Number
    def paramsDLIX = ["CONO": iCONO.toString().trim(), "DLIX": iDLIX.toString().trim()]
    def callbackDLIX = {
      Map < String,
        String > response ->
        if (response.DLIX == null) {
          mi.error("Invalid delivery Number " + iDLIX)
          return false
        }
    }
    miCaller.call("MWS410MI", "GetHead", paramsDLIX, callbackDLIX)

    return true

  }

  /**
   *Get Temporary Order Number for the final Order Number
   * @params String RIDN
   * @return String
   */
  private String getTemporaryOrderNumber(String RIDN) {
    String ORNO = ""
    def params = ["CONO": iCONO.toString().trim(),"ORNO":RIDN.toString().trim()]
    def callback = {
      Map < String,
        String > response ->
        if (response.ORNO != null) {
          ORNO = response.ORNO
        }
    }
    miCaller.call("OIS275MI", "GetTmpOrderStat", params, callback)
    return ORNO
  }


  /**
   * Get Player Line Details
   * @params String ORNR , String RIDL, String ITNO
   * @return void
   */
  private void getPlayerLineDetails(String ORNR, String RIDL, String ITNO) {
    DBAction query = database.table("EXTSTL").index("30").selection("EXPLID", "EXWHLO", "EXUDF3", "EXORQT", "EXORNO", "EXPONR", "EXITNO","EXORNR").build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", iCONO)
    container.set("EXDIVI", iDIVI)
    container.set("EXORNR", ORNR)
    container.set("EXPONR", RIDL as Integer)
    container.set("EXPOSX", 0)
    container.set("EXITNO", ITNO)

    mi.error("HERE ITNO: "+ITNO+" RIDL: "+RIDL)
    query.readAll(container, 6, getPlayerLineDetailsResult)
  }
  Closure < ? > getPlayerLineDetailsResult = {
    DBContainer containerResult ->
      String warehouse = containerResult.get("EXWHLO")
      String playerID = containerResult.get("EXPLID")
      String userDefinedField3 = containerResult.get("EXUDF3")
      String orderQuantity = containerResult.get("EXORQT")
      String orderNumber = containerResult.get("EXORNO")
      String temporaryOrderNumber = containerResult.get("EXORNR")
      String lineNumber = containerResult.get("EXPONR")
      String itemNumber = containerResult.get("EXITNO")
      String Header = playerID+","+temporaryOrderNumber.trim() + "," + lineNumber.trim()

      orderDetails = new HashMap < String,
        String > ()
      if (orderHeader.containsKey(Header)) {
        orderDetails = orderHeader.get(Header)
        orderHeader.remove(Header)
      }


      orderDetails.put("WHLO", warehouse)
      orderDetails.put("PLID", playerID)
      orderDetails.put("UDF3", "")
      orderDetails.put("ORQT", orderQuantity)
      orderDetails.put("ITNO", itemNumber)
      orderHeader.put(Header, orderDetails)

      getPlayerHeaderDetails(playerID,temporaryOrderNumber,lineNumber)
      updatePlayerLineDeliveryDetails(playerID, lineNumber, itemNumber,temporaryOrderNumber)
      updateUserDefinedFieldDetails(playerID, temporaryOrderNumber, lineNumber, itemNumber, "")
      getAllocationDetails(playerID,orderNumber , lineNumber, itemNumber,temporaryOrderNumber)
  }

  /**
   *Get header level details of the player from EXTSTH table.
   * @params String PLID,String ORNR, String RIDL
   * @return
   */
  private void getPlayerHeaderDetails(String PLID,String ORNR, String RIDL) {
    String Header = PLID+","+ORNR + "," + RIDL
    DBAction query = database.table("EXTSTH").index("00").selection("EXPLNM", "EXPLNU", "EXTEAM", "EXLEAG","EXORNO").build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", iCONO)
    container.set("EXDIVI", iDIVI)
    container.set("EXPLID", PLID)
    if (query.read(container)) {
      orderDetails = new HashMap < String,String > ()
      if (orderHeader.containsKey(Header)) {
        orderDetails = orderHeader.get(Header)
        orderHeader.remove(Header)
      }
      String playerNumber = container.get("EXPLNU")
      String team = container.get("EXTEAM")
      String league = container.get("EXLEAG")
      String playerName = container.get("EXPLNM")
      String orderNumber = container.get("EXORNO")
      orderDetails.put("PLNU", playerNumber)
      orderDetails.put("TEAM", team)
      orderDetails.put("LEAG", league)
      orderDetails.put("PLNM", playerName)
      orderDetails.put("ORNO", orderNumber)
      orderHeader.put(Header, orderDetails)
    }

  }

  /**
   *Update EXTSTL table with delivery Information
   * @params  String PLID,  String RIDL, String ITNO,String ORNR
   * @return
   */
  private void updatePlayerLineDeliveryDetails(String PLID, String RIDL, String ITNO,String ORNR) {
    DBAction query = database.table("EXTSTL").index("00").build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", iCONO)
    container.set("EXDIVI", iDIVI)
    container.set("EXPLID", PLID)
    container.set("EXORNR", ORNR)
    container.set("EXPONR", RIDL as Integer)
    container.set("EXITNO", ITNO)
    query.readLock(container, updateDeliveryInformation)
  }

  Closure<?> updateDeliveryInformation = {
    LockedResult lockedResult ->
      lockedResult.set("EXDLIX", iDLIX)
      lockedResult.update()
  }


  /**
   *Update EXTSTL table with UDF3 field
   * @params String PLID , String ORNO, String PONR, String ITNO, String UDF3
   * @return
   */
  private void updateUserDefinedFieldDetails(String PLID, String ORNR, String PONR, String ITNO, String UDF3) {
    userDefinedField3 = UDF3
    DBAction query = database.table("EXTSTL").index("00").build()
    DBContainer container = query.getContainer()
    container.set("EXCONO", iCONO)
    container.set("EXDIVI", iDIVI)
    container.set("EXPLID", PLID)
    container.set("EXORNR", ORNR)
    container.set("EXPONR", PONR as Integer)
    container.set("EXITNO", ITNO)
    query.readLock(container, updateCallBack)

  }

  Closure<?> updateCallBack = {
    LockedResult lockedResult ->
      lockedResult.set("EXUDF3", userDefinedField3)
      lockedResult.update()
  }


  /**
   * Get Allocation Details from MITALO table
   * @params String PLID,String RIDN , int RIDL,String ITNO, String ORNR
   * @return
   */
  private void getAllocationDetails(String PLID,String RIDN, String RIDL, String ITNO,String ORNR) {
    String WHLO = ""
    String Header = PLID+","+ORNR + "," + RIDL
    orderDetails = new HashMap < > ()
    if (orderHeader.containsKey(Header)) {
      orderDetails = orderHeader.get(Header)
      WHLO = orderDetails.get("WHLO")
    }

    DBAction query = database.table("MITALO").index("70").selection("MQWHSL", "MQALQT", "MQRIDN", "MQRIDL").build()
    DBContainer container = query.getContainer()
    container.set("MQCONO", iCONO)
    container.set("MQWHLO", WHLO)
    container.set("MQITNO", ITNO)
    container.set("MQTTYP", 31)
    container.set("MQRIDN", RIDN)
    container.set("MQRIDL", RIDL as Integer)
    query.readAll(container, 6, getAllocationDetails)
  }


  Closure<?> getAllocationDetails = {

    DBContainer container ->

      String WHSL = container.get("MQWHSL")
      String ALQT = container.get("MQALQT")
      String RIDN = container.get("MQRIDN")
      String RIDL = container.get("MQRIDL")
      String ORNR = getTemporaryOrderNumber(RIDN)
      String Header = ORNR.trim() + "," + RIDL.trim()
      allocationDeatils = new HashMap < > ()
      if (allocationHeader.containsKey(Header)) {
        allocationDeatils = allocationHeader.get(Header)
        allocationHeader.remove(Header)
      }

      allocationDeatils.put("WHSL", WHSL)
      allocationDeatils.put("ALQT", ALQT)
      allocationHeader.put(Header, allocationDeatils)
  }

  /**
   *Set API Output
   * @params
   * @return
   */
  private void setOutput() {
    double quantity = 0
    for (String key: orderHeader.keySet()) {
      String PLID = key.split(",")[0]
      String RIDN = key.split(",")[1]
      String RIDL = key.split(",")[2]
      orderDetails = orderHeader.get(key)
      String ITNO = orderDetails.get("ITNO")
      String WHLO = orderDetails.get("WHLO")
      String UDF3 = orderDetails.get("UDF3")
      String ORQT = orderDetails.get("ORQT")
      String PLNM = orderDetails.get("PLNM")
      String PLNU = orderDetails.get("PLNU")
      String TEAM = orderDetails.get("TEAM")
      String LEAG = orderDetails.get("LEAG")
      String ORNO = orderDetails.get("ORNO")
      String WHSL =""
      String ALQT = "0"


      allocationDeatils = allocationHeader.get(RIDN.trim()+","+RIDL.trim())
      if(allocationDeatils!=null)
      {
        WHSL = allocationDeatils.get("WHSL")
        ALQT = allocationDeatils.get("ALQT")
      }
      if (UDF3 == null) UDF3 = ""
      if (ALQT == null || ALQT.trim().isEmpty()) ALQT = "0"
      if (ORQT == null || ORQT.trim().isEmpty()) ORQT = "0"

      mi.outData.put("ORNO", ORNO)
      mi.outData.put("RIDL", RIDL)
      mi.outData.put("WHSL", WHSL)
      mi.outData.put("ITNO", ITNO)
      mi.outData.put("WHLO", WHLO)
      mi.outData.put("DLIX", iDLIX as String)

      double doubleALQT = Double.parseDouble(ALQT)
      double doubleORQT = Double.parseDouble(ORQT)
      if (quantity <= doubleALQT) {
        mi.outData.put("ORQT", ORQT)
        if (UDF3.trim().isEmpty() && quantity < doubleALQT) {
          quantity = quantity + doubleORQT
          mi.outData.put("PLID", PLID)
          mi.outData.put("PLNM", PLNM)
          mi.outData.put("PLNU", PLNU)
          mi.outData.put("TEAM", TEAM)
          mi.outData.put("LEAG", LEAG)
          updateUserDefinedFieldDetails(PLID, RIDN, RIDL, ITNO, WHSL)
        }
      }
      mi.write()
    }
  }

}
