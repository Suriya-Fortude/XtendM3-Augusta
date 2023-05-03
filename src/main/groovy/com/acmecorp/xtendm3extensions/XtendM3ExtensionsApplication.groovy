/**
 * README
 * This extension is being used to  get the features and option values from M3
 *
 * Name: EXT010MI.GetFeatureOptn
 * Description: Get the features and option values from M3
 * Date	      Changed By                      Description
 *20230322  SuriyaN@fortude.co              Get the features and option values from M3
 *
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class GetFeatureOptn extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program
  private final MICallerAPI miCaller

  private String iFACI, iSTRT, iPRNO, iITNO, SIDI, FTID, OPTN, QBMVC1, QBMVC2, QBMVC3, QBMVC4, QBMVC5, QBMVC6, QBMVC7, QBMVC8, QBMVC9, QAMCC1, QAMCC2, QAMCC3, QAMCC4, QAMCC5, QAMCC6, QAMCC7, QAMCC8,
                 QAMCC9, ITCL, feature2, option2 = ""
  private int iCONO
  private boolean  isFeatureAndOptionFound = true

  HashMap < String, String > sequenceSelectID = new HashMap < String, String > ()

  GetFeatureOptn(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
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
  void main() {
    iCONO = (mi.inData.get("CONO") == null || mi.inData.get("CONO").trim().isEmpty()) ? program.LDAZD.CONO as Integer : mi.inData.get("CONO") as Integer
    iFACI = (mi.inData.get("FACI") == null || mi.inData.get("FACI").trim().isEmpty()) ? "" : mi.inData.get("FACI")
    iSTRT = (mi.inData.get("STRT") == null || mi.inData.get("STRT").trim().isEmpty()) ? "" : mi.inData.get("STRT")
    iPRNO = (mi.inData.get("PRNO") == null || mi.inData.get("PRNO").trim().isEmpty()) ? "" : mi.inData.get("PRNO")
    iITNO = (mi.inData.get("ITNO") == null || mi.inData.get("ITNO").trim().isEmpty()) ? "" : mi.inData.get("ITNO")

    validateInput()
    getMaterialAndOperationDetails()
    getMatrixIdentityDetails(SIDI)
    getOperationOnMaterialLines()
    if(ITCL.trim().equals("PHCN") &&FTID == null && OPTN == null)
    {
      getFeatureOptnWithSelectionType()
      getFeatureOptnWithItemNumber()
    }
    setOutput()
  }

  /**
   *Validate Records
   * @params
   * @return
   */
  def validateInput() {

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

    //Validate Item Number
    params = ["CONO": iCONO.toString().trim(), "ITNO": iITNO.toString().trim()]
    callback = {
      Map < String,
        String > response ->
        if (response.ITNO == null) {
          mi.error("Invalid Item Number " + iITNO)
          return false
        } else {
          ITCL = response.ITCL
        }
    }
    miCaller.call("MMS200MI", "Get", params, callback)

    //Validate Facility Number
    DBAction query = database.table("CFACIL").index("00").build()
    DBContainer container = query.getContainer()
    container.set("CFCONO", iCONO)
    container.set("CFFACI", iFACI)
    if (!query.read(container)) {
      mi.error("Invalid Facility Number " + iFACI)
      return false
    }

    //Validate Product Number
    params = ["CONO": iCONO.toString().trim(), "PRNO": iPRNO.toString().trim(), "FACI": iFACI.toString().trim(), "STRT": iSTRT.toString().trim()]
    callback = {
      Map < String,
        String > response ->
        if (response.PRNO == null) {
          mi.error("Invalid Product Number - Structure Type  " + iPRNO+" "+iSTRT)
          return false
        }
    }
    miCaller.call("PDS001MI", "Get", params, callback)

  }

  /**
   *Get Sequence number and selection id component
   * @params
   * @return
   */
  def getMaterialAndOperationDetails() {
    DBAction query = database.table("MPDMAT").selection("PMMSEQ", "PMSIDI", "PMOPNO").index("00").build()
    DBContainer container = query.getContainer()
    container.set("PMCONO", iCONO)
    container.set("PMFACI", iFACI)
    container.set("PMPRNO", iPRNO)
    container.set("PMSTRT", iSTRT)
    query.readAll(container, 4, resultset)

  }

  Closure < ? > resultset = {
    DBContainer container ->
      String sequenceNumber = container.get("PMMSEQ")
      String selectIDComponent = container.get("PMSIDI")
      sequenceSelectID.put(selectIDComponent.trim(), sequenceNumber.trim())
      getMatrixDetails(selectIDComponent)

  }

  /**
   *Get matrix identity values
   * @params String SIDI
   * @return
   */
  void getMatrixIdentityDetails(String SIDI) {
    DBAction query = database.table("MPMXID").selection("QAMCC1", "QAMCC2", "QAMCC3", "QAMCC4", "QAMCC5", "QAMCC6", "QAMCC7", "QAMCC8", "QAMCC9").index("00").build()
    DBContainer container = query.getContainer()
    container.set("QACONO", iCONO)
    container.set("QAMXID", SIDI)
    if (query.read(container)) {
      if(isFeatureAndOptionFound) {
        QAMCC1 = container.get("QAMCC1")
        QAMCC2 = container.get("QAMCC2")
        QAMCC3 = container.get("QAMCC3")
        QAMCC4 = container.get("QAMCC4")
        QAMCC5 = container.get("QAMCC5")
        QAMCC6 = container.get("QAMCC6")
        QAMCC7 = container.get("QAMCC7")
        QAMCC8 = container.get("QAMCC8")
        QAMCC9 = container.get("QAMCC9")
      }else
      {
        feature2 = container.get("QAMCC2")
      }

    }

  }

  /**
   *Get matrix identity values
   * @params String SIDI
   * @return
   */
  void getMatrixDetails(String SIDI) {

    ExpressionFactory expression = database.getExpressionFactory("MPMXVA")
    if(isFeatureAndOptionFound) {
      expression = expression.eq("QBMRC1", iITNO)
    }else
    {
      expression = expression.like("PMMTNO", iITNO+"%")
    }
    DBAction query = database.table("MPMXVA").matching(expression).selection("QBMXID", "QBMVC1", "QBMVC2", "QBMVC3", "QBMVC4", "QBMVC5", "QBMVC6", "QBMVC7", "QBMVC8", "QBMVC9").index("00").build()
    DBContainer container = query.getContainer()
    container.set("QBCONO", iCONO)
    container.set("QBMXID", SIDI)
    query.readAll(container, 2, resultset_matrixDetails)

  }

  Closure < ? > resultset_matrixDetails = {
    DBContainer container ->
      SIDI = container.get("QBMXID").toString().trim()
      if(isFeatureAndOptionFound) {
        QBMVC1 = container.get("QBMVC1")
        QBMVC2 = container.get("QBMVC2")
        QBMVC3 = container.get("QBMVC3")
        QBMVC4 = container.get("QBMVC4")
        QBMVC5 = container.get("QBMVC5")
        QBMVC6 = container.get("QBMVC6")
        QBMVC7 = container.get("QBMVC7")
        QBMVC8 = container.get("QBMVC8")
        QBMVC9 = container.get("QBMVC9")
      }else
      {
        option2 = container.get("QBMVC2")
      }

  }

  /**
   *Get operation on material MPDOMA details
   * @params
   * @return
   */
  void getOperationOnMaterialLines() {
    DBAction query = database.table("MPDOMA").selection("PNFTID", "PNOPTN").index("00").build()
    DBContainer container = query.getContainer()
    container.set("PNCONO", iCONO)
    container.set("PNFACI", iFACI)
    container.set("PNPRNO", iPRNO)
    container.set("PNSTRT", iSTRT)
    container.set("PNMSEQ", sequenceSelectID.get(SIDI) as Integer)
    query.readAll(container, 5, resultset_operationDetails)
  }

  Closure < ? > resultset_operationDetails = {
    DBContainer container ->
      FTID = container.get("PNFTID")
      OPTN = container.get("PNOPTN")
  }

  void getFeatureOptnWithSelectionType() {
    ExpressionFactory expression = database.getExpressionFactory("MPDOMA")
    expression = expression.eq("PNFTID", QAMCC2).and(expression.eq("PNOPTN", QBMVC2))
    DBAction query = database.table("MPDOMA").matching(expression).selection("PNFTID", "PNOPTN").index("00").build()
    DBContainer container = query.getContainer()
    container.set("PNCONO", iCONO)
    container.set("PNFACI", iFACI)
    container.set("PNPRNO", iPRNO)
    container.set("PNSTRT", iSTRT)
    container.set("PNOTYP", "3")
    query.readAll(container, 5, resultset_operationDetails_withSelectionType)
  }

  Closure < ? > resultset_operationDetails_withSelectionType = {
    DBContainer container ->
      FTID = container.get("PNFTID")
      OPTN = container.get("PNOPTN")
  }

  void getFeatureOptnWithItemNumber() {
    if (FTID == null && OPTN == null) {
      isFeatureAndOptionFound = false
      iITNO = "0P9HB"
      ExpressionFactory expression = database.getExpressionFactory("MPDMAT")
      expression = expression.like("PMMTNO", iITNO+"%")
      DBAction query = database.table("MPDMAT").matching(expression).selection("PMMTNO").index("00").build()
      DBContainer container = query.getContainer()
      container.set("PMCONO", iCONO)
      container.set("PMFACI", iFACI)
      container.set("PMPRNO", iPRNO)
      container.set("PMSTRT", iSTRT)
      query.readAll(container, 4, resultset_operationDetails_withItemNumber)
    }
  }

  Closure < ? > resultset_operationDetails_withItemNumber = {
    DBContainer container ->
      FTID = QAMCC2
      OPTN = QBMVC2
  }

  /**
   *Set Output
   * @params
   * @return
   */
  void setOutput() {

    mi.outData.put("MCC1", QAMCC1)
    mi.outData.put("MCC2", QAMCC2)
    mi.outData.put("MCC3", QAMCC3)
    mi.outData.put("MCC4", QAMCC4)
    mi.outData.put("MCC5", QAMCC5)
    mi.outData.put("MCC6", QAMCC6)
    mi.outData.put("MCC7", QAMCC7)
    mi.outData.put("MCC8", QAMCC8)
    mi.outData.put("MCC9", QAMCC9)
    mi.outData.put("MVC1", QBMVC1)
    mi.outData.put("MVC2", QBMVC2)
    mi.outData.put("MVC3", QBMVC3)
    mi.outData.put("MVC4", QBMVC4)
    mi.outData.put("MVC5", QBMVC5)
    mi.outData.put("MVC6", QBMVC6)
    mi.outData.put("MVC7", QBMVC7)
    mi.outData.put("MVC8", QBMVC8)
    mi.outData.put("MVC9", QBMVC9)
    mi.outData.put("FTID", FTID)
    mi.outData.put("OPTN", OPTN)
    mi.write()
  }
}
