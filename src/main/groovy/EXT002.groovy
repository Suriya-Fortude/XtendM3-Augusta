import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class EXT002 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final ProgramAPI program

  EXT002(LoggerAPI logger, MICallerAPI miCaller, ProgramAPI program, DatabaseAPI database) {
    this.logger = logger
    this.miCaller = miCaller
    this.database = database
    this.program = program
  }

  //Global Variables
  int cono, maxPageSize = 1000000,atpDate180 = 0,atpDate185=0,atpQty180=0,atpQty185= 0
  List<String> phCrownProductGroup = new ArrayList<String>()
  List<String> phCrownWarehouse = new ArrayList<String>()
  List<String> validAccessControlObjects = new ArrayList<String>()
  HashMap<String, String> warehouseForACO = new HashMap<String, String>()
  HashMap<String, Integer> exportItemList = new HashMap<String, Integer>()
  String workingDaySerialNumber="",lowerStatus="",higherStatus = "", atpWarehouse = "",divi = "",atpDate="",threshHoldQuantity = ""
  boolean allTranslationsAvailable = true,availableToPromiseRecordsFound = false

  void main() {
    cono =  program.LDAZD.cono
    divi =  program.LDAZD.divi
    deleteCustomTableData()
    getTranslationValues()
    logger.info("Suriya phCrownProductGroup: "+phCrownProductGroup)
    logger.info("Suriya validAccessControlObjects: "+validAccessControlObjects)
    logger.info("Suriya warehouseForACO: "+warehouseForACO)
    if (allTranslationsAvailable) {
      getItems()
      setUpdateLoadFlag()
      logger.error("Total Item Size: "+exportItemList.size())
    }else
    {
      logger.error("ATP Inventory upload aborted due to missing translation values in CRS881")
    }
  }

  /**
   * Get All necessary translation values from CRS881
   * @params
   * @return void
   */
  private void getTranslationValues() {

    //Get PH Crown Product group list.
    def params = ["CONO": cono.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "PHCN"]
    def callback = {
      Map<String,
        String> response ->
        if (response.MBMD != null) {
          phCrownProductGroup = new ArrayList<>(Arrays.asList(response.MBMD.split(",")))
        } else {
          allTranslationsAvailable = false
          logger.error("PH Crown Product Groups not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    //Get valid warehouses for items with PH Crown product group.
    params = ["CONO": cono.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "PHCN_WHLO"]
    callback = {
      Map<String,
        String> response ->
        if (response.MBMD != null) {
          phCrownWarehouse  = new ArrayList<>(Arrays.asList(response.MBMD.split(",")))
        } else {
          allTranslationsAvailable = false
          logger.error("PH Crown Product Groups warehouse not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    //Get valid Access Control Object for items.
    params = ["CONO": cono.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"ACO"]
    callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          validAccessControlObjects =  new ArrayList<>(Arrays.asList(response.MBMD.split(",")))
        }else
        {
          allTranslationsAvailable = false
          logger.error("Valid Access Control Objects not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    //Get Valid warehouse for each access control object received in previous steP
    if(validAccessControlObjects!=null&&validAccessControlObjects.size()>0)
    {
      for(int i=0;i<validAccessControlObjects.size();i++)
      {
        String ACO = validAccessControlObjects[i]
        params = ["CONO": cono.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"ACO_WHLO_"+ACO.trim()]
        callback = {
          Map < String,
            String > response ->
            if (response.MBMD != null) {
              warehouseForACO.put(ACO.trim(),response.MBMD.trim())
            }else{
              allTranslationsAvailable = false
              logger.error("Valid warehouse not defined for ACO: "+ACO+". Please configure the same in CRS881")
              return
            }
        }
        miCaller.call("CRS881MI", "GetTranslData", params, callback)
      }
    }

    //Get lower status value applicable for records in MITMAS and MITBAL
    params = ["CONO": cono.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"STAL"]
    callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          lowerStatus = response.MBMD
        }else
        {
          allTranslationsAvailable = false
          logger.error("Valid lower Status not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    //Get higher status value applicable for records in MITMAS and MITBAL
    params = ["CONO": cono.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"STAH"]
    callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          higherStatus = response.MBMD
        }else
        {
          allTranslationsAvailable = false
          logger.error("Valid higher Status not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    //Get threshHold Quantity
    params = ["CONO": cono.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"THRESH HOLD"]
    callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          threshHoldQuantity = response.MBMD
        }else
        {
          allTranslationsAvailable = false
          logger.error("ThreshHold Quantity not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)
  }

  /**
   * Delete all records from EXTPQT and EXTPDT table
   * @params
   * @return void
   */
  private void deleteCustomTableData() {
    logger.info("Delet Custom table data function triggered.")
    //List all records from EXTPQT Table
    def params = ["CONO": cono.toString().trim()]
    def callback = {
      Map<String,
        String> response ->
        DBAction query = database.table("EXTPDT").index("00").build()
        DBContainer container = query.getContainer()
        container.set("EXCONO", cono)
        container.set("EXWHLO", response.WHLO)
        container.set("EXITNO", response.ITNO)
        if (query.read(container)) {
          query.readLock(container, deleteAllATPDateRecords)
        }
    }
    miCaller.call("EXT200MI", "LstATPDate", params, callback)

    //List all records from EXTPDT Table
    params = ["CONO": cono.toString().trim()]
    callback = {
      Map<String,
        String> response ->
        DBAction query = database.table("EXTPQT").index("00").build()
        DBContainer container = query.getContainer()
        container.set("EXCONO", cono)
        container.set("EXWHLO", response.WHLO)
        container.set("EXITNO", response.ITNO)
        if (query.read(container)) {
          query.readLock(container, deleteAllATPQuantityRecords)
        }
    }
    miCaller.call("EXT201MI", "LstATPQuantity", params, callback)
  }


  /**
   * check if access control object invalid for the product group
   * @params Product group, access control object
   * @return boolean
   */
  private boolean checkIfACOInvalidForProductGroup(String ITCL,String ACO) {

    if (ITCL == null || ITCL.trim().isEmpty()) ITCL = ''
    if (ACO == null || ACO.trim().isEmpty()) ACO = ''

    boolean invalidACO = false

    def params = ["CONO": cono.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "INVACO_" + ITCL.trim()]
    def callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) { //If translation product group and translation contains access control object, return true
          String accessControlObjectList = response.MBMD
          if(accessControlObjectList.contains(ACO))
          {
            invalidACO = true
          }
        }
    }

    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    return invalidACO
  }

  /**
   * Get all matching items
   * @params
   * @return
   */
  private void getItems()
  {
    ExpressionFactory expression = database.getExpressionFactory("MITMAS")
    //noinspection ChangeToOperator
    expression = expression.gt("MMSTAT",lowerStatus).and(expression.lt("MMSTAT",higherStatus))
    DBAction query = database.table("MITMAS").index("00").matching(expression).selection("MMITCL","MMITNO","MMACRF").build()
    DBContainer container = query.getContainer()
    container.set("MMCONO", cono)
    query.readAll(container, 1,maxPageSize, getItems)
  }

  Closure < ? > getItems = {
    DBContainer containerResult ->
      String productGroup = containerResult.getString("MMITCL")
      String accessControlObject = containerResult.getString("MMACRF")
      String itemNumber = containerResult.getString("MMITNO")
      String warehouse = (warehouseForACO.get(accessControlObject)==null)?"":warehouseForACO.get(accessControlObject)


      if (phCrownProductGroup.contains(productGroup)) {
        for (int i = 0; i < phCrownWarehouse.size(); i++) {
          boolean itemAvailable = itemAvailableInWarehouse(phCrownWarehouse[i], itemNumber)
          if (itemAvailable) {
            filterItemsAndUploadCrownItems(phCrownWarehouse[i], itemNumber)
          }
        }
      }else {
        boolean invalidItem = false

        def params = ["CONO": cono.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "OVRD_WHLO_" + itemNumber]
        def callback = {
          Map<String,
            String> response ->
            if (response.MBMD != null && warehouse.trim().isEmpty()) {
              warehouse = response.MBMD
            }
        }
        miCaller.call("CRS881MI", "GetTranslData", params, callback)

        String[] warehouses = warehouse.split(",")

        for (int i = 0; i < warehouses.length; i++) {
          //Keep checking for all warehouses until item is valid in at least one warehouse
          invalidItem = checkIfInvalidItem(itemNumber, warehouse, accessControlObject, productGroup)
          if (!invalidItem) //If item valid and is present in atleast one warehouse, then stop validation.
          {
            warehouse = warehouses[i]
            availableToPromiseRecordsFound = false
            filterItemsAndUploadQuantity(warehouse, itemNumber)
            filterItemsAndUploadDate(warehouse, itemNumber)
          }
        }

      }
  }


  /**
   * Check Item is valid
   * @params  itemnumber, warehouse, accessControlObject, product group
   * @return boolean
   */
  private boolean checkIfInvalidItem(String itemNumber, String warehouse, String accessControlObject, String productGroup) {
    boolean invalidItem = false

    if (validAccessControlObjects != null && validAccessControlObjects.size() > 0) {
      //If the Access Control Object of the item is a part of invalid access control object list for the product group, then set as invalid item.
      invalidItem = !validAccessControlObjects.contains(accessControlObject)
    }

    //If item number contains "." set invalid item
    if (itemNumber.contains(".")) {
      invalidItem = true
    }

    //If the item does not exists for the access control object warehouse or override warehouse in MMS002, then set invalid item.
    def params = ["CONO": cono.toString().trim(), "WHLO": warehouse.toString().trim(), "ITNO": itemNumber.toString().trim()]
    def callback = {
      Map < String,
        String > response ->
        if (response.ITNO == null) {
          invalidItem = true
        }
    }
    miCaller.call("MMS200MI", "GetItmWhsBasic", params, callback)

    //Get Invalid Access control object for each product group.
    invalidItem = checkIfACOInvalidForProductGroup(productGroup,accessControlObject)

    return invalidItem
  }
  /**
   * Check Item availble in warehouse
   * @params warehouse, item
   * @return
   */
  private boolean itemAvailableInWarehouse(String warehouse,String itemNumber){
    DBAction query = database.table("MITBAL").index("00").selection("MBWHLO").build()
    DBContainer container = query.getContainer()
    container.set("MBCONO", cono)
    container.set("MBITNO", itemNumber)
    container.set("MBWHLO", warehouse)
    return query.read(container)
  }

  /**
   * Get item quantity in the warehouse.
   * @params ItemNumber and warehouse
   * @return Integer
   */
  private Integer getWarehouseQuantity(String warehouse,String itemNumber){
    ExpressionFactory expression = database.getExpressionFactory("MITBAL")
    expression = expression.gt("MBSTAT",lowerStatus).and(expression.lt("MBSTAT",higherStatus))
    DBAction query = database.table("MITBAL").index("00").matching(expression).selection("MBSTQT","MBREQT").build()
    DBContainer container = query.getContainer()
    container.set("MBCONO", cono)
    container.set("MBITNO", itemNumber)
    container.set("MBWHLO", warehouse)
    if(query.read(container))
    {
      int onHandApproveQuantity = container.getInt("MBSTQT")
      int reservedQuantity = container.getInt("MBREQT")
      return  onHandApproveQuantity-reservedQuantity
    }else
    {
      return 0
    }
  }

  /**
   * Get planning time fence for the item in warehouse
   * @params ItemNumber and warehouse
   * @return
   */
  private Integer getPlanningTimeFence(String warehouse, String itemNumber) {
    DBAction query = database.table("MITBAL").index("00").selection("MBPFTM").build()
    DBContainer container = query.getContainer()
    container.set("MBCONO", cono)
    container.set("MBITNO", itemNumber)
    container.set("MBWHLO", warehouse)
    if (query.read(container)) {
      return container.getInt("MBPFTM")
    } else {
      return 0
    }
  }

  /**
   * Filter PH Crown items items and add data to quantity table
   * @params ItemNumber and warehouse
   * @return
   */
  private filterItemsAndUploadCrownItems(String warehouse,String itemNumber){
    int quantity = getWarehouseQuantity(warehouse,itemNumber)

    def params = ["CONO": cono.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.substring(3,15).toString().trim()]
    def callback = {
      Map < String,
        String > response ->
        if(response.VAL9!=null)
        {
          int currentQuantity = Integer.parseInt(response.VAL9)
          int thqty = Integer.parseInt(threshHoldQuantity)
          if(quantity!=currentQuantity&&((quantity<thqty)||(currentQuantity<thqty)))
          {
            updateItemsToATPQuantityTable(itemNumber.substring(3,15),warehouse,quantity)
          }
        }
        else if(response.errorMessage != null)
        {
          logger.error("Error adding Item Quantity for item: "+itemNumber+". Failed with error message: "+response.errorMessage)
        }else
        {
          addItemsToATPQuantityTable(itemNumber.substring(3,15),warehouse,quantity)
        }
    }
    miCaller.call("EXT201MI", "GetATPQuantity", params, callback)


  }
  /**
   * Filter items and add data to quantity table
   * @params ItemNumber and warehouse
   * @return
   */
  private filterItemsAndUploadQuantity(String warehouse,String itemNumber){
    int quantity=0
    DBAction query = database.table("MITATP").index("00").selection("MAAVTP").build() //Key fields to MITATP and get available quantity.
    DBContainer container = query.getContainer()
    container.set("MACONO", cono)
    container.set("MAWHLO", warehouse)
    container.set("MAITNO", itemNumber)
    container.set("MAPLDT", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())

    if (query.read(container)) {
      quantity = container.getInt("MAAVTP")

      if (quantity > 0) {
        def params = ["CONO": cono.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim()]
        def callback = {
          Map < String,
            String > response ->
            if(response.VAL9!=null)
            {
              int currentQuantity = Integer.parseInt(response.VAL9)
              int thshqty = Integer.parseInt(threshHoldQuantity)
              if(quantity!=currentQuantity&&((quantity<thshqty)||(currentQuantity<thshqty)))
              {
                addItemsToATPQuantityTable(itemNumber, warehouse, quantity)
              }
            }
            else if(response.errorMessage != null)
            {
              logger.error("Error adding Item Quantity for item: "+itemNumber+". Failed with error message: "+response.errorMessage)
            }
        }
        miCaller.call("EXT201MI", "GetATPQuantity", params, callback)

      }
      if (quantity == 0 && (warehouse.trim().equals("180") || warehouse.trim().equals("185"))) //If quantity is 0 and warehouse is 180 or 185, check for other dates as well.
      {
        query.readAll(container, 3,maxPageSize, readAvailableToPromiseQuantity)
      }
    }

  }

  /**
   * Get Planning Date from M3
   * @params planning Time Fence
   * @return String
   */
  private String getPlanningDate(int planningTimeFence){
    workingDaySerialNumber = "0"
    String planningDate = "0"
    def params = ["CONO": cono.toString().trim(), "YMD8": LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))]
    def callback = {
      Map<String,
        String> response ->
        workingDaySerialNumber = response.WDNO
        String deliveryDay = response.DDAY
        if(deliveryDay==null||deliveryDay.trim().isEmpty()) deliveryDay = "0"
        if (!deliveryDay.trim()==("1")) {
          def query = database.table("CSYCAL").index("00").selection("CDDDAY", "CDWDNO").build()
          def  container = query.getContainer()
          container.set("CDCONO", cono)
          container.set("CDDIVI", divi)
          query.readAll(container, 2, maxPageSize, getSystemCalendarDate)
        }
    }
    miCaller.call("CRS900MI", "GetSysCalDate", params, callback)
    int totalWorkingDay = planningTimeFence + Integer.parseInt(workingDaySerialNumber)

    params = ["CONO": cono.toString().trim(),"QERY":"CDYMD8 from CSYCAL where CDWDNO = '"+totalWorkingDay+"'  and CDDDAY = '1'"]
    callback = {
      Map < String,
        String > response ->
        if(response.REPL != null)
        {
          planningDate = response.REPL
          return
        }
    }
    miCaller.call("EXPORTMI", "Select", params, callback)
    return planningDate
  }
  /**
   * Filter items and add data to date table
   * @params Warehouse, itemnumber and quantity
   * @return
   */
  private filterItemsAndUploadDate(String warehouse,String itemNumber){
    availableToPromiseRecordsFound = false
    int planningTimeFence = getPlanningTimeFence(warehouse, itemNumber)
    DBAction query = database.table("MITATP").index("00").selection("MAAVTP","MAPLDT","MAAVTP","MAWHLO","MAITNO").build()
    DBContainer container = query.getContainer()
    container.set("MACONO", cono)
    container.set("MAWHLO", warehouse)
    container.set("MAITNO", itemNumber)

    String planningDate = getPlanningDate(planningTimeFence)
    //Add all valid items to EXTPDT table except for warehouse 180
    if(!warehouse.trim().equals("185")) {
      int quantity = 1
      query.readAll(container, 3, maxPageSize, readATPDateRecords)
      if(!availableToPromiseRecordsFound)
      {
        addItemsToATPDateTable(itemNumber, warehouse,quantity, planningDate)
      }
    }else if(warehouse.trim().equals("180"))
    {
      int count = 2
      for(int i=0;i<count;i++) // Run twice for warehouse 180 once and warehouse 185 once.
      {
        if(i==1) warehouse = "185"
        if (!availableToPromiseRecordsFound) {
          query.readAll(container, 3, maxPageSize, readATPDateRecords_warehouse_180_185)
        }
        if (!availableToPromiseRecordsFound) {
          int quantity = getWarehouseQuantity(warehouse, itemNumber)
          if (warehouse.trim() == "180") {
            atpQty180 = quantity
            atpDate180 = Integer.parseInt(planningDate)
          } else {
            atpQty185 = quantity
            atpDate185 = Integer.parseInt(planningDate)
          }
        }
        if (warehouse.trim() == "180") {
          availableToPromiseRecordsFound = false
          warehouse = "185"
        }else
        {
          warehouse=""
        }
      }
      int ATPQuantity = atpQty180 + atpQty185
      String ATPDate = atpDate180
      if (atpDate185 < atpDate180) {
        ATPDate = atpDate185
      }
      addItemsToATPDateTable(itemNumber, "180", ATPQuantity, ATPDate)
    }
  }
  /**
   * Add records to EXTPDT table for ECOM.
   * @params ItemNumber, warehouse and quantity
   * @return
   */
  private addItemsToATPDateTable(String itemNumber,String warehouse, int quantity, String planningDate)
  {
    DBAction action = database.table("EXTPDT").index("00").build()
    DBContainer query = action.getContainer()
    query.set("EXWHLO", warehouse)
    query.set("EXITNO", itemNumber)
    query.set("EXDATE", planningDate.substring(0,4)+"-"+planningDate.substring(4,6)+"-"+planningDate.substring(6,8)+" 00:00:00")
    query.set("EXUPDS", "1")
    query.set("EXCONO", cono)
    query.set("EXORQ9", quantity)
    query.set("EXRGDT", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())
    query.set("EXRGTM", LocalDateTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger())
    query.set("EXLMDT", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())
    query.set("EXLMTM", LocalDateTime.now().format(DateTimeFormatter.ofPattern("HHmmss")).toInteger())
    query.set("EXCHNO", 0)
    query.set("EXCHID", "MMZ311")
    action.insert(query)
  }
  /**
   * Add records to EXTPQT table for ECOM.
   * @params ItemNumber, warehouse and quantity
   * @return
   */
  private addItemsToATPQuantityTable(String itemNumber,String warehouse, int quantity)
  {
    def params = ["CONO": cono.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim(),"VAL9":quantity.toString().trim(),"UPDS":"1"]
    def callback = {
      Map < String,
        String > response ->
        if(response.errorMessage != null)
        {
          logger.error("Error adding Item Quantity for item: "+itemNumber+". Failed with error message: "+response.errorMessage)
        }
    }
    miCaller.call("EXT201MI", "AddATPQuantity", params, callback)
  }

  /**
   * Update records to EXTPQT table for ECOM.
   * @params ItemNumber, warehouse and quantity
   * @return
   */
  private updateItemsToATPQuantityTable(String itemNumber,String warehouse, int quantity)
  {
    def params = ["CONO": cono.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim(),"VAL9":quantity.toString().trim(),"UPDS":"1"]
    def callback = {
      Map < String,
        String > response ->
        if(response.errorMessage != null)
        {
          logger.error("Error adding Item Quantity for item: "+itemNumber+". Failed with error message: "+response.errorMessage)
        }
    }
    miCaller.call("EXT201MI", "UpdATPQuantity", params, callback)
  }

  Closure < ? > readAvailableToPromiseQuantity = {
    DBContainer containerResult ->
      int quantity = containerResult.getInt("MAAVTP")
      String itemNumber = containerResult.getString("MAITNO")
      String warehouse = containerResult.getString("MAWHLO")
      if(quantity>0)
      {
        addItemsToATPQuantityTable(itemNumber,warehouse,quantity)
        return
      }
  }
  Closure < ? > readATPDateRecords = {
    DBContainer containerResult ->
      availableToPromiseRecordsFound = true
      int quantity = containerResult.getInt("MAAVTP")
      String itemNumber = containerResult.getString("MAITNO")
      String warehouse = containerResult.getString("MAWHLO")
      int planningDate = containerResult.getInt("MAPLDT")

      if(quantity>0 && planningDate>0)
      {
        addItemsToATPDateTable(itemNumber,warehouse,quantity,planningDate+"")
      }

  }
  Closure < ? > readATPDateRecords_warehouse_180_185 = {
    DBContainer containerResult ->

      int quantity = containerResult.getInt("MAPQTY")
      String warehouse = containerResult.getString("MAWHLO")
      int planningDate = containerResult.getInt("MAPLDT")

      if(quantity>0 && planningDate>0)
      {
        if(warehouse.trim().equals("180")) {
          atpQty180 = containerResult.getInt("MAAVTP")
          atpDate180 = containerResult.getInt("MAPLDT")
          availableToPromiseRecordsFound = true
        }else
        {
          atpQty185 = containerResult.getInt("MAAVTP")
          atpDate185 = containerResult.getInt("MAPLDT")
          availableToPromiseRecordsFound = true
        }
      }
      //noinspection ChangeToOperator
      if(warehouse.trim().equals("180"))
      {
        atpWarehouse ="185"
        availableToPromiseRecordsFound = false
      }

      //  addItemsToATPDateTable(itemNumber,warehouse,quantity,planningDate+"")


  }
  Closure < ? > getSystemCalendarDate = {
    DBContainer containerResult ->
      String deliveryDate = containerResult.getString("CDDDAY")
      if(deliveryDate==null||deliveryDate.trim().isEmpty()) deliveryDate = ""
      if(deliveryDate.trim() == "1")
      {
        workingDaySerialNumber = containerResult.getString("CDWDNO")
        return
      }
  }
  Closure<?> deleteAllATPDateRecords = {
    LockedResult lockedResult ->
      logger.info("Deleted EXT200 records successfully")
      lockedResult.delete()
  }
  Closure<?> deleteAllATPQuantityRecords = {
    LockedResult lockedResult ->
      logger.info("Deleted EXT201 records successfully")
      lockedResult.delete()
  }

  /**
   * Set Update Load Flag
   * @params
   * @return
   */
  private setUpdateLoadFlag()
  {
    def params = ["CONO": cono.toString().trim(),"FILE":"ECOM_ATP","PK01":"UPDS"]
    def callback = {
      Map < String,
        String > response ->
    }
    miCaller.call("CUSEXTMI", "DelFieldValue", params, callback)

    params = ["CONO": cono.toString().trim(),"FILE":"ECOM_ATP","PK01":"UPDS"]
    callback = {
      Map < String,
        String > response ->
    }
    miCaller.call("CUSEXTMI", "AddFieldValue", params, callback)
  }
}
