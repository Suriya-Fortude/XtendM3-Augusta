import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class EXT002 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final ProgramAPI program

  EXT001(LoggerAPI logger, MICallerAPI miCaller, ProgramAPI program, DatabaseAPI database) {
    this.logger = logger
    this.miCaller = miCaller
    this.database = database
    this.program = program
  }

  //Global Variables
  int CONO, maxPageSize = 1000000,ATPDate180 = 0,ATPDate185=0,ATPQty180=0,ATPQty185= 0
  List<String> phCrownProductGroup = new ArrayList<String>()
  List<String> phCrownWarehouse = new ArrayList<String>()
  List<String> validAccessControlObjects = new ArrayList<String>()
  HashMap<String, String> invalidProductGroupACO = new HashMap<String, String>()
  HashMap<String, String> warehouseForACO = new HashMap<String, String>()
  HashMap<String, Integer> exportItemList = new HashMap<String, Integer>()
  String workingDaySerialNumber="",lowerStatus="",higherStatus = "", atpWarehouse = "",DIVI = "",atpDate="",threshHoldQuantity = ""
  boolean allTranslationsAvailable = true,availableToPromiseRecordsFound = false

  void main() {
    CONO =  program.LDAZD.CONO
    DIVI =  program.LDAZD.DIVI
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
  def getTranslationValues() {

    //Get PH Crown Product group list.
    def params = ["CONO": CONO.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "PHCN"]
    def callback = {
      Map<String,
        String> response ->
        if (response.MBMD != null) {
          phCrownProductGroup = new ArrayList<>(Arrays.asList(response.MBMD.split(",")))
          for (int i = 0; i < phCrownProductGroup.size(); i++) {
            //Get Invalid Access control object for each product group.
            getInvalidACOforProductGroup(phCrownProductGroup.get(i))
          }
        } else {
          allTranslationsAvailable = false
          logger.error("PH Crown Product Groups not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    //Get valid warehouses for items with PH Crown product group.
    params = ["CONO": CONO.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "PHCN_WHLO"]
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
    params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"ACO"]
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
        params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"ACO_WHLO_"+ACO.trim()]
        callback = {
          Map < String,
            String > response ->
            if (response.MBMD != null) {
              String list = response.MBMD
              warehouseForACO.put(ACO.trim(),list.trim())
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
    params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"STAL"]
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

    //Get lower status value applicable for records in MITMAS and MITBAL
    params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"STAH"]
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
    params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"THRESH HOLD"]
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
  def deleteCustomTableData() {
    logger.info("Delet Custom table data function triggered.")
    //List all records from EXTPQT Table
    def params = ["CONO": CONO.toString().trim()]
    def callback = {
      Map<String,
        String> response ->
        DBAction query = database.table("EXTPDT").index("00").build()
        DBContainer container = query.getContainer()
        container.set("EXCONO", CONO)
        container.set("EXWHLO", response.WHLO)
        container.set("EXITNO", response.ITNO)
        if (query.read(container)) {
          query.readLock(container, deleteAllATPDateRecords)
        }
    }
    miCaller.call("EXT200MI", "LstATPDate", params, callback)

    //List all records from EXTPDT Table
    params = ["CONO": CONO.toString().trim()]
    callback = {
      Map<String,
        String> response ->
        DBAction query = database.table("EXTPQT").index("00").build()
        DBContainer container = query.getContainer()
        container.set("EXCONO", CONO)
        container.set("EXWHLO", response.WHLO)
        container.set("EXITNO", response.ITNO)
        if (query.read(container)) {
          query.readLock(container, deleteAllATPQuantityRecords)
        }
    }
    miCaller.call("EXT201MI", "LstATPQuantity", params, callback)
  }


  /**
   * Get invalid access control objects for the product group.
   * @params ITCL
   * @return
   */
  def getInvalidACOforProductGroup(String ITCL)
  {
    if(ITCL==null||ITCL.trim().isEmpty()) ITCL = ""
    def params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"INVACO_"+ITCL.trim()]
    def callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          invalidProductGroupACO.put(ITCL,response.MBMD)
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)
  }

  /**
   * Get all matching items
   * @params
   * @return
   */
  def getItems()
  {
    ExpressionFactory expression = database.getExpressionFactory("MITMAS")
    expression = expression.gt("MMSTAT",lowerStatus).and(expression.lt("MMSTAT",higherStatus))
    DBAction query = database.table("MITMAS").index("00").matching(expression).selection("MMITCL","MMITNO","MMACRF").build()
    DBContainer container = query.getContainer()
    container.set("MMCONO", CONO)
    query.readAll(container, 1,maxPageSize, getItems)
    logger.info("Completed reading records: "+invalidProductGroupACO)
  }

  Closure < ? > getItems = {
    DBContainer containerResult ->
      String productGroup = containerResult.getString("MMITCL")
      String accessControlObject = containerResult.getString("MMACRF")
      String itemNumber = containerResult.getString("MMITNO")
      String warehouse = warehouseForACO.get(accessControlObject)
      boolean invalidItem = false
      boolean PHCrownProdcutGroup = false
      int planningTimeFence = 0

      productGroup =  (productGroup == null||productGroup.trim().isEmpty())?"":productGroup.trim()
      accessControlObject =  (accessControlObject == null||accessControlObject.trim().isEmpty())?"":accessControlObject.trim()
      itemNumber =  (itemNumber == null||itemNumber.trim().isEmpty())?"":itemNumber.trim()
      warehouse =  (warehouse == null||warehouse.trim().isEmpty())?"":warehouse.trim()

      if(phCrownProductGroup!=null && phCrownProductGroup.size()>0&&phCrownProductGroup.contains(productGroup))
      {
        PHCrownProdcutGroup = true
        for(int i=0;i<phCrownWarehouse.size();i++)
        {
          int phQuantity = getWarehouseQuantity(phCrownWarehouse[i],itemNumber)
          if(phQuantity>0) {
            filterItemsAndUploadCrownItems(phCrownWarehouse[i], itemNumber)
          }
        }
      }

      if (invalidProductGroupACO != null && invalidProductGroupACO.size() > 0) {
        //If the Access Control Object of the item is a part of invalid access control object list for the product group, then set as invalid item.
        String invalidACOList = invalidProductGroupACO.get(productGroup)
        invalidItem = invalidACOList.contains(accessControlObject)
      }
      //If item number contains "." set invalid item
      if (itemNumber.contains(".")) {
        invalidItem = true
      }

      //If translation defined, override valid warehouse for specific items.
      if (!invalidItem) {
        def params = ["CONO": CONO.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "OVRD_WHLO_" + itemNumber]
        def callback = {
          Map<String,
            String> response ->
            if (response.MBMD != null && warehouse.trim().isEmpty()) {
              warehouse = response.MBMD
            }
        }
        miCaller.call("CRS881MI", "GetTranslData", params, callback)
      }

      String[] warehouses = warehouse.split(",")

      for (int i = 0; i < warehouses.length; i++) {
        //If the item does not exists for the access control object warehouse or override warehouse in MMS002, then set invalid item.
        def params = ["CONO": CONO.toString().trim(), "WHLO": warehouses[i].toString().trim(), "ITNO": itemNumber.toString().trim()]
        def callback = {
          Map<String,
            String> response ->
            if (response.ITNO == null) {
              invalidItem = true
            }else
            {
              planningTimeFence = Integer.parseInt(response.PFTM)
              i=warehouses.length //Break for loop if the item exists in at least one warehouse.
            }
        }
        miCaller.call("MMS200MI", "GetItmWhsBasic", params, callback)
      }

      if(!invalidItem&&!PHCrownProdcutGroup)
      {
        for (int i = 0; i < warehouses.length; i++) //Iterate through all available warehouses.
        {
          warehouse = warehouses[i]
          availableToPromiseRecordsFound = false
          filterItemsAndUploadQuantity(warehouse,itemNumber)
          filterItemsAndUploadDate(warehouse,itemNumber,planningTimeFence)
        }
      }
      if(ATPQty180>0)
      {
        int ATPQuantity = ATPQty180 + ATPQty185
        String ATPDate = ATPDate180
        if(Integer.parseInt(ATPDate185)<Integer.parseInt(ATPDate180))
        {
          ATPDate = ATPDate185
        }
        addItemsToATPDateTable(itemNumber, warehouse,ATPQuantity, ATPDate)

      }
  }

  /**
   * Get item quantity in the warehouse.
   * @params ItemNumber and warehouse
   * @return
   */
  private Integer getWarehouseQuantity(String warehouse,String itemNumber){
    ExpressionFactory expression = database.getExpressionFactory("MITBAL")
    expression = expression.gt("MBSTAT",lowerStatus).and(expression.lt("MBSTAT",higherStatus))
    DBAction query = database.table("MITBAL").index("00").matching(expression).selection("MBSTQT","MBREQT").build()
    DBContainer container = query.getContainer()
    container.set("MBCONO", CONO)
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
   * Filter PH Crown items items and add data to quantity table
   * @params ItemNumber and warehouse
   * @return
   */
  private filterItemsAndUploadCrownItems(String warehouse,String itemNumber){
    int quantity = getWarehouseQuantity(warehouse,itemNumber)

    def params = ["CONO": CONO.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.substring(3,15).toString().trim()]
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
    container.set("MACONO", CONO)
    container.set("MAWHLO", warehouse)
    container.set("MAITNO", itemNumber)
    container.set("MAPLDT", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())

    if (query.read(container)) {
      quantity = container.getInt("MAAVTP")
      if (quantity == 0 && (warehouse.trim().equals("180") || warehouse.trim().equals("185"))) //If quantity is 0 and warehouse is 180 or 185, check for other dates as well.
      {
        query.readAll(container, 3,maxPageSize, readAvailableToPromiseQuantity)
      }

      if (quantity > 0) {
        def params = ["CONO": CONO.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim()]
        def callback = {
          Map < String,
            String > response ->
            if(response.VAL9!=null)
            {
              int qty = Integer.parseInt(quantity)
              int currentQuantity = Integer.parseInt(response.VAL9)
              int thqty = Integer.parseInt(threshHoldQuantity)
              if(qty!=currentQuantity&&((qty<thqty)||(currentQuantity<thqty)))
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
    }

  }
  /**
   * Filter items and add data to date table
   * @params Warehouse, itemnumber and quantity
   * @return
   */
  private filterItemsAndUploadDate(String warehouse,String itemNumber,int planningTimeFence){
    DBAction query = database.table("MITATP").index("00").selection("MAAVTP","MAPLDT","MAAVTP","MAWHLO","MAITNO").build()
    DBContainer container = query.getContainer()
    container.set("MACONO", CONO)
    container.set("MAWHLO", warehouse)
    container.set("MAITNO", itemNumber)

    workingDaySerialNumber = ""
    def params = ["CONO": CONO.toString().trim(), "YMD8": LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))]
    def callback = {
      Map<String,
        String> response ->
        workingDaySerialNumber = response.WDNO
        String deliveryDay = response.DDAY
        if(workingDaySerialNumber==null||workingDaySerialNumber.trim().isEmpty()) workingDaySerialNumber = "0"
        if(deliveryDay==null||deliveryDay.trim().isEmpty()) deliveryDay = "0"
        if (!deliveryDay.trim().equals("1")) {
          query = database.table("CSYCAL").index("00").selection("CDDDAY", "CDWDNO").build()
          container = query.getContainer()
          container.set("CDCONO", CONO)
          container.set("CDDIVI", DIVI)
          query.readAll(container, 2, maxPageSize, getSystemCalendarDate)
        }
    }
    miCaller.call("CRS900MI", "GetSysCalDate", params, callback)
    int totalWorkingDay = planningTimeFence + Integer.parseInt(workingDaySerialNumber)
    String planningDate = ""
    params = ["CONO": CONO.toString().trim(),"QERY":"CDYMD8 from CSYCAL where CDWDNO = '"+totalWorkingDay+"'  and CDDDAY = '1'"]
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

    //Add all valid items to EXTPDT table except for warehouse 180
    if(!warehouse.trim().equals("180")) {
      query.readAll(container, 3, maxPageSize, readATPDateRecords)
      addItemsToATPDateTable(itemNumber, warehouse, planningDate)
    }else
    {
      if(!availableToPromiseRecordsFound) {
        query.readAll(container, 3, maxPageSize, readATPDateRecords_warehouse_180_185)
      }
      if(!availableToPromiseRecordsFound)
      {
        atpDate = planningDate
        int quantity = getWarehouseQuantity(warehouse,itemNumber)
        if(warehouse.trim().equals("180"))
        {
          ATPQty180 = quantity.toString()
          ATPDate180 = atpDate
        }else
        {
          ATPQty185 = quantity.toString()
          ATPDate185 = atpDate
        }
      }
      if(warehouse.trim().equals("180"))
      {
        availableToPromiseRecordsFound = false
      }
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
    query.set("EXCONO", CONO)
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
    def params = ["CONO": CONO.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim(),"VAL9":quantity.toString().trim(),"UPDS":"1"]
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
    def params = ["CONO": CONO.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim(),"VAL9":quantity.toString().trim(),"UPDS":"1"]
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
      String itemNumber = containerResult.getString("MAITNO")
      String warehouse = containerResult.getString("MAWHLO")
      int planningDate = containerResult.getInt("MAPLDT")

      if(quantity>0 && planningDate>0)
      {
        if(warehouse.trim().equals("180")) {
          ATPQty180 = containerResult.getInt("MAAVTP")
          ATPDate180 = containerResult.getInt("MAPLDT")
          availableToPromiseRecordsFound = true
        }else
        {
          ATPQty185 = containerResult.getInt("MAAVTP")
          ATPDate185 = containerResult.getInt("MAPLDT")
          availableToPromiseRecordsFound = true
        }
      }
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
      if(deliveryDate.trim().equals("1"))
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
    def params = ["CONO": CONO.toString().trim(),"FILE":"ECOM_ATP","PK01":"UPDS"]
    def callback = {
      Map < String,
        String > response ->
    }
    miCaller.call("CUSEXTMI", "DelFieldValue", params, callback)

    params = ["CONO": CONO.toString().trim(),"FILE":"ECOM_ATP","PK01":"UPDS"]
    callback = {
      Map < String,
        String > response ->
    }
    miCaller.call("CUSEXTMI", "AddFieldValue", params, callback)
  }
}
