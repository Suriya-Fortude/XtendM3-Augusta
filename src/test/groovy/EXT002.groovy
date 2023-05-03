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
  int CONO
  List<String> phCrownProductGroup = new ArrayList<String>()
  List<String> phCrownWarehouse = new ArrayList<String>()
  List<String> validAccessControlObjects = new ArrayList<String>()
  HashMap<String, String> invalidProductGroupACO = new HashMap<String, String>()
  HashMap<String, String> warehouseForACO = new HashMap<String, String>()
  HashMap<String, Integer> exportItemList = new HashMap<String, Integer>()
  String workingDaySerialNumber="",lowerStatus="",higherStatus="",threshHoldQuantity = ""


  void main() {
    CONO =  program.LDAZD.CONO
    getThreshHoldQuantity()
    getPHCrownProductTypesAndWarehouse()
    getValidAccessControlObjects()
    getValidWarehouseForAccessControlObjects()
    getItemWarehouseLowerAndHigherStatus()
    logger.info("Suriya phCrownProductGroup: "+phCrownProductGroup)
    logger.info("Suriya validAccessControlObjects: "+validAccessControlObjects)
    logger.info("Suriya warehouseForACO: "+warehouseForACO)
    if (!phCrownProductGroup.isEmpty() && !validAccessControlObjects.isEmpty() && !warehouseForACO.isEmpty()&&!lowerStatus.trim().isEmpty()&&!higherStatus.trim().isEmpty()&&!threshHoldQuantity.trim().isEmpty()) {
      getItems()
      setUpdateLoadFlag()
    }
  }

  /**
   * Get threshhold quantity from translation
   * @params
   * @return
   */
  def getThreshHoldQuantity() {
    def params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"THRESH HOLD"]
    def callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          threshHoldQuantity = response.MBMD
        }else
        {
          logger.error("ThreshHold Quantity not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)
  }
  /**
   * Get Item Warehouse lower and higher Status
   * @params
   * @return
   */
  def getItemWarehouseLowerAndHigherStatus() {
    def params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"STAL"]
    def callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          lowerStatus = response.MBMD
        }else
        {
          logger.error("Valid lower Status not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"STAH"]
    callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          higherStatus = response.MBMD
        }else
        {
          logger.error("Valid higher Status not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)
  }
  /**
   * Get valid PH Crown Product types from translation table.
   * @params
   * @return
   */
  def getPHCrownProductTypesAndWarehouse() {
    def params = ["CONO": CONO.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "PHCN"]
    def callback = {
      Map<String,
        String> response ->
        if (response.MBMD != null) {
          phCrownProductGroup = new ArrayList<>(Arrays.asList(response.MBMD.split(",")))
          for (int i = 0; i < phCrownProductGroup.size(); i++) {
            getInvalidACOforProductGroup(phCrownProductGroup.get(i))
          }
        } else {
          logger.error("PH Crown Product Groups not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)

    params = ["CONO": CONO.toString().trim(), "DIVI": "", "TRQF": "0", "MSTD": "ECOM", "MVRS": "1", "BMSG": "ECOM ATP", "IBOB": "O", "ELMP": "ECOM ATP", "ELMD": "ECOM ATP", "MVXD": "PHCN_WHLO"]
    callback = {
      Map<String,
        String> response ->
        if (response.MBMD != null) {
          phCrownWarehouse  = new ArrayList<>(Arrays.asList(response.MBMD.split(",")))
        } else {
          logger.error("PH Crown Product Groups not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)
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
   * Get valid access control objects from translation table.
   * @params
   * @return
   */
  def getValidAccessControlObjects()
  {
    def params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"ACO"]
    def callback = {
      Map < String,
        String > response ->
        if (response.MBMD != null) {
          validAccessControlObjects =  new ArrayList<>(Arrays.asList(response.MBMD.split(",")))

        }else
        {
          logger.error("Valid Access Control Objects not defined in CRS881. Please configure the same.")
          return
        }
    }
    miCaller.call("CRS881MI", "GetTranslData", params, callback)
  }

  /**
   * Get valid warehouse for the access control object
   * @params
   * @return
   */
  def getValidWarehouseForAccessControlObjects()
  {
    logger.info("Suriya validAccessControlObjects:  "+validAccessControlObjects+" ")
    logger.info("Suriya validAccessControlObjects:  "+validAccessControlObjects[0])
    if(validAccessControlObjects!=null&&validAccessControlObjects.size()>0)
    {
      for(int i=0;i<validAccessControlObjects.size();i++)
      {
        String ACO = validAccessControlObjects[i]
        def params = ["CONO": CONO.toString().trim(),"DIVI":"","TRQF":"0","MSTD":"ECOM","MVRS":"1","BMSG":"ECOM ATP","IBOB":"O","ELMP":"ECOM ATP","ELMD":"ECOM ATP","MVXD":"ACO_WHLO_"+ACO.trim()]
        def callback = {
          Map < String,
            String > response ->
            if (response.MBMD != null) {
              String list = response.MBMD
              warehouseForACO.put(ACO.trim(),list.trim())
            }else{
              logger.error("Valid warehouse not defined for ACO: "+ACO+". Please configure the same in CRS881")
              return
            }
        }

        miCaller.call("CRS881MI", "GetTranslData", params, callback)
      }
    }
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
    int pageSize = 10000000
    query.readAll(container, 1,pageSize, getItems)
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

      if (productGroup == null) productGroup = ""
      if (accessControlObject == null) accessControlObject = ""
      if (itemNumber == null) itemNumber = ""
      if (warehouse == null) warehouse = ""

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
        //If the item does not exists for the access control object warehouse or override warehouse, then set invalid item.
        def params = ["CONO": CONO.toString().trim(), "WHLO": warehouses[i].toString().trim(), "ITNO": itemNumber.toString().trim()]
        def callback = {
          Map<String,
            String> response ->
            if (response.ITNO == null) {
              invalidItem = true
            }else
            {
              i=warehouses.length //Break for loop if the item exists in at least one warehouse.
            }
        }
        miCaller.call("MMS200MI", "GetItmWhsBasic", params, callback)
      }

      if(phCrownProductGroup!=null && phCrownProductGroup.size()>0&&phCrownProductGroup.contains(productGroup))
      {
        PHCrownProdcutGroup = true
        invalidItem = false
      }

      if(!invalidItem)
      {
        if(PHCrownProdcutGroup)
        {
          for(int i=0;i<phCrownWarehouse.size();i++)
          {
            String phCrownWarehouse = phCrownWarehouse[i]
          }
        }else {
          for (int i = 0; i < warehouses.length; i++) {
            warehouse = warehouses[i]
            String quantity = "0"
            DBAction query = database.table("MITATP").index("00").selection("ATPQTY").build()
            DBContainer container = query.getContainer()
            container.set("ATCONO", CONO)
            container.set("ATWHLO", warehouse)
            container.set("ATITNO", itemNumber)
            container.set("ATPLDT", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")).toInteger())
            if (query.read(container)) {
              quantity = container.getString("ATPQTY")
              if (quantity == null || quantity.trim().isEmpty()) quantity == "0"
              if (Integer.parseInt(quantity) == 0 && (warehouse.trim().equals("180") || warehouse.trim().equals("185"))) {
                query = database.table("MITATP").index("00").selection("ATPQTY", "ATITNO", "ATWHLO").build()
                container = query.getContainer()
                container.set("ATCONO", CONO)
                container.set("ATWHLO", warehouse)
                container.set("ATITNO", itemNumber)
                query.readAll(container, 3, readAvailableToPromiseQuantity)
              }
              query.readAll(container,3,10000000,readATPDateRecords)

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
                      updateItemsToATPQuantityTable(itemNumber,warehouse,qty.toString())
                    }
                  }
                  else if(response.errorMessage != null)
                  {
                    logger.error("Error adding Item Quantity for item: "+itemNumber+". Failed with error message: "+response.errorMessage)
                  }
              }
              miCaller.call("EXT201MI", "GetATPQuantity", params, callback)

              if (Integer.parseInt(quantity) > 0) {
                addItemsToATPQuantityTable(itemNumber, warehouse, quantity)
              }
            }

          }
        }
      }
  }

  Closure < ? > readAvailableToPromiseQuantity = {
    DBContainer containerResult ->
      String quantity = containerResult.getString("ATPQTY")
      String itemNumber = containerResult.getString("ATITNO")
      String warehouse = containerResult.getString("ATWHLO")
      if(quantity==null||quantity.trim().isEmpty()) quantity=="0"

      if(Integer.parseInt(quantity)>0)
      {
        addItemsToATPQuantityTable(itemNumber,warehouse,quantity)
        return
      }
  }

  /**
   * Add records to EXTPQT table for ECOM.
   * @params ItemNumber, warehouse and quantity
   * @return
   */
  private addItemsToATPQuantityTable(String itemNumber,String warehouse, String quantity)
  {
    def params = ["CONO": CONO.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim(),"VAL9":quantity,"UPDS":"1"]
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
  private updateItemsToATPQuantityTable(String itemNumber,String warehouse, String quantity)
  {
    def params = ["CONO": CONO.toString().trim(),"WHLO":warehouse.toString().trim(),"ITNO":itemNumber.toString().trim(),"VAL9":quantity,"UPDS":"1"]
    def callback = {
      Map < String,
        String > response ->
        if(response.errorMessage != null)
        {
          logger.error("Error updating Item Quantity for item: "+itemNumber+". Failed with error message: "+response.errorMessage)
        }
    }
    miCaller.call("EXT201MI", "UpdATPQuantity", params, callback)
  }
  Closure < ? > readATPDateRecords = {
    DBContainer containerResult ->
      String quantity = containerResult.getString("ATPQTY")
      String itemNumber = containerResult.getString("ATITNO")
      String warehouse = containerResult.getString("ATWHLO")
      String planningDate = containerResult.getString("ATPLDT")

      if(planningDate==null||planningDate.trim().isEmpty()) planningDate=="0"
      if(quantity==null||quantity.trim().isEmpty()) quantity=="0"

      if(Integer.parseInt(quantity)>0 && Integer.parseInt(planningDate)>0)
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

      //Get Working Day Serial Number
      workingDaySerialNumber = ""
      def params = ["CONO": CONO.toString().trim(),"YMD8":LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))]
      def callback = {
        Map < String,
          String > response ->
          if(response.WDNO != null)
          {
            workingDaySerialNumber = response.WDNO
          }else
          {
            DBAction query = database.table("CSYCAL").index("00").selection("CDDDAY","CDWDNO").build()
            DBContainer container = query.getContainer()
            container.set("CDCONO", CONO)
            container.set("CDDIVI", "")
            query.readAll(container,2,10000,getSystemCalendarDate)
          }
      }
      miCaller.call("CRS900MI", "GetSysCalDate", params, callback)
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
    miCaller.call("CUSEXTMI", "addFieldValue", params, callback)
  }

  Closure < ? > getSystemCalendarDate = {
    DBContainer containerResult ->
      String deliveryDate = containerResult.getString("CDDDAY")
      if(deliveryDate==null||deliveryDate.trim().isEmpty()) deliveryDate = ""
      if(deliveryDate.trim().equals("1"))
      {
        workingDaySerialNumber = containerResult.getString("CDWDNO")
      }
  }
}

