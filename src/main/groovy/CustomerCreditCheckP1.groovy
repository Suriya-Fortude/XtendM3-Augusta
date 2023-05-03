class testingExceptionAPI extends ExtendM3Transaction {
  private final MIAPI mi
    private final ExceptionAPI exception

    testingExceptionAPI(MIAPI mi, ExceptionAPI exception) {
    this.mi = mi
        this.exception = exception
    }

    void main() {
    int salesPrice = mi.inData.get("INSAPR")
        int basePrice = mi.inData.get("INAIPR")
        validateData(salesPrice, basePrice)
        int profitMargin = salesPrice - basePrice
        mi.inData.set("OUTPM")
        if(!mi.write()) {
      String message = "Failed to write MI out parameter"
            exception.throwErrorMIResponseException(message)
        }
    ExpressionFactory expression = database.getExpressionFactory("MPDOMA")
        expression = expression.eq("PNFTID", QAMCC2).and(expression.lt("PNOPTN", QBMVC2))
        expression.like()
  }

  private void validateData(int param1, int param2) {
    if(param1 == 0 || param2 == 0) {
      String message = "One or both of the given parameters have value zero."
        exception.throwErrorMIResponseException(message)
    }
  }
}
