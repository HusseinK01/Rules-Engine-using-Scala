import scala.io.Source
import java.time.{Duration, Instant, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.time.temporal.ChronoUnit


object DiscountsEngine extends App{


  /* -------------------------------- Functions -------------------------------- */
  /* --------------------------------------------------------------------------- */

  /* ------- Logger Functions ---------------------------------------------------*/

  def log_event(writer: PrintWriter, file: File, log_level: String, message: String): Unit = {
    writer.write(s"Timestamp: ${Instant.now()}\tLogLevel: ${log_level}\tMessage: ${message}\n")
    writer.flush()
  }

  def log_order(writer: PrintWriter, file: File, status: String): Unit = {
    log_event(writer, file, "Info/Debug", s"${status} processing an order")
  }

  def log_qualification(writer: PrintWriter, file: File, status: String): Unit = {
    log_event(writer, file, "Info", s"${status} qualification")
  }

  def log_discount(writer: PrintWriter, file: File, value: Double): Unit = {
    log_event(writer, file, "Info", s"calculated discount: ${value}")
  }

  def log_order_process_duration(writer: PrintWriter, file: File, value: Long): Unit = {
    log_event(writer, file, "Debug", s"Time taken to process order: ${value}ms")
  }

  /* --------------------------------------------------------------------------- */

  /* ------- Intermediate Order processing Function -----------------------------*/

  // helper function to convert a string representing a date to a LocalDate Object
  def strDateConverter(date_string: String, date_pattern: String)={

    val formatter = DateTimeFormatter.ofPattern(date_pattern)
    val dateTime = LocalDate.parse(date_string, formatter)
    dateTime
  }

  // helper function to split a the product_name string on "-" taking what's before the dash as product category
  //used in program
  def splitProductNameCatgeory(product_name: String): (String,String) = {
    val name_parts = product_name.split("-")
    if (name_parts.length > 1) (name_parts(0).trim, product_name)
    else ("Unknown", name_parts(0))
  }

  // helper function to check if the product_name string contains only some predefined words (categories),
  // setting the category to that word if so
  // not used in program
  def splitProductNameCatgeory2(product_name: String): (String, String) = {
    val product_category = {
      if (product_name.toLowerCase.contains("cheese")) "Cheese"
      else if (product_name.toLowerCase.contains("wine")) "Wine"
      else "Unknown"
    }
    (product_category, product_name)

  }

  def process_order(order: String) = {
    val order_parts = order.split(",")
    val order_date = strDateConverter(order_parts(0), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    val expiry_date = strDateConverter(order_parts(2), "yyyy-MM-dd")
    val days_to_expiry = ChronoUnit.DAYS.between(order_date, expiry_date).toInt
    val (product_category, product_name) = splitProductNameCatgeory(order_parts(1))
    val quantity = order_parts(3).toInt
    val unit_price = order_parts(4).toDouble
    val channel = order_parts(5)
    val payment_method = order_parts(6)
    (order_date,expiry_date, days_to_expiry, product_category, product_name, quantity,
      unit_price, channel, payment_method)
    /*
      1 order_date
      2 expiry_date
      3 days_to_expiry
      4 product_category
      5 product_name
      6 quantity
      7 unit_price
      8 channel
      9 payment_method
     */
  }

  /* --------------------------------------------------------------------------- */

  /* ------- Qualifying Functions -----------------------------------------------*/

  def qualify_23March(order: String): Boolean = {
    val date = process_order(order)._1
    if (date.getDayOfMonth == 23 && date.getMonthValue == 3) true
    else false
  }

  def qualify_expiryDays(order: String): Boolean = {
    val days_to_expiry = process_order(order)._3
    if (days_to_expiry < 30) true
    else false
  }

  def qualify_category(order: String): Boolean = {
    val product_category = process_order(order)._4
    product_category match {
      case "Wine" => true
      case "Cheese" => true
      case _ => false
    }
  }

  def qualify_visa(order: String): Boolean = {
    val channel = process_order(order)._9
    if (channel == "Visa") true
    else false
  }

  def qualify_quantity(order: String): Boolean = {
    val quantity = process_order(order)._6
    if (quantity > 5) true
    else false
  }

  def qualify_app(order: String): Boolean = {
    val channel = process_order(order)._8
    if (channel == "App") true
    else false
  }

  /* --------------------------------------------------------------------------- */

  /* ------- Discount Calculation Functions -------------------------------------*/

  def discount_23March(order: String): Double = {
    0.5
  }

  def discount_expiryDays(order: String): Double = {
    val days_to_expiry = process_order(order)._3
    (30.0 - days_to_expiry) / 100.0
  }

  def discount_category(order: String): Double = {
    val product_category = process_order(order)._4
    product_category match {
      case "Wine" => 0.05
      case "Cheese" => 0.1
      case _ => 0.00
    }
  }

  def discount_visa(order: String): Double = {
    0.05
  }

  def discount_quantity(order: String): Double = {
    val quantity = process_order(order)._6
    quantity match {
      case x if (x < 10) => 0.05
      case x if (x < 15) => 0.07
      case _ => 0.1
    }
  }

  def discount_app(order: String): Double = {
    val channel = process_order(order)._8
    val quantity = process_order(order)._6

    def helper(qunatity: Int): Double = {
      val remainder = qunatity % 5
      if (remainder == 0) {
        qunatity / 100.0 // No rounding needed if already a multiple of 5
      } else {
        (quantity + (5 - remainder)) / 100.0 // Round up to the next multiple of 5
      }
    }

    helper(quantity)
  }

  /* --------------------------------------------------------------------------- */

  /* ------- Qualification-Calculation Function Pairs List ----------------------*/

  val qualify_discount_map: List[((String) => Boolean, (String) => Double)] = List(
    (qualify_quantity, discount_quantity),
    (qualify_category, discount_category),
    (qualify_23March, discount_23March),
    (qualify_expiryDays, discount_expiryDays),
    (qualify_app, discount_app),
    (qualify_visa, discount_visa)
  )

  /* --------------------------------------------------------------------------- */

  /* ------- Qualification-Calculation Function (Main Engine) -------------------*/

  // initial version using .map()
  // not used in program.
  def rule_applier(order: String, rules: List[((String) => Boolean, (String) => Double)]): Double = {
    val discount = average(rules.filter(rule => rule._1(order)).
      map(rule => rule._2(order)).sortBy(x => x).reverse.take(2))
    if (discount > 0) discount else 0
  }

  // second version using a recursive function to allow for easier logging
  // used in program
  def rule_applier2(order: String, rules: List[((String) => Boolean, (String) => Double)], writer: PrintWriter, f: File): Double = {
    val start_timestamp = Instant.now()
    log_order(writer, f, "Started")

    def applyRules(rules: List[((String) => Boolean, (String) => Double)], acc: List[Double]): List[Double] = {
      rules match {
        case Nil => acc // Base case: no more rules to apply, return accumulated discounts
        case (condition, discount) :: tail =>
          if (condition(order)) {
            log_qualification(writer, f, "Successful")
            val appliedDiscount = discount(order)
            log_discount(writer, f, appliedDiscount)
            applyRules(tail, appliedDiscount :: acc) // Apply the current rule and accumulate the discount
          } else {
            log_qualification(writer, f, "Failed")
            applyRules(tail, acc) // Skip the current rule and continue with the next one
          }
      }
    }

    println("---------started processing a new order-------")
    val discounts = applyRules(rules, Nil) // Apply rules recursively and accumulate discounts
    println("all discounts calculated: " + discounts)
    val topTwoDiscounts = discounts.sorted.reverse.take(2) // Select the top two discounts
    println("top two discounts calculated: " + topTwoDiscounts)
    val avgDiscount = if (topTwoDiscounts.nonEmpty) average(topTwoDiscounts) else 0 // Calculate average discount
    println("avg of top two discounts: " + avgDiscount)
    log_order(writer, f, "Ended")
    val end_timestamp = Instant.now()
    val order_processing_duration = Duration.between(start_timestamp, end_timestamp).toMillis
    log_order_process_duration(writer, f, order_processing_duration)

    if (avgDiscount > 0) avgDiscount else 0 // Return the average discount or 0 if no discount applied

  }

  // helper functions used to calculate avg to 3 decimal places
  def average(nums: List[Double]): Double = {
    BigDecimal(nums.sum / nums.length).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /* --------------------------------------------------------------------------- */

  /* ------- Final Order Processing Function ------------------------------------*/

  def prepare_for_writing(order: String, rules: List[((String) => Boolean, (String) => Double)], writer: PrintWriter, f: File) = {
    val initial_processing = process_order(order)
    val discount = rule_applier2(order, rules, writer, f)
    val total_due = (initial_processing._7 * initial_processing._6) -
      (discount * initial_processing._7 * initial_processing._6)
    (
      initial_processing._1,
      initial_processing._2,
      initial_processing._3,
      initial_processing._4,
      initial_processing._5,
      initial_processing._6,
      initial_processing._7,
      initial_processing._8,
      initial_processing._9,
      discount,
      total_due
    )
  }

  /* --------------------------------------------------------------------------- */

  /* ------- Database Writer Function -------------------------------------------*/

  def write_to_db(orders: List[String], rules: List[((String) => Boolean, (String) => Double)], writer: PrintWriter, f: File): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val username = "scala_project"
    val password = "123"

    val data = orders.map(
      order =>
        prepare_for_writing(order, rules, writer, f)
    )

    val insertStatement =
      """
        |INSERT INTO orders (order_date, expiry_date, days_to_expiry, product_category,
        |                   product_name, quantity, unit_price, channel, payment_method,
        |                   discount, total_due)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver") // Load the Oracle JDBC driver
      connection = DriverManager.getConnection(url, username, password)
      log_event(writer, f, "Debug", "Successfully Opened database connection")
      // Prepare the INSERT statement
      preparedStatement = connection.prepareStatement(insertStatement)

      // Insert data into the table
      data.foreach { case (orderDate, expiryDate, daysToExpiry, productCategory, productName, quantity,
      unitPrice, channel, paymentMethod, discount, totalDue) =>
        preparedStatement.setDate(1, Date.valueOf(orderDate.toString))
        preparedStatement.setDate(2, Date.valueOf(expiryDate.toString))
        preparedStatement.setInt(3, daysToExpiry)
        preparedStatement.setString(4, productCategory)
        preparedStatement.setString(5, productName)
        preparedStatement.setInt(6, quantity)
        preparedStatement.setDouble(7, unitPrice)
        preparedStatement.setString(8, channel)
        preparedStatement.setString(9, paymentMethod)
        preparedStatement.setDouble(10, discount)
        preparedStatement.setDouble(11, totalDue)

        preparedStatement.addBatch() // Add the current INSERT statement to the batch
      }

      // Execute the batch of INSERT statements
      preparedStatement.executeBatch()
    } catch {
      case e: Exception =>
        log_event(writer, f, "Error", s"Failed to close preparedStatement: ${e.getMessage}")
    } finally {
      // Close resources
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
      log_event(writer, f, "Info", "Successfully inserted into database")
      log_event(writer, f, "Debug", "Closed database connection")
    }
  }

  /* --------------------------------------------------------------------------- */


  /* ------------------------------- Main Program ------------------------------- */
  /* --------------------------------------------------------------------------- */

  // creating/opening the logs file
  val f: File = new File("src/main/resources/logs.log")

  // creating the writer
  val writer = new PrintWriter(new FileOutputStream(f,true))

  // recording program start timestamp
  val program_start_time = Instant.now()

  // logging the start of the program
  log_event(writer, f, "Info/Debug", "Program Started")

  // loading the csv file to process
  val orders = Source.fromFile("src/main/resources/TRX1000.csv").getLines().toList.tail

  // calling the Database Writer Function (it calls all other functions)
  write_to_db(orders, qualify_discount_map, writer, f)

  // calculating the duration it took to run the program
  val program_work_duration = Duration.between(program_start_time, Instant.now()).toMillis

  // logging that the program has finished running
  log_event(writer, f, "Info/Debug", "Program Finished")

  // logging the duration it took to run the program in milliseconds
  log_event(writer, f, "Debug", s"Program took ${program_work_duration}ms to run")

}
