
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

case class first_million_v2(Region_2: String,Country_1: String,Item_Type:String,Sales_Channel:String,Order_Priority:String,Order_Date:String,
                            Order_ID:String,Ship_Date:String,Units_Sold:String,Unit_Price:String,Unit_Cost:String,Total_Revenue:String,
                            Total_Cost:String,Total_Profit:String)
case class half_million_v2(Region_1: String,Country: String,Item_Type:String,Sales_Channel:String,Order_Priority:String,Order_Date:String,
                            Order_ID:String,Ship_Date:String,Units_Sold:String,Unit_Price:String,Unit_Cost:String,Total_Revenue:String,
                            Total_Cost:String,Total_Profit:String)
case class One_And_half_million_v2(Region_3: String,Country: String,Item_Type:String,Sales_Channel:String,Order_Priority:String,Order_Date:String,
                           Order_ID:String,Ship_Date:String,Units_Sold:String,Unit_Price:String,Unit_Cost:String,Total_Revenue:String,
                           Total_Cost:String,Total_Profit:String)
object csvExample {
  def main(args: Array[String])
  {

    //Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession
        .builder()
        .config(conf)
      .getOrCreate();
    import spark.implicits._

    val input_hafM_records = spark.read
      .option("header", "true").csv("file:///home/raviteja/500000_Sales_Records.csv")
      .select($"Region".as("Region_1"),$"Country",$"Item Type".as("Item_Type"),
        $"Sales Channel".as("Sales_Channel"),$"Order Priority".as("Order_Priority"),
        $"Order Date".as("Order_Date"),$"Order ID".as("Order_ID"),
        $"Ship Date".as("Ship_Date"),$"Units Sold".as("Units_Sold"),
        $"Unit Price".as("Unit_Price"),$"Unit Cost".as("Unit_Cost"),
        $"Total Revenue".as("Total_Revenue"),$"Total Cost".as("Total_Cost"),
        $"Total Profit".as("Total_Profit"))
         .as[half_million_v2]
    input_hafM_records.persist(StorageLevel.MEMORY_ONLY_SER)
    input_hafM_records.printSchema()
    println(input_hafM_records.count())

    val input_1M_records_v2 = spark.read
      .option("header", "true").csv("file:///home/raviteja/1000000_Sales_Records.csv")
      .select($"Region".as("Region_2"),$"Country".as("Country_1"),$"Item Type".as("Item_Type"),
        $"Sales Channel".as("Sales_Channel"),$"Order Priority".as("Order_Priority"),
        $"Order Date".as("Order_Date"),$"Order ID".as("Order_ID"),
        $"Ship Date".as("Ship_Date"),$"Units Sold".as("Units_Sold"),
        $"Unit Price".as("Unit_Price"),$"Unit Cost".as("Unit_Cost"),
        $"Total Revenue".as("Total_Revenue"),$"Total Cost".as("Total_Cost"),
        $"Total Profit".as("Total_Profit"))
      .as[first_million_v2]
    input_1M_records_v2.persist(StorageLevel.MEMORY_ONLY_SER)
    input_1M_records_v2.printSchema()
     println(input_1M_records_v2.count())

    val input_1_5M_records = spark.read
      .option("header", "true").csv("file:///home/raviteja/1500000_Sales_Records.csv")
      .select($"Region".as("Region_3"),$"Country",$"Item Type".as("Item_Type"),
        $"Sales Channel".as("Sales_Channel"),$"Order Priority".as("Order_Priority"),
        $"Order Date".as("Order_Date"),$"Order ID".as("Order_ID"),
        $"Ship Date".as("Ship_Date"),$"Units Sold".as("Units_Sold"),
        $"Unit Price".as("Unit_Price"),$"Unit Cost".as("Unit_Cost"),
        $"Total Revenue".as("Total_Revenue"),$"Total Cost".as("Total_Cost"),
        $"Total Profit".as("Total_Profit"))
          .as[One_And_half_million_v2]
    input_1_5M_records.persist(StorageLevel.MEMORY_ONLY_SER)
       input_1_5M_records.printSchema()
     println(input_1_5M_records.count())


    input_1_5M_records.unpersist()
    input_1M_records_v2.unpersist()
    input_hafM_records.unpersist()

  }
}
