package prueba.spark.save;

import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CsvToDataframeApp {

  public static void main(String[] args) {
    CsvToDataframeApp app = new CsvToDataframeApp();
    app.start();
  }

  private void start() {
	Statement stmt = null;
	Connection conn = null;
	String url = "jdbc:mysql://localhost:3306/bankdb";
	String table = "sample_data_table";
	    
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to Dataset")
        .master("local")
        .getOrCreate();
    //Creates schema
    StructType schema = new StructType()
    	      .add("age","int",true)
    	      .add("job","string",true)
    	      .add("marital","string",true)
    	      .add("education","string",true)
    	      .add("default","string",true)
    	      .add("balance","int",true)
    	      .add("housing","string",true)
    	      .add("loan","string",true)
    	      .add("contact","string",true)
    	      .add("day","int",true)
    	      .add("month","string",true)
    	      .add("duration","int",true)
    	      .add("campaign","int",true)
    	      .add("pdays","int",true)
    	      .add("previous","int",true)
    	      .add("poutcome","string",true)
    	      .add("deposit","string",true);
    //Reads from csv and put data in a data set
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .schema(schema)
        .load("bank.csv");
    

    //Save data from csv to MySql database
    try {
        conn =
           DriverManager.getConnection( url + "?user=root&password=12345");

        stmt = conn.createStatement();

        stmt.execute("DROP TABLE IF EXISTS " + table);
        
        Properties prop = new java.util.Properties();
        prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        prop.setProperty("user", "root");
        prop.setProperty("password", "12345"); 

        df.write().mode("append").jdbc(url, table, prop);
        
    } catch (SQLException ex) {
        // handle any errors
        System.out.println("SQLException: " + ex.getMessage());
        System.out.println("SQLState: " + ex.getSQLState());
        System.out.println("VendorError: " + ex.getErrorCode());
    }
     

    df.createOrReplaceTempView("bank");
    
    //answer test questions
    System.out.println("¿Cual es el rango de edad, qué contrata más prestamos?");
    Dataset<Row> sqlDF1 = spark.sql("SELECT age FROM (SELECT count(*) as count, age FROM bank WHERE loan = 'yes' GROUP BY age) WHERE count IN (SELECT MAX(count) FROM (SELECT count(*) as count,age FROM bank WHERE loan = 'yes' GROUP BY age))");
    sqlDF1.show();
    
    System.out.println("¿Cuál es el rango edad y estado civil que tiene más dinero en las cuentas?");
    Dataset<Row> sqlDF2 = spark.sql("SELECT age, marital, balance FROM bank WHERE balance IN (select MAX(balance) from bank)");
    sqlDF2.show();
    
    System.out.println("¿Cuál es la forma más común de contactar a los clientes, entre 25-35 años?");
    Dataset<Row> sqlDF3 = spark.sql("SELECT contact FROM (SELECT count(*) as count, contact FROM bank WHERE age > '25' AND age < '35' GROUP BY contact) WHERE count IN(SELECT MAX(count) FROM (SELECT count(*) as count, contact FROM bank WHERE age > '25' AND age < '35' GROUP BY contact))");
    sqlDF3.show();
    
    System.out.println("¿Cuál es el balance medio, máximo y minimo por cada tipo de campaña, teniendo encuenta su estado civil y profesión?");
    int maxCol = (int) df.count();
    df.groupBy("campaign", "marital", "job").max("balance").orderBy("campaign", "marital", "job").show(maxCol, true);
    df.groupBy("campaign", "marital", "job").min("balance").orderBy("campaign", "marital", "job").show(maxCol, true);
    df.groupBy("campaign", "marital", "job").avg("balance").orderBy("campaign", "marital", "job").show(maxCol, true);
    
    System.out.println("¿Cuál es el tipo de trabajo más común, entre los casados (job=married), que tienen casa propia (housing=yes), y que tienen en la cuenta más de 1.200€ y qué son de la campaña 3?");
    Dataset<Row> sqlDF4 = spark.sql("SELECT job FROM (SELECT count(*) as count, job FROM bank WHERE marital ='married' AND housing='yes' AND campaign='3' AND balance > '1200' GROUP BY job) WHERE count IN (SELECT MAX(count) from (SELECT count(*) as count FROM bank WHERE marital ='married' AND housing='yes' AND campaign='3' AND balance > '1200'  GROUP BY job))");
    sqlDF4.show();
    
  }
}
