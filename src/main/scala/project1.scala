import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext





import java.util.Scanner
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

object project1  {
  
     def main(args: Array[String]): Unit = {
          

          
       System.setSecurityManager(null)
       //System.setProperty() // change if winutils.exe is in a different bin folder
         val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1")    // Change to whatever app name you want
         val sc = new SparkContext(conf)
            sc.setLogLevel("ERROR")
         val hiveCtx = new HiveContext(sc)
            import hiveCtx.implicits._

          
      sc.stop()
          
          val driver = "com.mysql.cj.jdbc.Driver"
          val url = "jdbc:mysql://localhost:3306/project1"
          val username = "sqluser"
          val password = "password" 
          var connection:Connection = null
          connection = DriverManager.getConnection(url, username, password)
          val statement = connection.createStatement()
      
      
      
          var scan = new Scanner(System.in)

          var w = new adminOrUser();
          var x = w.choice(scan)

     
     
      
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
     }
  
  

  
         
    
     
    
       
    

     
}