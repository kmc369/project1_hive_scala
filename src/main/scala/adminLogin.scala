import java.util.Scanner
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext



class adminLogin {

  
          val driver = "com.mysql.cj.jdbc.Driver"
          val url = "jdbc:mysql://localhost:3306/project1"
          val username = "sqluser"
          val password = "####" 
          var connection:Connection = null
          connection = DriverManager.getConnection(url, username, password)
          val statement = connection.createStatement() 
  
            System.setSecurityManager(null)
       //System.setProperty() // change if winutils.exe is in a different bin folder
         val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1")    // Change to whatever app name you want
         val sc = new SparkContext(conf)
            sc.setLogLevel("ERROR")
         val hiveCtx = new HiveContext(sc)
            import hiveCtx.implicits._
  
  
  
  def login(scan:Scanner): Unit ={
        
        println("                                                                                             ")
        println("                                                                                             ")
        println("                                           CLASSIFIED                                        ")
        println("**************************************UFO: REAL OR REAL?*******************************************")
        println("                                                                                             ")
        println("                                                                                             ")
        println("                                                                                             ")
        
          var a = true;
     do{   
        
         try{
         println("")
         println("ENTER USERNAME: ");
         println("")
         println("")
         var attemptedName = scan.nextLine();
        println("")
        println(" ")
         println("ENTER PASSWORD: ");
     
         println("")
         var attemptedPassword = scan.nextLine().toLowerCase()
         
         println("")
          Class.forName(driver);

          val statement = connection.createStatement();
          
        
          val table = "admin_login";
          var query = ("SELECT user_password FROM  admin_login where user_name = '" + attemptedName  +"'; ")
          var res = statement.executeQuery(query);
          var x = ""
       
       
              while (res.next()) {
            
               x=res.getString(1)
                
              }

              if (attemptedPassword==x){
                
                databaseInteraction(scan)
                
              }
            
             else{
                throw new BadUserException
               
             }
            
        } catch {
            case e: BadUserException => println("PASSWORD INCORRECT PLEASE TRY AGAIN")
            a=false;
          }
        
         
        
       } while(a!=true)
  
  }





  

    
 
 
 
 
 
 
  

  def databaseInteraction(scan:Scanner): Unit = {
      
      var b = true;
    
  
  
     
   
 do{
        println()
        println("WHAT WOULD YOU LIKE TO DO?")
        println()
        println("1)DELETE USER FROM DATABASE")
        println()
      
        println("2)VIEW THE DATABASE")
        println()
     
        println("3)EXIT THE DATABASE")
        println()
        println()
        
        var choice = scan.nextLine.toUpperCase()
 
     
      if (choice =="DELETE"){
          println(" ")
          println ("ENTER THE USERNAME OF THE PERSON YOU WANT TO DELETE")
          val userName = scan.nextLine()
          println(" ")

          val query = ("DELETE FROM user_login WHERE user_name = '"+userName +"';")
          val rs =statement.executeUpdate(query)
          println(" ")
          println("******DELETE SUCCESSFUL*****")
          
         
      
      }
      
      if(choice=="VIEW"){
           hiveCtx.sql("USE project1_hive_scala;")
           val summary = hiveCtx.sql("SELECT * FROM ufo_data_part LIMIT 10;") //this is my partitioned table
           summary.show()
      }

      if (choice == "EXIT") {
   
        exitprogram(scan)
        b= false;
      
      } 

    }while(b)
    exitprogram(scan)
 }  
 
 

    def exitprogram(scan:Scanner){
      var a = true;
      while(a){
        println("*****************GOODBYE************e**")
        a = false;
      }
      exitprogram(scan)
    }
 
    
    
    
    
}
