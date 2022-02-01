class userLogin {
          import org.apache.spark.sql.hive.HiveContext
          import org.apache.spark.SparkConf
          import org.apache.spark.SparkContext
          import org.apache.spark.SparkContext._
          import org.apache.spark.sql.SQLContext
          

          System.setSecurityManager(null)
       //System.setProperty() // change if winutils.exe is in a different bin folder
         val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project1")    // Change to whatever app name you want
        val sc = new SparkContext(conf)
            sc.setLogLevel("ERROR")
         val hiveCtx = new HiveContext(sc)
            import hiveCtx.implicits._
          
          
          import java.util.Scanner
          import java.sql.SQLException;
          import java.sql.Connection;
          import java.sql.ResultSet;
          import java.sql.Statement;
          import java.sql.DriverManager;
          val driver= "com.mysql.cj.jdbc.Driver";
          val url ="jdbc:mysql://localhost:3306/project1";
          val usN = "sqluser";
          val pass = "password";
          val connection = DriverManager.getConnection(url,usN,pass);
          Class.forName(driver);
          val statement = connection.createStatement();
 
 
 
 
 def login(scan:Scanner): Unit ={
    var a= true;   
 do{       
    try{
        println("ENTER USERNAME")
        var attemptedName = scan.nextLine();
        
        println("ENTER PASSWORD")
        var attemptedPassword = scan.nextLine().toLowerCase()
       

            
          Class.forName(driver);
          val statement = connection.createStatement();
    
    
 
          val table = "user_login";
          var query = ("SELECT user_password FROM  user_login where user_name = '" + attemptedName  +"'; ")
          var res = statement.executeQuery(query);
          var x = ""
          
            while (res.next()) {
            
               
               
              x=res.getString(1)
          
          }
      
                 if (attemptedPassword==x){
                updatepassword(scan)
            }
          
           else{
                
             throw new BadUserException
            }
            
       } catch {
           case e: BadUserException => println("PASSWORD INCORRECT PLEASE TRY AGAIN")
            a=false;
       }
      
      }while(a!=true)
  
    

  }
        

      

   def updatepassword(scan:Scanner): Unit = {
        
    var b = true;
  do{
        var timeElapse = scan.nextLine().toUpperCase()
         //var query = ("SELECT * from user_login")
          //var res = statement.executeQuery(query);
        try {

    
          println("HAS IT BEEN MORE THAN 30 DAYS SINCE YOUR SIGN-UP DATE?")
          var timeElapse = scan.nextLine().toUpperCase()
        
       if(timeElapse=="YES"){
          println("ENTER USERNAME")
          var userName= scan.nextLine()

          println("ENTER UPDATED USERNAME")
          var newUserName= scan.nextLine();
          println("ENTER UPDATED PASSWORD")
          var newPassword = scan.nextLine()
       
        //var query = ("UPDATE user_login SET user_password = ('" + newPassword+ "'), user_name = ('"+ newUserName +"') Where user_name = ('"+ userName + "');")
        var query = ("UPDATE user_login  SET user_name = ('"+newUserName+"'), user_password = ('"+newPassword+"') WHERE user_name = ('"+userName+"');")

        val rs =statement.executeUpdate(query)
        
        questions(hiveCtx:HiveContext,scan)
        }

        else if (timeElapse == "NO"){
         questions(hiveCtx:HiveContext,scan)
      } 
    
       
    }catch {
      case e : BadUserException => println("PLEASE ENTER YES OR NO")
      b= false
    }
  
     
  } while (b!=true)    
  
  
  
  
  
  }



def questions(hiveCtx:HiveContext, scan:Scanner): Unit = {



var b = true;

do{
      
     println("1) IN WHAT COUNTRY AM I MOST LIKELY TO SPOT A UFO?")
     println()
    
     println("2) WHAT IS THE LONGEST ENCOURTERS SOMEONE HAS HAD WITH A UFO IN THE UNITED STATES")
     println()
    
     println("3) WHAT IS THE MOST COMMON SHAPE OF UFO'S THAT ARE IDENTIFIED?")
     println()
     
     println("4) WHAT IS THE AVERAGE TIME SOMEONE HAS SEEN A UFO IN NEW JERSEY?")
     println()
    
     println("5) WHAT SHAPE IS MOSTLY LIKELY TO BE CONFUSED AS A UFO??")
     println()
     
     println("6) WHAT STATE AM I LEAST LIKELY TO SPOT A UFO IN THE UNITED STATES?")
     println()
     println("7) RETURN TO LOGIN?")
     println()
     println("8) EXIT")
    
     var a = scan.nextInt() 

 

    val output = hiveCtx.read
           .format("csv")
           .option("inferSchema", "true")
           .option("header", "true")
           .load("/Users/zenw/ufo_data/complete.csv")
         output.limit(1).show() 
  
  
  output.createOrReplaceTempView("temp_data")
      
      /*  hiveCtx.sql("USE project1_hive_scala;")
  
    
        
        
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS ufo_data (datetime String, city STRING, state STRING, country string, shape STRING, duration_seconds String, duration_HoursMin String, comments String, date_posted STRING,latitude STRING, Longitude STRING) row format delimited fields terminated by ',' stored as textfile; ") 
        hiveCtx.sql("INSERT INTO ufo_data SELECT * FROM temp_data")
          
      val questions = hiveCtx.sql("SELECT * FROM ufo_data LIMIT 10;")
     // questions.show()

      hiveCtx.sql("CREATE TABLE IF NOT EXISTS ufo_data_part(datetime String, city STRING, country string, shape STRING, duration_seconds String, duration_HoursMin String, comments String, date_posted STRING,latitude STRING, Longitude STRING) Partitioned by (state String) row format delimited fields terminated by ',' stored as textfile; ") 
      hiveCtx.sql(" set hive.exec.dynamic.partition.mode=nonstrict;")
      hiveCtx.sql("INSERT INTO ufo_data_part SELECT datetime String, city STRING, country string, shape STRING, duration_seconds String, duration_HoursMin String, comments String, date_posted STRING,latitude STRING, Longitude STRING , state String  FROM ufo_data;")

      val part = hiveCtx.sql("select * from ufo_data_part limit 10")
      //part.show()

    
      hiveCtx.sql("CREATE TABLE IF NOT EXISTS ufo_data_bucket(datetime String, city STRING, country string, shape STRING, duration_seconds String, duration_HoursMin String, comments String, date_posted STRING,latitude STRING, Longitude STRING) Partitioned by (state String) CLUSTERED BY (city) into 4 buckets row format delimited fields terminated by ',' stored as textfile; ") 
      hiveCtx.sql("set hive.enforce.bucketing=true;")
      //hiveCtx.sql("INSERT INTO ufo_data_bucket SELECT datetime String, city STRING, country string, shape STRING, duration_seconds String, duration_HoursMin String, comments String, date_posted STRING,latitude STRING, Longitude STRING , state String  FROM ufo_data;")
      
      //val buck = hiveCtx.sql("select * from ufo_data_bucket limit 10")
      //buck.show()
      */
   
      hiveCtx.sql("USE project1_hive;")
   
   
   
    if(a == 1){
       
      val country_max = hiveCtx.sql("SELECT count(country) as totalsighting, country from ufo_808_part group by country order by totalsighting desc limit 1;")
      country_max.show()

    }
     if(a == 2){
       
     val max_encounter = hiveCtx.sql("Select MAX(duration_seconds) as encountertime, state from ufo_808_part WHERE country = 'us' group by state order by  encountertime DESC limit 5;")
      max_encounter.show()

      //WHAT IS THE LONGEST ENCOURTERS SOMEONE HAS HAD WITH A UFO IN THE UNITED STATES //help with this query
    }
     if(a == 3){
       
      val ufo_shape = hiveCtx.sql("SELECT Count(shape) as totalshape ,shape  FROM ufo_808_part group by shape order by totalshape desc limit 5;")
      ufo_shape.show();

      //WHAT IS THE MOST COMMON SHAPE OF UFO'S THAT ARE IDENTIFIED?
    
    }
    if (a ==4){
     val city_shape = hiveCtx.sql("SELECT AVG(duration_seconds) FROM ufo_808_part WHERE state = 'nj';")
    city_shape.show()
    //WHAT IS THE AVERAGE TIME SOMEONE HAS SEEN A UFO IN NEW JERSEY
    }
    
  if (a==5){
    val total_time = hiveCtx.sql("SELECT Min(shape) as possibleshape, shape from ufo_808_part group by shape order by possibleshape ASC limit 2; ")
    total_time.show();
    //WHAT SHAPE IS MOSTLY LIKELY TO ME CONFUSED AS A UFO?
  
  }

  if(a==6){
    val least_likely = hiveCtx.sql("SELECT count(state) as leastlikely,state from ufo_808_part where country = 'us'  group by state order by leastlikely ASC limit 5; ")
    least_likely.show()
 //WHAT state AM I LEAST LIKELY TO SPOT A UFO  
}

if (a==7){
    sc.stop()
    b=false;
    val goback = new adminOrUser().choice(scan)

}
if(a==8){
  
  b =false;
}

}while(b) 
 
 //val goback = new adminOrUser().choice(scan)
 

} 




/*def showdata(hiveCtx:HiveContext, scan:Scanner): Unit = {
      
   
     
     val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("/Users/zenw/ufo_data/complete.csv")
        //output.limit(1).show() 
      


       //output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("USE project1_hive;")
        //hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1 (datetime String, city STRING, state STRING, country string, shape STRING, duration_seconds String, duration_HoursMin String, comments String, date_posted STRING,latitude STRING, Lngitude STRING) row format delimited fields terminated by ',' stored as textfile; ") 
        //hiveCtx.sql("INSERT INTO data1 SELECT * FROM temp_data")
          
      val summary = hiveCtx.sql("SELECT * FROM ufo_data_part LIMIT 10;") //this is my partitioned table
      summary.show()
      sc.stop()
     
     
      questions(hiveCtx,scan:Scanner)
     
     
     
  

     
  }*/









}