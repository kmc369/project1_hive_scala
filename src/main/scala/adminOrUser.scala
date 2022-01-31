class adminOrUser {
import java.util.Scanner;

   

def choice(scan:Scanner):Unit = { 
          
//var scan = new Scanner(System.in)
        var a = true;
    do{
             println("-----------------------------------------------------")
             println("-----------------------------------------------------")
             println("-----------------------------------------------------")
             println("      ARE AN ADMIN OR EVERYDAY USER?");
             println("-----------------------------------------------------")
             println("-----------------------------------------------------")
             println("-----------------------------------------------------")
    var choice = scan.nextLine().toUpperCase();
   
            if (choice == "ADMIN"){
              
                var adminL = new adminLogin();
                 adminL.login(scan);
                a = false;
            }
            if (choice == "USER"){
                
                  var userL = new userLogin();
                  userL.login(scan);
                a =false;
            }
        
       
    }while(a)
    
        }

}


         

    











