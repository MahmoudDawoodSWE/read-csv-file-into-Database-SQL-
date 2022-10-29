//install -> apache commons lang jar 
//install -> opencsv jar - > https://sourceforge.net/projects/opencsv/files/opencsv/
import java.io.*;   
import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Arrays;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;


class ex5 {
	private static   final String   PATH = "C:\\Users\\Asus\\Desktop\\sof_survey_2021.csv";
	private static  int DATALODAED = 0;
	public static void main(String[] args) {
		menu();
        //DB.printPer();
    	//DB.printLanguage();
    	//DB.language_person_print(); 
}
	 public static int optionForMenu() {
	        Scanner scanner = new Scanner(System.in);
	        int tmp;
	        int option;
	        System.out.print("What do you want to do?\n");
	        System.out.print("1. Load Data\n");
	        System.out.print("2. Language\n");
	        System.out.print("3. Age\n");
	        System.out.print("4. Special\n");
	        System.out.print("6. Exit\n");
	        tmp = scanner.nextInt();
	        return tmp;
	    }
	 public static void menu(){
	        ArrayList<String> arrayList = new ArrayList<String>();
	        Scanner scanner = new Scanner(System.in);
	       while(true) {
	            int option = optionForMenu();
	            switch (option) {
	                case 1:
	                    if(DATALODAED == 1) {
	            	        System.out.print("cannot load the data again\n");
	            	        }
	                    else {
	                    	DB.startTables();
	                    	readFile();
	                    	DATALODAED = 1;
		                    break;
	                    }
	                    break;
	                case 2:
	                    while(true){
	            	        System.out.println("insert Language to insert enter 1  or other number to skip");
	            	        if(1 == scanner.nextInt()) {
		            	        System.out.println("enter language  name");
	            	        	arrayList.add(scanner.next());
	            	        }
	            	        else 
	    	                    break; 	
	                    }
            	        System.out.println("result");
            	        if(arrayList.size() != 0)
	            		DB.command2_print(arrayList);
	                    break;
	                case 3:
	                	String []array = null;
            	    System.out.println("enter the min age than the max age");
	                String age = scanner.nextInt()+"-"+scanner.nextInt()+" years old";
	                arrayList.add(age);
	                String country;
            	    System.out.println("insert country to insert enter 1  or other number to skip");
        	        if(1 == scanner.nextInt()) {
                	    System.out.println("enter the country");
                	    arrayList.add(scanner.next());
                		DB.command3_count(arrayList);
        	        }
        	        else {
        	        System.out.println("result");
        	        DB.command3_count(arrayList);
        	        }
	                    break;
	                case 4:
	                	System.out.println("result");
	                    DB.command4();
	                    break;
	                case 6:
	                    return;
	                   
	                default:
	                    System.out.println("Enter number from the list");
	                    break;
	            }
	        }
	       }
	public static void readFile() {
		try {
			CSVReader reader = new CSVReader(new FileReader(PATH));
			String []row;
			String []Language;
	        ArrayList<String> arrayList = new ArrayList<String>();
			boolean flag;
			int counter = 0;
			reader.readNext();
			while((row=reader.readNext()) != null) {
				DB.insertrPer(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[8], row[9]);
				Language= row[7].split(";");
				for(int i=0;i<Language.length;i++) {
				flag = true;
				for(String s: arrayList) {
					if(s.equals(Language[i])) {
						flag = false;}}
				if(flag) {
					DB.insertrLanguage(counter, Language[i]);;
					counter++;
					arrayList.add(Language[i]);}
				
				}
				for(int i=0;i<Language.length;i++) {
					for(String s: arrayList) { 
						if(s.equals(Language[i])) {
							DB.language_person(arrayList.indexOf(Language[i]),row[0]);
							} }
						}		
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CsvValidationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
}
}
class DB {
    public static Connection connect() {
        Connection connection = null;
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:sof_survey_2021");
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
        finally {
            return connection;
        }
    }
    
    public static void startTables() {
    	Connection con = connect();
		String commandTablePerson="create table Person("
				+ "ID varchar(10) primary key,"
				+ "MainBranch varchar(20),"
				+ "Employment varchar(20),"
				+ "Country varchar(20),"
				+ "Age1stCode varchar(20),"
				+ "LearnCode varchar(20),"
				+ "YearsCode varchar(20),"
				+ "age varchar(20),"
				+ "Gender varchar(20)"
				+ ");";
		String commandTableLanguage="create table Language("
				+ "IdLanguage int primary key,"
				+ "nameLanguage varchar(20)"
				+ ");";
		String commandTableSpeakLanguage="create table SpeakLanguage("
				+ "IdLanguage int,"
				+ "ID varchar(10),"
				+ "foreign key(ID) references Person,"
				+ "foreign key(IdLanguage) references Language"
				+ ");";
		
        try {
			Statement st = con.createStatement();
			 st.execute(commandTablePerson);
			 st.execute(commandTableLanguage);
			 st.execute(commandTableSpeakLanguage);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
    public static void deleatTable(String name, Connection co) {
    	String deleatTableSQL = "DROP TABLE " + name;
    	 try {
 			Statement st = co.createStatement();
 			 st.execute(deleatTableSQL);

 		} catch (SQLException e) {
 			e.printStackTrace();
 		}
    }
    public static void insertrPer(String id,String MainBranch, String Employment,String Country, String Age1stCode ,String LearnCode,String YearsCode, String age, String Gender) {
        Connection con = connect();
    	String insertSQL ="INSERT INTO Person(ID, MainBranch, Employment, Country, Age1stCode, LearnCode, YearsCode, age, Gender) VALUES(?,?,?,?,?,?,?,?,?)";
    	try {
    		PreparedStatement pre= con.prepareStatement(insertSQL);
    		pre.setString(1, id);
    		pre.setString(2, MainBranch);
    		pre.setString(3, Employment);
    		pre.setString(4, Country);
    		pre.setString(5, Age1stCode);
    		pre.setString(6, LearnCode);
    		pre.setString(7, YearsCode);
    		pre.setString(8, age);
    		pre.setString(9, Gender);
    		pre.executeUpdate();
    		pre.close();
            con.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void insertrLanguage(int idLan, String nameLan) {
        Connection con = connect();
    	String insertSQL ="INSERT INTO Language(IdLanguage, nameLanguage) VALUES(?,?)";
    	try {
    		PreparedStatement pre= con.prepareStatement(insertSQL);
    		pre.setInt(1, idLan);
    		pre.setString(2, nameLan);
    		pre.executeUpdate();
    		pre.close();
            con.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void language_person(int IdLanguage, String ID) {
        Connection con = connect();
    	String insertSQL ="INSERT INTO SpeakLanguage(IdLanguage, ID) VALUES(?,?)";
    	try {
    		PreparedStatement pre= con.prepareStatement(insertSQL);
    		pre.setInt(1, IdLanguage);
    		pre.setString(2, ID);
    		pre.executeUpdate();
    		pre.close();
            con.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void printPer() {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql = "select * from Person ;";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()) {
                String s = res.getString("ID");
//            	+ ", " + res.getString("age") + ", " + res.getString("LearnCode")+ ", "+ ", " + res.getString("Gender")+ ", " 
//                +res.getInt("YearsCode") + ", " +res.getString("Country")+ ", " +res.getString("Employment")+ ", "
//                		+res.getString("MainBranch")+ ", " +res.getString("Age1stCode") ;
                System.out.println(s);
            }
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void printLanguage() {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql = "select * from Language;";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()) {
                String s = res.getInt("IdLanguage") + ", " + res.getString("nameLanguage");
                System.out.println(s);
            }
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void language_person_print() {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql = "select * from SpeakLanguage;";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()) {
                String s = res.getInt("IdLanguage") + ", " + res.getString("ID");
                System.out.println(s);
            }
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void command2_print(ArrayList<String> arrayList) {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql = "select P.Employment,P.Country,P.age from SpeakLanguage S,Language L,Person P where S.ID = p.ID AND L.IdLanguage = S.IdLanguage AND (";
            for(int i = 0;i<(arrayList.size() -1);i++) {
            	sql+="L.nameLanguage = '"+arrayList.get(i)+"'or ";
            }
            sql += "L.nameLanguage = '"+arrayList.get(arrayList.size() -1)+"' );";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()) {
                String s =  "Employment<" + res.getString("Employment") + ">  Country<" + res.getString("Country")+ ">  age<" + res.getString("age")+">";
                System.out.println(s);
            }
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void command2_count(String[] lan) {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql = "select count(*)  from SpeakLanguage S,Language L,Person P where S.ID = p.ID AND L.IdLanguage = S.IdLanguage AND (";
            for(int i = 0;i<(lan.length -1);i++) {
            	sql+="L.nameLanguage = '"+lan[i]+"'or ";
            }
            sql += "L.nameLanguage = '"+lan[lan.length -1]+"' );";
            ResultSet res = statement.executeQuery(sql);
            res.next();
                String s =  "the total number of the pepole who knows "+Arrays.toString(lan)+" --> " + res.getString(1);
                System.out.println(s);
            
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void command3_count(ArrayList<String> arrayList) {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql = null;
            if(arrayList.size() == 2) {
            	sql = "select count(*)  from Person P where P.age = '"+arrayList.get(0)+"'and P.Country = '"+arrayList.get(1)+"' ;";}
            if(arrayList.size() == 1) {
                sql = "select count(*)  from Person P where P.age = '"+arrayList.get(0)+"' ;";}
            
            ResultSet res = statement.executeQuery(sql);
            res.next();
                String s =  "the total number of the pepole who have age "+arrayList.get(0);
                if(arrayList.size() == 2)
                	s+="and live in "+arrayList.get(1);
                s+="  is --->"+ res.getString(1);
                System.out.println(s);
            
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
    public static void command4() {
        try {
            Connection connection = connect();
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();
            String sql1 = ""
            		+ "CREATE VIEW  two as "
            		+ " select P.ID,count(*) AS D"
           		    + " from SpeakLanguage P "
            		+ "	group by P.ID "
            		+ " having count(*) <= 2 ; ";
            String sql ="select P.ID,P.MainBranch,P.Employment,P.Age1stCode,P.LearnCode,P.YearsCode,P.age,P.Gender "
            	    + "from Person P , two T "
            	    + "where T.ID =P.ID and  P.Country = 'United States of America' ;";
      
            statement.executeUpdate(sql1);
            ResultSet res = statement.executeQuery(sql);

            res.next();
            while(res.next()) {
                String s =  "ID<" + res.getString(1)
                +">_MainBranch<"+ res.getString("MainBranch")+">__Employment<"+ res.getString("Employment")+">__Age1stCode<"+
                		res.getString("Age1stCode")+
                ">__LearnCode<"+ res.getString("LearnCode")+
                ">__YearsCode<" + res.getString("YearsCode")+">__age<"+ res.getString("age")+">__Gender<" + res.getString("Gender")+">";
                System.out.println(s);

            }
            res.close();
            statement.close();
            connection.close();
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }
}


