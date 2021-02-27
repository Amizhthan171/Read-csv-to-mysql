package csv;

import java.io.*;
import java.sql.*;

import org.ini4j.Wini;

public class covid {

	public static void main(String[] args) {

    	Connection myConn = null;
    	Statement myStmt=null;
        ResultSet myRs=null;
   
       
       
        String csvFilePath = "/Users/amizhthan/Desktop/covid.csv";
        int batchSize = 20;
        
    	try{
    	   	String loc=args[0];
            Wini ini = new Wini(new File(loc));
            String uname = ini.get("owner", "username");
            String pass = ini.get("owner", "password");
            String url = ini.get("database", "url");        
            myConn = DriverManager.getConnection(url, uname, pass);
            myConn.setAutoCommit(false);
			System.out.println("\nConnection successful!\n");
			String sql = "INSERT INTO covid (Province_State, Country_region, Last_Update, Lat, Long_,confirmed,Deaths,Recovered,active,FIPS,Incident_Rate,Total_Test_Results,People_Hospitalized,Case_Fatality_Ratio,UID,IS03,Testing_Rate,Hospitalization_Rate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = myConn.prepareStatement(sql);
 
            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;
 
            int count = 0;
 
            lineReader.readLine(); 
            while ((lineText = lineReader.readLine()) != null)
            {
            	String[] data = lineText.split(",");
            	String province=data[0];
            	String country=data[1];
            	String last = data[2];
            	String lat=data[3];
            	String long_=data[4];
            	String confirmed=data[5];
            	String deaths=data[6];
            	String Recovered=data[7];
            	String active=data[8];
            	String fips=data[9];
            	String incident=data[10];
            	String total=data[11];
            	String people=data[12];
            	String case_=data[13];
            	String uid=data[14];
            	String iso=data[15];
            	String test=data[16];
            	String hospital=data[17];
            	
            	statement.setString(1, province);
            	statement.setString(2, country);
            	Timestamp sqlTimestamp = Timestamp.valueOf(last);
                statement.setTimestamp(3, sqlTimestamp);
                Float flat = Float.parseFloat(lat);
                statement.setFloat(4, flat);
                Float flon = Float.parseFloat(long_);
                statement.setFloat(5, flon);
                Integer con=Integer.parseInt(confirmed);
                statement.setInt(6,con);
                Integer death=Integer.parseInt(deaths);
                statement.setInt(7,death);
                Float reco = Float.parseFloat(Recovered);
                statement.setFloat(8, reco);
                Float act = Float.parseFloat(active);
                statement.setFloat(9, act);
                Float fip = Float.parseFloat(fips);
                statement.setFloat(10, fip);
                Float inci = Float.parseFloat(incident);
                statement.setFloat(11, inci);
                Float tot = Float.parseFloat(total);
                statement.setFloat(12, tot);
                Integer peop=Integer.parseInt(people);
                statement.setInt(13,peop);
                Float cas = Float.parseFloat(case_);
                statement.setFloat(14, cas);
                Float ui = Float.parseFloat(uid);
                statement.setFloat(15, ui);
                statement.setString(16, iso);
                Double testing=Double.parseDouble(test);
                statement.setDouble(17,testing);
                Integer hosp=Integer.parseInt(hospital);
                statement.setInt(18,hosp);
                statement.addBatch();
                
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            } lineReader.close();
            statement.executeBatch();
            
            myConn.commit();
            myConn.close();

    	}catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
 
            try {
                myConn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
	}
}
}
