
package mqttmsghandler.utilities;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Logger;

/**
 *
 * @author zakhiyah arsal
 */
public class MariaDBSQLConnection {
    
    public static java.sql.Date getCurrentDatetime() {
        java.util.Date today = new java.util.Date();
        return new java.sql.Date(today.getTime());
    }
    
    static Logger mLog = Logger.getLogger(MariaDBSQLConnection.class.getName());
    
    public static Connection getConnection() throws IOException, SQLException {
                Config properties = new Config();
                String driver=new String(properties.getProperty("db.driver"));
                String server_ip=new String(properties.getProperty("db.ip"));
                String port=new String(properties.getProperty("db.port"));
                String dbname=new String(properties.getProperty("db.name"));
                String username=new String(properties.getProperty("db.username"));
                String password=new String(properties.getProperty("db.password"));
                
		Connection conn = null;
		try {
                        Class.forName("org.mariadb.jdbc.Driver");
			StringBuilder url = new StringBuilder("jdbc:mariadb://");
                        url.append(server_ip).append(":").append(port).append("/").append(dbname);
                        //System.out.println(url.toString());
                        conn = DriverManager.getConnection(url.toString(), username, password);
                        //System.out.println("mariaDB Connection Created");
		} catch (ClassNotFoundException e) {	
			e.printStackTrace();
		}
	return conn;
	}
    
    
    public static void insertTankData(int TankNo, int ConsoleId, String SiteID, String ProductID ,Date dateRTS,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV,int Status) {
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        String RTS = dateFormat.format(dateRTS);
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            mLog.info("Insert data into tankdata");

            ps3 = conn.prepareStatement("Insert into tankdata " +
                "(TankNo,ConsoleID,SiteID,ReadTime,ProductID,Level,Volume,Density,Temperature,Mass,VolumeFlowrate,MassFlowrate,GSV,Status) Values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Statement.RETURN_GENERATED_KEYS);

            ps3.setInt(1,TankNo); 
            ps3.setInt(2,ConsoleId);
            ps3.setString(3,SiteID);
            //ps3.setString(4,sf.format(date));
            ps3.setString(4,RTS);
            ps3.setString(5,ProductID); 
            ps3.setFloat(6,Level); 
            ps3.setFloat(7,Volume);
            ps3.setFloat(8,Density); 
            ps3.setFloat(9,Temperature); 
            ps3.setFloat(10,Mass); 
            ps3.setFloat(11,VolumeFlowrate);  
            ps3.setFloat(12,MassFlowrate); 
            ps3.setFloat(13, GSV);
            ps3.setInt(14,Status);
            
            ps3.executeUpdate();
//            mLog.info("**Insert into TRNQUOTACUSTOMER**");
            ps3.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void updateTankData(int TankNo, int ConsoleId, String SiteID, String ProductID, Date dateRTS, float Level, float Volume, float Density, float Temperature, float Mass, float VolumeFlowrate, float MassFlowrate, float GSV, int status) throws ParseException {
    
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        //RTS = ReadTimeSite
        String RTS = dateFormat.format(dateRTS);  
        
        Connection conn = null;
        PreparedStatement ps3 = null;
     
        
        try {
            conn = getConnection();
            mLog.info("Update data into tankdata");
            //mLog.info("RT Update TankData 2 :" +RTS);
            ps3 = conn.prepareStatement("UPDATE TankData SET "+
                    "ReadTime=?,Level=?,Volume=?,Density=?,Temperature=?,Mass=?,VolumeFlowrate=?,MassFlowrate=?,GSV=?,status=? "+
                    "where TankNo=? and ConsoleId=? and SiteID=? and ProductID=?");
            
            ps3.setString(1,RTS);
            ps3.setFloat(2,Level); 
            ps3.setFloat(3,Volume);
            ps3.setFloat(4,Density); 
            ps3.setFloat(5,Temperature); 
            ps3.setFloat(6,Mass); 
            ps3.setFloat(7,VolumeFlowrate);  
            ps3.setFloat(8,MassFlowrate); 
            ps3.setFloat(9, GSV);
            ps3.setInt(10, status);
            ps3.setInt(11,TankNo); 
            ps3.setInt(12,ConsoleId);
            ps3.setString(13,SiteID);
            ps3.setString(14,ProductID);
                  
            ps3.executeUpdate();
            ps3.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void processedTankData(int TankNo, int ConsoleId, String SiteID, String readTimeSite ,String ProductID ,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV,int Status) throws ParseException {
        //SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //Date date = new Date();
        
        Date dateRTS=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(readTimeSite);
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            //mLog.info("Process save data");
            //mLog.info("RT Processed :" +readTimeSite);

              ps3 = conn.prepareStatement(" select * from TankData where TankNo=? and ConsoleId=? and SiteID=?");
            
      
            ps3.setInt(1,TankNo); 
            ps3.setInt(2,ConsoleId);
            ps3.setString(3,SiteID);
                  
           ResultSet rs = ps3.executeQuery();
           if (rs.next()) {
               //mLog.info("RT Processed 2 :" +readTimeSite);
               updateTankData(TankNo, ConsoleId, SiteID,ProductID,dateRTS,Level, Volume, Density, Temperature, Mass, VolumeFlowrate, MassFlowrate, GSV,Status);
           } else {
               insertTankData(TankNo, ConsoleId, SiteID,ProductID,dateRTS,Level, Volume, Density, Temperature, Mass, VolumeFlowrate, MassFlowrate, GSV,Status);
           }

            ps3.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void insertTankDataHistory (int TankNo, int ConsoleId, String SiteID ,String readTimeSite,String ProductID,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV,int Status) throws IOException, ParseException {
        //SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        //Date date = new Date();
        
        Date dateRTS=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(readTimeSite);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        //RTS = ReadTimeSite
        String RTS = dateFormat.format(dateRTS);  
        
        UUID uuid = UUID.randomUUID();
        String uuidAsString = uuid.toString();
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            mLog.info("Insert data into tankdatahistory");

            ps3 = conn.prepareStatement("Insert into tankdatahistory " +
                "(RowID,TankNo,ConsoleID,SiteID,ReadTime,ProductID,Level,Volume,Density,Temperature,Mass,VolumeFlowrate,MassFlowrate,GSV,Status) Values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Statement.RETURN_GENERATED_KEYS);
            ps3.setString(1,uuidAsString);
            ps3.setInt(2,TankNo); 
            ps3.setInt(3,ConsoleId);
            ps3.setString(4,SiteID);
            //ps3.setString(5,sf.format(date));
            ps3.setString(5,RTS); 
            ps3.setString(6,ProductID);
            ps3.setFloat(7,Level); 
            ps3.setFloat(8,Volume);
            ps3.setFloat(9,Density); 
            ps3.setFloat(10,Temperature); 
            ps3.setFloat(11,Mass); 
            ps3.setFloat(12,VolumeFlowrate);  
            ps3.setFloat(13,MassFlowrate); 
            ps3.setFloat(14, GSV);
            ps3.setInt(15,Status);
            
            ps3.executeUpdate();
            ps3.close();
            conn.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        
        
    }
        
}
