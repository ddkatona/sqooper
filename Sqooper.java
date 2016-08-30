package sqooper;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.log4j.BasicConfigurator;

/**
 * @author DANIEL KATONA - LynxAnalytics
 */

public class Sqooper {
    
    public static int min[] = new int[500];
    public static int hour[] = new int[500];
    public static String SRC[] = new String[500];
    public static String HQL[] = new String[500];
    public static String TRG[] = new String[500];
    
    public static int NUM;
    
    public static void sqoop(String SOURCE_VIEW, String TARGET_VIEW) {
        String user = "lynxkite";
        String password = "lynxkite";
        String host = "10.6.18.18";
        int port = 22;
        
        String cmd;

        try{
            java.util.Properties config = new java.util.Properties(); 
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            session.setConfig(config);
            session.connect();
            
            //printLog("Sqooping started for " + SOURCE_VIEW + " to " + TARGET_VIEW);
            
            // DELETE PREVIOUS
            for(int func = 0; func < 2; func++) {
                
                if(func == 1) {
                    cmd = "sqoop import -D oraop-disabled=true --connect jdbc:oracle:thin:@10.128.57.82:1521/datamart --username dm --password dm123 --table " + SOURCE_VIEW + " -m 1 --verbose --hive-import --hive-table " + TARGET_VIEW;
                }
                else{
                    cmd = "hadoop fs -rmr " + SOURCE_VIEW;
                }
                
                Channel channel = session .openChannel("exec");
                ((ChannelExec)channel).setCommand(cmd);

                InputStream in=channel.getInputStream();
                channel.connect();
                byte[] tmp=new byte[1024];
                while(!channel.isClosed()){
                  while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                  }
                  
                  try{Thread.sleep(1000);}catch(Exception ee){ }
                }
                channel.disconnect();
            }
        }
        catch(Exception e){
            System.out.println("Failed to sqoop " + SOURCE_VIEW + " to " + TARGET_VIEW);
            //e.printStackTrace();
        }
        
    }
    
    public static class MyRunnable implements Runnable {
        private final String sc;
        private final String tg;
        private final String hq;

        MyRunnable(String sc, String tg, String hq) {
            this.sc = sc;
            this.tg = tg;
            this.hq = hq;
        }
        
        @Override
        public void run() {
            
            boolean comment = false;
            if(sc.length() > 1) {
                if(sc.substring(0, 2).equals("//")){
                    comment = true;
                }
            }
            
            if(!comment){
                
                String exact_name = "20160606"; //DEFAULT DATE

                if(!sc.equals("-")) {
                    try {
                        printLog(sc + "[GETTING EXTENTION]");
                        exact_name = getNewestTable(sc);
                    } catch (SQLException ex) {
                        Logger.getLogger(Sqooper.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (ClassNotFoundException ex) {
                        Logger.getLogger(Sqooper.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    if(!tg.equals("-")) {
                        printLog("[" + exact_name + "] ====SQOOPING====> [" + tg + "]");
                        sqoop(exact_name, tg);
                    }
                }

                if(!hq.equals("-")){
                    try {
                        printLog("EXECUTING [" + hq + "]");
                        callHiveCmd(exact_name, hq, exact_name.substring(exact_name.length() - 8, exact_name.length()));
                    } catch (SQLException ex) {
                        Logger.getLogger(Sqooper.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (ClassNotFoundException ex) {
                        Logger.getLogger(Sqooper.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (IOException ex) {
                        Logger.getLogger(Sqooper.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            
        }
    }
    
    public static void readSCH() throws FileNotFoundException {
        Scanner S = new Scanner(new FileReader("schedule.txt"));
        
        int i = 0;
        while(S.hasNextLine()){
            
            try{
                String part[] = S.nextLine().split(" ");

                SRC[i]  = part[0];
                TRG[i]  = part[1];
                HQL[i]  = part[2];
                hour[i] = Integer.parseInt(part[3]);
                min[i]  = Integer.parseInt(part[4]);

                i++;
            }
            catch(Exception ex) {
                printLog("Warning: Not enough arguments line " + i + "!");
            }
        }
        NUM = i;
    }
    
    public static String getNewestTable(String ext) throws SQLException, ClassNotFoundException {
        Class.forName("oracle.jdbc.OracleDriver");  
        Connection con = DriverManager.getConnection("jdbc:oracle:thin:@10.128.57.82:1521:datamart","dm","dm123");
        Statement stmt = con.createStatement();
        
        //String cmd = "select max(to_number(regexp_substr(view_name, '[0-9]{8,}$'))) as max from all_views where view_name like '" + ext + "%'";
        String cmd = "select view_name as max from ALL_VIEWS AV cross join (select max(TO_NUMBER(REGEXP_SUBSTR(VIEW_NAME, '[0-9]{8,}$'))) as MX from ALL_VIEWS where VIEW_NAME like '" + ext + "%') M where VIEW_NAME like '" + ext + "%' and instr(av.view_name,m.mx) > 0";
        
        ResultSet rs = stmt.executeQuery(cmd);
        //printLog(cmd);
        
        String exact_table = "";
        while ( rs.next() ) {
            exact_table = rs.getString("max");
        }
        
        if(exact_table == null || exact_table.equals("")){
            exact_table = "20160606";
        }
        
        return exact_table;
    }
    
    // prints String with the time
    public static void printLog(String log) {
        Calendar now = Calendar.getInstance();
        int hour = now.get(Calendar.HOUR_OF_DAY);
        int minute = now.get(Calendar.MINUTE);
        System.out.print("[" + String.format("%02d", hour) + ":" + String.format("%02d", minute) + "] ");
        System.out.println(log);
    }
    
    public static void callHiveCmd(String sc, String hql, String date) throws SQLException, ClassNotFoundException, IOException {
        
        if(!sc.equals("-")) {
            String fileHql = readFile(hql, date);
            reWriteFile(hql, fileHql);
        }
        
        exec("hive -f " + hql);
    }
    
    public static String readFile(String fileName, String date) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            int i = 0;
            while (line != null) {
                if(i == 0) {
                    sb.append("SET DAY_ID = '" + date + "';");
                }
                else{
                    sb.append(line);
                }
                
                sb.append("\n");
                line = br.readLine();
                i++;
            }
            return sb.toString();
        } finally {
            br.close();
        }
    }
    
    public static void reWriteFile(String hql, String content) throws FileNotFoundException, IOException {
        File file = new File(hql);
        DataOutputStream outstream= new DataOutputStream(new FileOutputStream(file,false));
        outstream.write(content.getBytes());
        outstream.close();
    }
    
    public static void exec(String cmd) {
        String user = "lynxkite";
        String password = "lynxkite";
        String host = "10.6.18.18";
        int port = 22;

        try{
            java.util.Properties config = new java.util.Properties(); 
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            session.setConfig(config);
            session.connect();
                
            Channel channel = session .openChannel("exec");
            ((ChannelExec)channel).setCommand(cmd);

            InputStream in=channel.getInputStream();
            channel.connect();
            byte[] tmp=new byte[1024];
            while(true){
              while(in.available()>0){
                int i=in.read(tmp, 0, 1024);
                if(i<0)break;
                //System.out.print(new String(tmp, 0, i));
              }
              if(channel.isClosed()){
                //System.out.println("exit-status: "+channel.getExitStatus());
                break;
              }
              try{Thread.sleep(1000);}catch(Exception ee){}
            }
            channel.disconnect();
            
            session.disconnect();
        }
        catch(Exception e){
            //e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException, IOException {
        
        printLog("********* HIVE SCHEDULER 1.0 *********");
        ExecutorService executor = Executors.newCachedThreadPool();
        
        while(true) {
            
            try{
                readSCH();
            }
            catch(FileNotFoundException f) {
                printLog("schedule.txt not found!");
            }
            
            Calendar now = Calendar.getInstance();
            int c_hour = now.get(Calendar.HOUR_OF_DAY);
            int c_min = now.get(Calendar.MINUTE);
            
            for(int i = 0; i < NUM; i++) {
                if(c_hour == hour[i] && c_min == min[i]) {
                    // THREAD
                    executor.execute(new MyRunnable(SRC[i], TRG[i], HQL[i]));
                }
            }
            
            // waiting until next second
            Calendar now2 = Calendar.getInstance();
            int m = now2.get(Calendar.MINUTE);
            while(m == c_min){
                now2 = Calendar.getInstance();
                m = now2.get(Calendar.MINUTE);
            }
            
        }
    }
   
}
