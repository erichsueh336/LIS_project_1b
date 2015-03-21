package com.cs5300.pj1;

import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
//import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.sql.Timestamp;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;




// For simpleDB connection
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.*;
// Self-defined class
import com.cs5300.pj1.SessionData;
import com.cs5300.pj1.ServerStatus;

/**
 * Servlet implementation class HelloWorld
 */
@WebServlet("/HelloWorld")
public class HelloWorld extends HttpServlet {
    private static final long serialVersionUID = 1L;
    
    // Local Data Table
    private static Map<String, SessionData> data_table = new HashMap<String, SessionData>();
    private static Map<String, ServerStatus> group_view = new HashMap<String, ServerStatus>();
    
    // Parameters
    private static String local_IP = null;				// Local IP address
    private static String cookie_name = "CS5300PJ1ERIC";// Default cookie name
    private static int garbage_collect_period = 600; 	// Time to run garbage collection again
    private static int view_exchange_period = 600; 		// Time to run view exchange thread again
    private static int sess_num = 0;					// Part of session ID
    private static int sess_timeout_secs = 1800000; 	// Default session timeout duration
    private static int UDP_port = 8888;					// Default UDP port
    private static int RPC_call_ID = 0;					// RPC call ID, increment after each call
    private static boolean connectedToDB = false; 		// Check whether a new booted server has view or not
    private static boolean isLocal = true; 				// True, for local test
    
    /**
     * @throws IOException 
     * @see HttpServlet#HttpServlet()
     */
    public HelloWorld() throws IOException {
        super();
        connectedToDB = getViewFromSimpleDB();
        startGarbageCollect(garbage_collect_period);
        startViewExchange(view_exchange_period);
        local_IP = getLocalIP(isLocal);
        System.out.println("Local IP = " + local_IP);
        startUDPServer(UDP_port);
        // add local server into view
        group_view.put(local_IP, new ServerStatus("UP", System.currentTimeMillis()) );
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // initialization
        String cookie_str = "";
        String cookie_timeout = "";
        String test = "";
        
        // Get cookie from HTTP request
        Cookie c = getCookie(request, cookie_name);
        
        if (c == null) {
            // No cookie --> first time user
        	// SessID = < sess_num, SvrID >
            // Cookie value = < SessID, version, < SvrIDprimary, SvrIDbackup > >
        	String cookie_value = null;
        	
        	// TODO when deploy to EB, uncomment line below
        	// while (!connectedToDB); // wait until view exchanged
        	
            if (connectedToDB == false) { // View not yet retrieved
            	// create non_replicated session cookie
            	cookie_value = String.valueOf(sess_num) + "_" + local_IP + "_0_" 
                        + local_IP + "_0.0.0.0";
            } else { // View retrieved
            	// Randomly choose backup server
            	Random random = new Random();
            	List<String> good_server_list = new ArrayList<String>();
            	for (Map.Entry<String, ServerStatus> entry : group_view.entrySet()) {
            		if (!entry.getKey().equals(local_IP) && entry.getValue().getStatus().equals("UP")) {
            			good_server_list.add(entry.getKey());
            		}
            	}
            	String randomServerIP = good_server_list.get( random.nextInt(good_server_list.size()) );
            	            	
            	// TODO Write to backup server
            	// RPC: sessionWrite(sessID, version, data, discard_time)
            	
            	// Construct cookie
            	cookie_value = String.valueOf(sess_num) + "_" + local_IP + "_0_" 
                        + local_IP + "_" + randomServerIP;
            }
        	
        	// store new session data into table         
            test = "first time user, on " + local_IP + ", sess_num = " + String.valueOf(sess_num);
            sess_num++; // increment session number for future session
            Cookie new_cookie = new Cookie(cookie_name, cookie_value); // create cookie
            new_cookie.setMaxAge(sess_timeout_secs); // set timeout to one hour
            response.addCookie(new_cookie); // add cookie to data_table
            
            data_table.put(cookie_value, new SessionData(0, "Hello new user!!", System.currentTimeMillis()  + sess_timeout_secs));
            
            cookie_str = data_table.get(cookie_value).getMessage();
            cookie_timeout = String.valueOf(data_table.get(cookie_value).getTimestamp());
        } else { // returned user
            
            // action control
            String act = request.getParameter("act");
            String[] cookie_value_token = c.getValue().split("_");
            String sessionID = cookie_value_token[0] + "_" + cookie_value_token[1];
            //int version_number = Integer.parseInt(cookie_value_token[2]);
            String primary_server = cookie_value_token[3];
            String backup_server = cookie_value_token[4];
            SessionData sessionData = null;
            // token format: < sess_num, SvrID, version, SvrIDprimary, SvrIDbackup >
            if (primary_server.equals(local_IP) || backup_server.equals(local_IP)) {
                // session data is in local table
                sessionData = data_table.get(sessionID);
                
                // TODO what if the version in data table is smaller than version from cookie?????
            }
            else { // session data not exists in local session table
                
                // TODO Perform RPC sessionRead to primary and backup concurrently
                
                // TODO If request failed, return an HTML page with a message "session timeout or failed"
                
                // TODO Delete cookie for the bad session
            }
            
            if (sessionData == null) {
                test = "Error: cookie exists but sessionData is null!"; // Error occur!
            
            } else { // sessionData exist
                
                // Increment version number
            	sessionData.setVersion(sessionData.getVersion() + 1);

                if (act == null) {
                    test = "revisit";
                    c.setMaxAge(sess_timeout_secs); // reset cookie timeout to one hour
                    sessionData.setTimestamp(System.currentTimeMillis() + sess_timeout_secs);
                    cookie_str = sessionData.getMessage();
                    cookie_timeout = String.valueOf(sessionData.getTimestamp());
                    data_table.put(sessionID, sessionData); // replace old data with same key
                }
                else if (act.equals("Refresh")) { // Refresh button was pressed
                    test = "Refresh";
                    // Redisplay the session message, with an updated session expiration time;
                    sessionData.setTimestamp(System.currentTimeMillis() + sess_timeout_secs);
                    c.setMaxAge(sess_timeout_secs); 		// reset cookie timeout to one hour
                    data_table.put(sessionID, sessionData); // replace old data with same key
                    
                    // Display message
                    cookie_str = sessionData.getMessage();
                    cookie_timeout = String.valueOf(sessionData.getTimestamp());
                }
                else if (act.equals("Replace")) { // Replace button was pressed
                    test = "Replace";
                    // Replace the message with a new one (that the user typed into an HTML form field)
                    // and display the (new) message and expiration time;
                    String message = request.getParameter("message");
                    final byte[] utf8Bytes = message.getBytes("UTF-8"); // get message byte length
                    Scanner sc = new Scanner(message);

                    if (utf8Bytes.length > 512) {
                        test = "String too long";
                    }
                    else if (!sc.hasNext("[A-Za-z0-9\\.-_]+")) {
                        test = "Invalid string! only allow [A-Za-z.-_]";
                    }
                    else { // error message
                        // is safe characters and length < 512 bytes
                        test = "valid replace";
                        sessionData.setTimestamp(System.currentTimeMillis() + sess_timeout_secs);
                        sessionData.setMessage(message);
                        sc.close();
                    }
                    c.setMaxAge(sess_timeout_secs); 		// reset cookie timeout to one hour
                    data_table.put(sessionID, sessionData); // replace old data with same key
                    
                    // Display message
                    cookie_str = sessionData.getMessage();
                    cookie_timeout = String.valueOf(sessionData.getTimestamp());
                }
                else if (act.equals("Logout")) {    // Logout button was pressed
                    test = "Logout";
                    data_table.remove(sessionID); 	// remove cookie information from table
                    c.setMaxAge(0); 				// Terminate the session
                    
                    // Display message
                    cookie_str = "Goodbye";
                    cookie_timeout = "Expired";
                }
                response.addCookie(c); // put cookie in response
                
                // TODO RPC, store new session state into remote server
                // SessionWrite( SessID, new_version, new_data, discard_time )
                // discard time = System.currentTimeMillis() + sess_timeout_secs + delta

                // Try primary or backup server in session ID to avoid obsolete copy
                
                // Response timeout
                // 1. Choose different server for backup
                // 2. No backup can be found, abort
                
                // ReSponse success
                // Update view with < SvrID, up, now >
                // Construct new session cookie

                // Response fail
                // Update view with < SvrID, down, now >
                // Create new cookie with backup = NULL 
            }
        }
        
        // for page display
        request.setAttribute("test", test);
        request.setAttribute("cookie_str", cookie_str);
        request.setAttribute("cookie_timeout", cookie_timeout);
        request.getRequestDispatcher("/HelloWorld.jsp").forward(request, response);
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
    }
    
    // helper functions
    /**
	 * Get cookie by name from HTTP request
	 * 
	 * @param request HTTP client request
	 * @param name cookie name that we want to retrieve
	 * 
	 * @return cookie which has name = "name", or null if not found
	 **/
    public static Cookie getCookie(HttpServletRequest request, String name) {
        if (request.getCookies() != null) {
            for (Cookie cookie : request.getCookies()) {
                if (cookie.getName().equals(name)) {
                    return cookie;
                }
            }
        }
        return null;
    }
    
    // TODO Connect simpleDB to retrieve view
    /**
	 * Connect to SimpleDB server to get membership view
	 **/
    public boolean getViewFromSimpleDB() {
        // Exchange View
        
        // return true if retrieve success
        return true;
    }
    
    /**
	 * Start scheduled thread for garbage collection.
	 * 
	 * @param period run thread every "period" seconds
	 **/
    public void startGarbageCollect(int period) {
    	System.out.println("Start Garbage Collector");
    	ScheduledExecutorService garbageCollector = Executors.newSingleThreadScheduledExecutor();
        garbageCollector.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // Iterate through session table
            	Iterator<Map.Entry<String, SessionData>> it = data_table.entrySet().iterator();
            	long currentTime = System.currentTimeMillis();
            	while (it.hasNext()) {
                    Map.Entry<String, SessionData> sessData = it.next();
                    
                    // collect data which has discard time < now
                    if (sessData.getValue().getTimestamp() < currentTime) {
                    	it.remove();
                    	System.out.println(sessData.getKey() + " = " + sessData.getValue());
                    }
                    
                    it.remove(); // avoids a ConcurrentModificationException
                }
                
            }
        }, 0, period, TimeUnit.SECONDS);
    }
    
    /**
	 * Start scheduled thread for view exchange
	 * 
	 * @param period run thread every "period" seconds
	 **/
    public void startViewExchange(int period) {
    	System.out.println("Start View Exchanger");
    	ScheduledExecutorService viewExchanger = Executors.newSingleThreadScheduledExecutor();
        viewExchanger.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // TODO
                // Randomly choose a server
                // 1. Normal Server
                // RPC : hisView = exchangeViews(myView)
                // Merge hisView and MyView
                // Replace MyView with Merged view

                // 2. SimpleDB
                // Note
                // store the View in SimpleDB as a character string, using an encoding
                // similar to the way metadata is encoded in a session cookie. That way,
                // an entire View can be read from or written back to SimpleDB using a 
                // single API call.

                // Read ViewSDB from SimpleDB.
                // Compute a new merged View, Viewm, from ViewSDB and the current View.
                // Store Viewm back into SimpleDB.
                // Replace the current View with Viewm.

                /* avoid convoys
                Random generator = new Random();
                    ...
                  while(true) {
                    ... gossip with another site chosen at random ...
                    sleep( (GOSSIP_SECS/2) + generator.nextInt( GOSSIP_SECS ) )
                  }
                */


                
            }
        }, 0, period, TimeUnit.SECONDS);
    }
    
    /**
	 * Start UDP server for RPC
	 * 
	 * @param port start server on port
     * @throws IOException 
	 **/
    public void startUDPServer(int port) throws IOException {
        System.out.println("=======================================");
        System.out.println("    UDP server started at port " + UDP_port);
        System.out.println("=======================================");
		(new Thread(new UDPServer(port))).start();
    }
    
    /**
	 * Get server local IP from network interface(local test) or terminal command(AWS)
	 * 
	 * @param isLocal true, if perform local test. false when deployed to AWS
	 * 
	 * @return local IP address
	 **/
    public String getLocalIP(boolean isLocal) {
    	if (isLocal == true) {
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface iface = interfaces.nextElement();
                    // filters out 127.0.0.1 and inactive interfaces
                    if (iface.isLoopback() || !iface.isUp())
                        continue;
    
                    Enumeration<InetAddress> addresses = iface.getInetAddresses();
                    InetAddress addr = addresses.nextElement(); // Get first element (mac address)
                    addr = addresses.nextElement(); // Get second element (ip address)
                    return addr.getHostAddress();
                    
                    /*
                    while(addresses.hasMoreElements()) {
                        InetAddress addr = addresses.nextElement();
                        ip = addr.getHostAddress();
                        System.out.println(iface.getDisplayName() + " " + ip);
                    }
                    */
                }
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        // Method #2: Runtime.exec()
        else {
            try {
                String[] cmd = new String[3];
                cmd[0] = "/opt/aws/bin/ec2-metadata";
                cmd[1] = "--public-ipv4";
                Runtime rt = Runtime.getRuntime();
                Process proc = rt.exec(cmd);
                InputStream stdin = proc.getInputStream();
                InputStreamReader isr = new InputStreamReader(stdin);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ( (line = br.readLine()) != null) {
                    // example output = public-ipv4: ww.xx.yy.zz
                    return line.substring(13);
                }
                int exitVal = proc.waitFor();
                System.out.println("Process exitValue: " + exitVal);
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
        }
    	
    	return null;
    }

}
