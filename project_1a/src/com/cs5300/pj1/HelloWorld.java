package com.cs5300.pj1;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



// For simpleDB connection
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;

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
    private static Map<String, SessionData> session_table = new HashMap<String, SessionData>();
    private static Map<String, ServerStatus> group_view = new HashMap<String, ServerStatus>();
    
    // Parameters
    private static String local_IP = null;				// Local IP address
    private static String cookie_name = "CS5300PJ1ERIC";// Default cookie name
    private static String domain_name = "dbView";		// Name of simpleDB's group view
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
        local_IP = getLocalIP(isLocal);
        System.out.println("Local IP = " + local_IP);
        
        // UDP server initialize
        startUDPServer(UDP_port);
        
        connectedToDB = getViewFromSimpleDB();
        startGarbageCollect(garbage_collect_period);
        startViewExchange(view_exchange_period);
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
    	// Update local server status
    	group_view.put(local_IP, new ServerStatus("UP", System.currentTimeMillis()) );
    	
    	
    	// initialization
        String cookie_str = "";
        String cookie_timeout = "";
        String test = "";
        
        // Get cookie from HTTP request
        Cookie c = getCookie(request, cookie_name);
        
        if (c == null) {
            // No cookie --> first time user
        	// SessID = < sess_num, SvrIP >
            // Cookie value = < SessID, version, < SvrIDprimary, SvrIDbackup > >
        	String cookie_value = null;
        	SessionData first_session_data = new SessionData(0, "Hello new user!!", System.currentTimeMillis()  + sess_timeout_secs);
        	// TODO when deploy to EB, uncomment line below
        	// while (!connectedToDB); // wait until view exchanged
        	
            if (connectedToDB == false) { // View not yet retrieved
            	// create non_replicated session cookie
            	cookie_value = String.valueOf(sess_num) + "_" + local_IP + "_0_" 
                        + local_IP + "_0.0.0.0";
            } else { // View retrieved
            	// Randomly choose backup server from group view
            	String backup_ip = null;
            	backup_ip = writeDataToBackupServer(first_session_data);
            	
            	// Construct cookie
            	if (backup_ip != null) {
            		cookie_value = String.valueOf(sess_num) + "_" + local_IP + "_0_" 
            				+ local_IP + "_" + backup_ip;
            	} else {
            		cookie_value = String.valueOf(sess_num) + "_" + local_IP + "_0_" 
            				+ local_IP + "_0.0.0.0";
            	}
            }
        	
        	// store new session data into table
            session_table.put(String.valueOf(sess_num) + "_" + local_IP, first_session_data);
            
            test = "first time user, on " + local_IP + ", sess_num = " + String.valueOf(sess_num);
            sess_num++; // increment session number for future session
            
            Cookie new_cookie = new Cookie(cookie_name, cookie_value); // create cookie
            new_cookie.setMaxAge(sess_timeout_secs); // set timeout to one hour
            response.addCookie(new_cookie); // add cookie to session_table
            
            // Display on page
            cookie_str = session_table.get(cookie_value).getMessage();
            cookie_timeout = String.valueOf(session_table.get(cookie_value).getTimestamp());
            
        } else { // returned user
            
            // action control
            String act = request.getParameter("act");
            String[] cookie_value_token = c.getValue().split("_");
            String sessionID = cookie_value_token[0] + "_" + cookie_value_token[1];
            int version_number = Integer.parseInt(cookie_value_token[2]);
            String primary_server = cookie_value_token[3];
            String backup_server = cookie_value_token[4];
            SessionData sessionData = null;
            // token format: < sess_num, SvrIP, version, SvrIPprimary, SvrIPbackup >
            if ( (primary_server.equals(local_IP) || backup_server.equals(local_IP)) 
            		&& (version_number == session_table.get(sessionID).getVersion())) {
            	// local IP address is either primary server or backup server for this session
            	// and the version number is correct --> session data is in local table
                sessionData = session_table.get(sessionID);
            }
            else { // session data does not exists in local session table
                
                // Perform RPC sessionRead to primary and backup concurrently
            	sessionData = RPCSessionRead(sessionID, version_number, primary_server, backup_server);
            }
            
            if (sessionData == null) {
                
                // TODO sessionData means RPC request failed
            	// return an HTML page with a message "session timeout or failed"
            	test = "Error: session timeout or failed!"; // Error occur!
                // TODO Delete cookie for the bad session
            	c.setMaxAge(0); // Terminate the session
            	response.addCookie(c); // put cookie in response
            	
            } else { // sessionData exist
                
                // Increment version number
            	sessionData.setVersion(sessionData.getVersion() + 1);
            	boolean isLogout = false;
                if (act == null) {
                    test = "revisit";
                    c.setMaxAge(sess_timeout_secs); // reset cookie timeout to one hour
                    sessionData.setTimestamp(System.currentTimeMillis() + sess_timeout_secs);
                    cookie_str = sessionData.getMessage();
                    cookie_timeout = String.valueOf(sessionData.getTimestamp());
                    session_table.put(sessionID, sessionData); // replace old data with same key
                }
                else if (act.equals("Refresh")) { // Refresh button was pressed
                    test = "Refresh";
                    // Redisplay the session message, with an updated session expiration time;
                    sessionData.setTimestamp(System.currentTimeMillis() + sess_timeout_secs);
                    c.setMaxAge(sess_timeout_secs); 		// reset cookie timeout to one hour
                    session_table.put(sessionID, sessionData); // replace old data with same key
                    
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
                    else if (!sc.hasNext("[A-Za-z0-9\\.-]+")) {
                        test = "Invalid string! only allow [A-Za-z.-]";
                    }
                    else { // error message
                        // is safe characters and length < 512 bytes
                        test = "valid replace";
                        sessionData.setTimestamp(System.currentTimeMillis() + sess_timeout_secs);
                        sessionData.setMessage(message);
                        sc.close();
                    }
                    c.setMaxAge(sess_timeout_secs); // reset cookie timeout to one hour
                    session_table.put(sessionID, sessionData); // replace old data with same key
                    
                    // Display message
                    cookie_str = sessionData.getMessage();
                    cookie_timeout = String.valueOf(sessionData.getTimestamp());
                }
                else if (act.equals("Logout")) { // Logout button was pressed
                	isLogout = true;
                	test = "Logout";
                    session_table.remove(sessionID); // remove cookie information from table
                    c.setMaxAge(0); // Terminate the session
                    
                    // Display message
                    cookie_str = "Goodbye";
                    cookie_timeout = "Expired";
                }
                
                if (!isLogout) {
	                // TODO RPC, store new session state into remote server                
	                String backup_ip = null;
	            	backup_ip = writeDataToBackupServer(sessionData);
	            	
	            	// Construct cookie
	            	if (backup_ip != null) { // Response success
	            		c.setValue(String.valueOf(sess_num) + "_" + local_IP + "_" + sessionData.getVersion()
	            				+ "_" + local_IP + "_" + backup_ip);
	            	} else { // Response fail
	            		c.setValue(String.valueOf(sess_num) + "_" + local_IP + "_" + sessionData.getVersion()
	            				+ "_" + local_IP + "_0.0.0.0");
	            	}
	            	response.addCookie(c); // put cookie in response
                }
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
        // Auto-generated method stub
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
     * @throws IOException 
	 **/
    public boolean getViewFromSimpleDB() throws IOException {
    	
    	
    	AWSCredentials credentials=new BasicAWSCredentials("1JgVDeFfZO5dKcVlo4lY3Fbby1bAbq1lOZ759Eqc",
    			"AKIAJW6MMYVAEV3VWVIQ");
    	final AmazonSimpleDB sdb = new AmazonSimpleDBClient(credentials);

		System.out.println("===========================================");
		System.out.println("Getting Started with Amazon SimpleDB");
		System.out.println("===========================================\n");
    	
		// Get view
		System.out.println("Checking if the domain " + domain_name + " exists in your account:\n");
		
		int isExistViews = 0; // domain
			
		for (String domainName : sdb.listDomains().getDomainNames()) {
			if (domain_name.equals(domainName)) {
				isExistViews = 1;
				break;
			}
		}
		if (isExistViews == 0) {
			System.out.println("domain " + domain_name + " does not exist in simpleDB.\n");
			// Create domain
			System.out.println("Creating domain called " + domain_name + ".\n");
			sdb.createDomain(new CreateDomainRequest(domain_name));

			// Add current server into DB view
			List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();
			sampleData.add(new ReplaceableItem().withName("view").withAttributes(
	                new ReplaceableAttribute().withName("viewString")
	                .withValue(local_IP + "_UP_" + String.valueOf(System.currentTimeMillis()))));
			sdb.batchPutAttributes(new BatchPutAttributesRequest(domain_name, sampleData));

			// Add current server into local view
			group_view.put(local_IP, new ServerStatus("UP", System.currentTimeMillis()) );
		} else {
			System.out.println("Domain " + domain_name + " exists in simpleDB.\n");
			System.out.println("Download view from simpleDB.\n");
			// Select data from a domain
			// Notice the use of backticks around the domain name in our select expression.
			String selectExpression = "select * from `" + domain_name + "`";
			System.out.println("Selecting: " + selectExpression + "\n");
			SelectRequest selectRequest = new SelectRequest(selectExpression);

			// download simpleDB data
			String viewString = null;
			for (Item item : sdb.select(selectRequest).getItems()) {
				if (item.getName().equals("view")) {
					for (Attribute attribute : item.getAttributes()) {
						viewString = attribute.getValue();
					}
				}
			}
			
			// Add current server into local view
			group_view.put(local_IP, new ServerStatus("UP", System.currentTimeMillis()) );
			
			// Format viewString and put view into group_view
			mergeViewStringToLocalView(viewString);
			
			// View retrieval success
			return true;
		}
		
        // fail to get view
        return false;
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
            	Iterator<Map.Entry<String, SessionData>> it = session_table.entrySet().iterator();
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
    	
    	Thread viewExchangeThread = new Thread() {
            public void run() {
    	        try {
    	        	Random random = new Random();
    	        	while (true) {
	    	        	List<String> good_server_list = new ArrayList<String>();
	                	for (Map.Entry<String, ServerStatus> entry : group_view.entrySet()) {
	                		if (!entry.getKey().equals(local_IP) && entry.getValue().getStatus().equals("UP")) {
	                			good_server_list.add(entry.getKey());
	                		}
	                	}
	                	good_server_list.add("simpleDB");
	                	
	            		int rand = random.nextInt(good_server_list.size());
	            		String randomBackupServerIP = good_server_list.get(rand);
	            		good_server_list.remove(rand); // removed server IP that has been chosen
	            		
	            		if (randomBackupServerIP.equals("simpleDB")) {
	                        // Read ViewSDB from SimpleDB.
	                        // Compute a new merged View, Viewm, from ViewSDB and the current View.
	                        // Store Viewm back into SimpleDB.
	                        // Replace the current View with Viewm.
	            			
	            			
	            			final AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
	            					HelloWorld.class.getResourceAsStream("AwsCredentials.properties")));

	            			System.out.println("===========================================");
	            			System.out.println("Getting Started with Amazon SimpleDB");
	            			System.out.println("===========================================\n");
	            	    	
	            			// Get view
	            			System.out.println("Domain " + domain_name + " exists in simpleDB.\n");
            				System.out.println("Download view from simpleDB.\n");
            				// Select data from a domain
            				// Notice the use of backticks around the domain name in our select expression.
            				String selectExpression = "select * from `" + domain_name + "`";
            				System.out.println("Selecting: " + selectExpression + "\n");
            				SelectRequest selectRequest = new SelectRequest(selectExpression);
            				
            				// download simpleDB data and add to local view.
            				String viewString = null;
            				for (Item item : sdb.select(selectRequest).getItems()) {
            					if (item.getName().equals("view")) {
            						for (Attribute attribute : item.getAttributes()) {
            							viewString = attribute.getValue();
            						}
            					}
            				}
            				
            				// Format viewString and put view into group_view
            				mergeViewStringToLocalView(viewString);
            				
            				// Put updated view back to DB
            				ReplaceableAttribute replaceableAttribute = new ReplaceableAttribute()
                            	.withName("viewString").withValue(getLocalViewString()).withReplace(true);
            				
            				sdb.putAttributes(new PutAttributesRequest()
                            	.withDomainName(domain_name)
                            	.withItemName("view")
                            	.withAttributes(replaceableAttribute));
	            			
	            		} else { // normal valid IP address
	            			mergeViewStringToLocalView(RPCViewExchange(randomBackupServerIP));
	            		}
	            		sleep( (view_exchange_period / 2) + random.nextInt(view_exchange_period));
    	        	}
    	        } catch (Exception e) {
    	        	e.printStackTrace();
    	        }
            }
         };
         viewExchangeThread.start();
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
    
    /**
	 * Start UDP server for RPC
	 * 
	 * @param port start server on port
     * @throws IOException
     * @throws SocketException 
	 **/
    public void startUDPServer(final int port) throws IOException {
        System.out.println("=======================================");
        System.out.println("    UDP server started at port " + UDP_port);
        System.out.println("=======================================");
		
		Thread UDPthread = new Thread() {
            public void run() {
    	        try {
    	        	@SuppressWarnings("resource")
					DatagramSocket rpcSocket = new DatagramSocket(port);
            		while(true) {

        	        	byte[] inBuf = new byte[1024];
        	    		byte[] outBuf = null;
        	    		DatagramPacket recvPkt = new DatagramPacket(inBuf, inBuf.length);
        		        rpcSocket.receive(recvPkt);
        		        
        		        InetAddress IPAddress = recvPkt.getAddress();
        		        int returnPort = recvPkt.getPort();
        		        String recv_string = new String(recvPkt.getData());
        		        String[] recv_string_token = recv_string.split("_");
        		        int operationCode = Integer.parseInt(recv_string_token[1]);
        		        
        		        switch (operationCode) {
        					case 0: // sessionRead
        						outBuf = serverSessionRead(recvPkt.getData(), recvPkt.getLength());
        						break;
        					
        					case 1: // sessionWrite
        						outBuf = serverSessionWrite(recvPkt.getData(), recvPkt.getLength());
        						break;
        					
        					case 2: // exchangeView
        						outBuf = serverExchangeView(recvPkt.getData(), recvPkt.getLength());
        						break;
        						
        					default: // error occur
        						System.out.println("receive unknown operation!!!!");
        						break;
        				}
        		        
        		        DatagramPacket sendPacket = new DatagramPacket(outBuf, outBuf.length, IPAddress, returnPort);
        		        rpcSocket.send(sendPacket);
            		}
    	        } catch (Exception e) {
    	        	e.printStackTrace();
    	        }
            }
         };
         UDPthread.start();
    }
    
    /**TODO
	 * Server RPC to send out session data requested for function RPCSessionRead
	 * 
	 * @param pktData
	 * @param pktLen
	 * 
	 * @return recvStr receive data string
	 **/
    public byte[] serverSessionRead(byte[] pktData, int pktLen) {
    	// Received data format = callID + operation + sessionID(num+ip)
    	// output data format = callID + sessionData(ver#_message_timestamp)
    	String recv_string = new String(pktData);
        String[] recv_string_token = recv_string.split("_");
        String return_string =  recv_string_token[0] + "_"
        		+ session_table.get(recv_string_token[2] + "_" + recv_string_token[3]).toString();
		return return_string.getBytes();
	}
    
    /**TODO
	 * Server RPC to write received session data into server session table
	 * 
	 * @param pktData
	 * @param pktLen
	 * 
	 * @return recvStr receive data string
	 **/
	public byte[] serverSessionWrite(byte[] pktData, int pktLen) {
		// Received data format = callID + operation code + sessionID(2) + session data(3)
    	// output data format = callID
    	String recv_string = new String(pktData);
        String[] recv_string_token = recv_string.split("_");
        String sessionID =  recv_string_token[2] + "_" + recv_string_token[3];
        session_table.put(sessionID, new SessionData(Integer.valueOf(recv_string_token[4]), 
        		recv_string_token[5], Integer.valueOf(recv_string_token[6])));
		return recv_string_token[0].getBytes();
	}
	
	/**TODO
	 * Server RPC to return view for function RPCViewExchange
	 * 
	 * @param pktData
	 * @param pktLen
	 * 
	 * @return recvStr receive data string
	 **/
	public byte[] serverExchangeView(byte[] pktData, int pktLen) {
		// Received data format = callID + 3 + viewString
    	// output data format = callID + viewString
    	String recv_string = new String(pktData);
        String[] recv_string_token = recv_string.split("_");
        mergeViewStringToLocalView(recv_string_token[2]);

		return (recv_string_token[0] + getLocalViewString()).getBytes();
	}
    
    /**TODO
	 * RPC to read session data which is not in local session table
	 * 
	 * @param sessionID
	 * @param version_number session data version number
	 * @param primary_IP session data primary storage
	 * @param backup_IP session data backup storage
	 * 
	 * @return recvStr receive data string
     * @throws IOException 
	 **/
    public SessionData RPCSessionRead(String sessionID, int version_num, String primary_IP, String backup_IP) throws IOException {
    	DatagramSocket rpcSocket = new DatagramSocket();
    	byte[] outBuf = new byte[1024];
    	byte[] inBuf = new byte[1024];
    	
    	
    	int received_version_num = -1;
    	int received_call_ID = -1;
    	int local_call_ID = RPC_call_ID;
    	SessionData return_data = null;
    	
    	// Increment call ID for next RPC
    	RPC_call_ID++;
    	
    	// message = callID + operation code + sessionID
    	String message = String.valueOf(local_call_ID) + "_0_" + sessionID; 
    	outBuf = message.getBytes();
    	
    	// Send to primary server
    	DatagramPacket sendPkt = new DatagramPacket(outBuf, outBuf.length, InetAddress.getByName(primary_IP), UDP_port);
    	rpcSocket.send(sendPkt);
    	
    	// Send to backup server
    	sendPkt = new DatagramPacket(outBuf, outBuf.length, InetAddress.getByName(backup_IP), UDP_port);
    	rpcSocket.send(sendPkt);
    	
    	DatagramPacket recvPkt = new DatagramPacket(inBuf, inBuf.length);
    	try {
    		do {
    			recvPkt.setLength(inBuf.length);
    			rpcSocket.receive(recvPkt);
    			String received_string = new String(recvPkt.getData());
    			String[] received_string_token = received_string.split("_");
    			
    			
    			// Received data format = callID + sessionData(ver#_message_timestamp)
    			received_call_ID = Integer.parseInt( received_string_token[0] );
    			received_version_num = Integer.parseInt( received_string_token[1] );
    			return_data = new SessionData(received_version_num, 
    										received_string_token[2], 
    										Integer.parseInt( received_string_token[3]) );
    			
    		} while (received_call_ID != local_call_ID && version_num != received_version_num);
    	} catch (SocketTimeoutException stoe) {
    		// RPC timeout, return null string
    		System.out.println("RPC session read timeout");
    		recvPkt = null;
    	} catch(IOException ioe) {
    		// other error
    	}
    	
    	rpcSocket.close();
    	return return_data;
    }
    
    /**TODO
	 * RPC to read session data which is not in local session table
	 * 
	 * @param sessionID
	 * 
	 * @return recvStr receive data string
     * @throws IOException 
	 **/
    public boolean RPCSessionWrite(String sessionID, String ip, SessionData data) throws IOException {
    	DatagramSocket rpcSocket = new DatagramSocket();
    	byte[] outBuf = new byte[1024];
    	byte[] inBuf = new byte[1024];
    	String return_string = null;
    	int received_call_ID = -1;
    	int local_call_ID = RPC_call_ID;
    	
    	RPC_call_ID++;
		// message = callID + operation code + sessionID + session data
    	String message = String.valueOf(local_call_ID) + "_1_" + sessionID + "_" + data.toString();
    	outBuf = message.getBytes();
    	
    	DatagramPacket sendPkt = new DatagramPacket(outBuf, outBuf.length, InetAddress.getByName(ip), UDP_port);
    	rpcSocket.send(sendPkt);
    	
    	DatagramPacket recvPkt = new DatagramPacket(inBuf, inBuf.length);
    	try {
    		do {
    			recvPkt.setLength(inBuf.length);
    			rpcSocket.receive(recvPkt);
    			return_string = new String(recvPkt.getData());
    			received_call_ID = Integer.parseInt( (return_string.split("_"))[0] );
    		} while (received_call_ID != local_call_ID);
    	} catch (SocketTimeoutException stoe) {
    		// timeout
    		recvPkt = null;
    		return false;
    	} catch(IOException ioe) {
    		// other error
    	}
    	rpcSocket.close();
    	return true;
    }
    
    /**
	 * RPC to exchange view with target server or simpleDB
	 * 
	 * @param sessionID
	 * 
	 * @return recvStr receive data string
     * @throws IOException 
	 **/
    public String RPCViewExchange(String ip) throws IOException {
    	DatagramSocket rpcSocket = new DatagramSocket();
    	byte[] outBuf = new byte[1024];
    	byte[] inBuf = new byte[1024];
    	String return_string = null;
    	int received_call_ID = -1;
    	int local_call_ID = RPC_call_ID;
    	
    	RPC_call_ID++;
		// message = callID + operation code + local viewString
    	String message = String.valueOf(local_call_ID) + "_2_" + getLocalViewString(); 
    	outBuf = message.getBytes();
    	
    	DatagramPacket sendPkt = new DatagramPacket(outBuf, outBuf.length, InetAddress.getByName(ip), UDP_port);
    	rpcSocket.send(sendPkt);
    	
    	DatagramPacket recvPkt = new DatagramPacket(inBuf, inBuf.length);
    	try {
    		do {
    			// received = callID + viewString
    			recvPkt.setLength(inBuf.length);
    			rpcSocket.receive(recvPkt);
    			return_string = new String(recvPkt.getData());
    			received_call_ID = Integer.parseInt( (return_string.split("_"))[0] );
    		} while (received_call_ID != local_call_ID);
    	} catch (SocketTimeoutException stoe) {
    		// timeout
    		recvPkt = null;
    		return null;
    	} catch(IOException ioe) {
    		// other error
    	}
    	rpcSocket.close();
    	return (return_string.split("_"))[1];
    }
    
    /**
	 * RPC to exchange view with target server or simpleDB
	 * 
	 * @param data session data to write to remote server
	 * 
	 * @return backup_ip null, if no server can be written to
     * @throws IOException 
	 **/
    public String writeDataToBackupServer(SessionData data) throws IOException {
    	// Choose available backup server from group view
    	List<String> good_server_list = new ArrayList<String>();
    	for (Map.Entry<String, ServerStatus> entry : group_view.entrySet()) {
    		if (!entry.getKey().equals(local_IP) && entry.getValue().getStatus().equals("UP")) {
    			good_server_list.add(entry.getKey());
    		}
    	}
    	
    	boolean backup_written = false; // True, if data is wrote to other server by RPC
    	String backup_ip = null;
    	
    	while (backup_written) { // Keep trying if data is not written
    		if (good_server_list.isEmpty()) break; // No available server
    		Random random = new Random();
    		int rand = random.nextInt(good_server_list.size());
    		String randomBackupServerIP = good_server_list.get(rand);
    		good_server_list.remove(rand); // removed server IP that has been chosen
    	            	
    		// Write to backup server
    		String session_ID = String.valueOf(sess_num) + "_" + local_IP;
    		
    		if ( RPCSessionWrite(session_ID, randomBackupServerIP, data) ) {
    			backup_written = true;
    			backup_ip = randomBackupServerIP;
    			
    			// Update good server status
    			group_view.put(backup_ip, new ServerStatus("UP", System.currentTimeMillis()));
    			break;
    		};
    		
    		// Update failed server status
    		group_view.put(randomBackupServerIP, new ServerStatus("DOWN", System.currentTimeMillis()));
    	}
    	
    	return backup_ip;
    }
    
    public void mergeViewStringToLocalView(String viewString) {
    	String[] tokens = viewString.split("_");
    	int num_views = tokens.length / 3;
    	for (int i = 0; i < num_views; i++) {
    		String ip = tokens[i*3];
    		String status = tokens[i*3+1];
    		int timeStamp = Integer.valueOf(tokens[i*3+2]);
    		if (!group_view.containsKey(ip) || group_view.get(ip).getTimeStamp() < timeStamp) {
    			// Add non-exist or out of date view
    			group_view.put(ip, new ServerStatus(status, timeStamp));
    		}
    	}
    }
    
    public String getLocalViewString() {
    	StringBuilder sb = new StringBuilder();
    	for (Map.Entry<String, ServerStatus> entry : group_view.entrySet()) {
    		sb.append(entry.getKey() + "_");
    		sb.append(entry.getValue().getStatus() + "_");
    		sb.append(entry.getValue().getTimeStamp() + "_");
    	}
    	return sb.toString().substring(0, sb.length()-1);
    }
}
