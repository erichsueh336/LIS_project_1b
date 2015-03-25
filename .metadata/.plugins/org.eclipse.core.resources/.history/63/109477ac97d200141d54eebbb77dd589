/*
 * Copyright 2010 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;

/**
 * This sample demonstrates how to make basic requests to Amazon SimpleDB using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon SimpleDB. For more information on
 * Amazon SimpleDB, see http://aws.amazon.com/simpledb.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */

/**
 * The code above is from SimpleDBSample.java as the default setting.
 */

/**
 * GossipProtocol test.
 */
public class GossipProtocolTest {

	public static void main(String[] args) throws Exception {
		System.out.println("===========================================");
		System.out.println("Start testing gossip protocol!");
		System.out.println("===========================================");
		GossipProtocol gossipProtocol = new GossipProtocol();
		gossipProtocol.run();
	}
	/**
	 * Function Description:
	 * Use the Gossip Protocol to exchange Views among Servers and SimpleDB.
	 */
	//public class GossipProtocol extends Thread {
	public static class GossipProtocol {
		public class View {
			public String SvrID;  // should be IP address.
			public String status; // should be either "up" or "down".
			public String time;   // should be return by System.currentTimeMillis().
			public View(String SvrID, String status, String time) {
				this.SvrID = SvrID;
				this.status = status;
				this.time = time;
			}
		}
		private String mySvrID = "192.168.100.201"; // TODO: How to get my IP Address?
		private static final String strStatus = "status";
		//private static final String strTime = "time";
		View local = new View(mySvrID, "up", Long.toString(System.currentTimeMillis()));
		private ArrayList<View> myView = new ArrayList<View>();

		private static final int GOSSIP_SECS = 10*1000; // gossip period, 10 seconds.
		Random generator = new Random(); // a variable period for avoiding convoy.
		Random iServer = new Random(); // gossip object: another server or simpleDB.
		String myDomainName = "Views";

		/**
		 * @throws IOException 
		 */
		public void run() throws IOException {
			/**
			 * 1: Set the parameters for simpleDB.
			 */
			final AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
						GossipProtocol.class.getResourceAsStream("AwsCredentials.properties")));

			System.out.println("===========================================");
			System.out.println("Getting Started with Amazon SimpleDB");
			System.out.println("===========================================\n");

			/**
			 * PS: terms in simpleDB, all of them are strings.
			 *     domain : table name
			 *     item : row name
			 *     attribute : column name
			 *     value : element name. 
			 */

			/**
			 * 2: List domains. Check if the domain "Views" exists in simpleDB.
			 *    Add myself to my view.
			 *    If "Views" does not exist
			 *    	Create this domain and add mySvrID to it.
			 *    else
			 *    	download simpleDB data to my view.
			 *    	If mySvrID dose not exist, add to it.
			 *    	else, update mySvrID in this domain.
			 */
			int isExistViews = 0; // domain
			int isExistMySvrID = 0; // item
			try {
				myView.add(local);
				System.out.println("Checking if the domain " + myDomainName + " exists in your account:\n");
				for (String domainName : sdb.listDomains().getDomainNames()) {
					if (myDomainName.equals(domainName)) {
						isExistViews = 1;
						break;
					}
				}
				if (isExistViews == 0) {
					System.out.println("Sorry, the domain " + myDomainName + " does not exist in simpleDB.\n");
					System.out.println("Don't worry!, I will create it and add the local server status to it.\n");
					// Create a domain
					System.out.println("Creating domain called " + myDomainName + ".\n");
					sdb.createDomain(new CreateDomainRequest(myDomainName));
					addOneServer(local);
				} else {
					System.out.println("Cong! The domain " + myDomainName + " exists in simpleDB.\n");
					System.out.println("Use the Gossip Protocol to update local and simpleDB.\n");
					// Select data from a domain
					// Notice the use of backticks around the domain name in our select expression.
					String selectExpression = "select * from `" + myDomainName + "`";
					System.out.println("Selecting: " + selectExpression + "\n");
					SelectRequest selectRequest = new SelectRequest(selectExpression);
					// download simpleDB data and add to my view.
					View t = new View("", "", "");
					for (Item item : sdb.select(selectRequest).getItems()) {
						t.SvrID = item.getName();
						if (mySvrID.equals(t.SvrID)) {
							isExistMySvrID = 1;
						} else {
							for (Attribute attribute : item.getAttributes()) {
								if (strStatus.equals(attribute.getName()))
									t.status = attribute.getValue();
								else
									t.time = attribute.getValue();
							}
							myView.add(t);
						}
					}
					// update simpleDB.
					if (isExistMySvrID == 0) {
						System.out.println("Hello, fresh server! I would add you to simpleDB.\n");
						addOneServer(local);
					} else {
						System.out.println("How time flies, my old friend. I will update your status in simpleDB.\n");
						updateOneServer(local);
					}
				}
			} catch (AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which means your request made it "
						+ "to Amazon SimpleDB, but was rejected with an error response for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
			} catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which means the client encountered "
						+ "a serious internal problem while trying to communicate with SimpleDB, "
						+ "such as not being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			}

			/**
			 * 3: Use the Gossip Protocol to exchange Views among 
			 *    Servers and SimpleDB with a mean period of GOSSIP_SECS.
			 */
			while (true) {
				try {
					System.out.println("***Current Time: " + new Date(System.currentTimeMillis()));
					int i = iServer.nextInt( 1 + myView.size() );
					if (i == myView.size()) {
						System.out.println("gossip with simpleDB.");
						gossipWithSimpleDB();
					} else {
						View t = myView.get(i);
						if (mySvrID.equals(t.SvrID)) {
							System.out.println("gossip with yourself.");
						} else {
							System.out.println("gossip with another server.");
							//TODO: hisView = excahngeViews(myView) using RPC.
						}
					}
					Thread.sleep( (GOSSIP_SECS/2) + generator.nextInt( GOSSIP_SECS ) );
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		/**
		 * Add one server to simpleDB.
		 *
		 * @return 0 is successful.
		 */
		private int addOneServer(View x) {
			AmazonSimpleDB sdb;
			try {
				sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
							GossipProtocol.class.getResourceAsStream("AwsCredentials.properties")));
				List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();

				sampleData.add(new ReplaceableItem().withName(x.SvrID).withAttributes(
							new ReplaceableAttribute().withName("status").withValue(x.status),
							new ReplaceableAttribute().withName("time").withValue(x.time)));

				// Put data into a domain
				System.out.println("Putting you into " + myDomainName + " domain.\n");
				sdb.batchPutAttributes(new BatchPutAttributesRequest(myDomainName, sampleData));				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		}

		/**
		 * Update one server to simpleDB.
		 *
		 * @return 0 is successful.
		 */
		private int updateOneServer(View x) {
			AmazonSimpleDB sdb;
			try {
				sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
							GossipProtocol.class.getResourceAsStream("AwsCredentials.properties")));			
				// Replace an attribute
				System.out.println("Updating " + x.SvrID + " status and time in the simpleDB.\n");

				ReplaceableAttribute status = new ReplaceableAttribute()
					.withName("status")
					.withValue(x.status)
					.withReplace(true);
				sdb.putAttributes(new PutAttributesRequest()
						.withDomainName(myDomainName)
						.withItemName(x.SvrID)
						.withAttributes(status));

				ReplaceableAttribute time = new ReplaceableAttribute()
					.withName("time")
					.withValue(x.time)
					.withReplace(true);
				sdb.putAttributes(new PutAttributesRequest()
						.withDomainName(myDomainName)
						.withItemName(x.SvrID)
						.withAttributes(time));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			return 0;
		}

		/**
		 * Gossip with simpleDB: firstly download, update and merge views.
		 *
		 * @return 0 is successful.
		 */
		private int gossipWithSimpleDB() {
			AmazonSimpleDB sdb;
			try {
				sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
							GossipProtocol.class.getResourceAsStream("AwsCredentials.properties")));
				ArrayList<View> hisView = new ArrayList<View>();
				System.out.println("Use the Gossip Protocol to update local and simpleDB.\n");
				// Select data from a domain
				// Notice the use of backticks around the domain name in our select expression.
				String selectExpression = "select * from `" + myDomainName + "`";
				System.out.println("Selecting: " + selectExpression + "\n");
				SelectRequest selectRequest = new SelectRequest(selectExpression);
				try {
					// 1: download simpleDB data to store to hisView.
					View t = new View("", "", "");
					for (Item item : sdb.select(selectRequest).getItems()) {
						t.SvrID = item.getName();
						for (Attribute attribute : item.getAttributes()) {
							if (strStatus.equals(attribute.getName()))
								t.status = attribute.getValue();
							else
								t.time = attribute.getValue();
						}
						hisView.add(t);
					}
					// 2: update myView.
					for (int i=0; i<myView.size(); i++) {
						t = myView.get(i);
						if (mySvrID.equals(t.SvrID)) {
							t.status = "up";
							t.time = Long.toString(System.currentTimeMillis());
							myView.set(i, t);
							break;
						}
					}
					// 3: merge myView and hisView to myView.
					View my, his;
					int i, j;
					int sizeMyView = myView.size();
					int sizeHisView = hisView.size();
					for (j=0; j<sizeHisView; j++) {
						his = hisView.get(j);
						for (i=0; i<sizeMyView; i++) {
							my = myView.get(i);
							if ((my.SvrID).equals(his.SvrID)) {
								if ((my.time).compareTo(his.time) > 0) {
									System.out.println("Use myView for " + my.SvrID);
								} else {
									System.out.println("Use hisView for " + his.SvrID);
									my.status = his.status;
									my.time = his.time;
									myView.set(j, my);
								}
								break;
							}
						}
						if (i == sizeMyView) {
							// his.SvrID is new to myView.
							myView.add(his);
						}
					}

					// 4: update simpleDB.
					System.out.println("Update to simpleDB.\n");
					int isExist;
					for (i=0; i<myView.size(); i++) {
						t = myView.get(i);
						isExist = 0;
						for (Item item : sdb.select(selectRequest).getItems()) {
							if ( (t.SvrID).equals(item.getName()) ) {
								updateOneServer(t);
								isExist = 1;
								break;
							}
						}
						if (isExist == 0)
							addOneServer(t);
					}
				} catch (AmazonServiceException ase) {
					System.out.println("Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon SimpleDB, but was rejected with an error response for some reason.");
					System.out.println("Error Message:    " + ase.getMessage());
					System.out.println("HTTP Status Code: " + ase.getStatusCode());
					System.out.println("AWS Error Code:   " + ase.getErrorCode());
					System.out.println("Error Type:       " + ase.getErrorType());
					System.out.println("Request ID:       " + ase.getRequestId());
				} catch (AmazonClientException ace) {
					System.out.println("Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with SimpleDB, "
							+ "such as not being able to access the network.");
					System.out.println("Error Message: " + ace.getMessage());
				}
			}  catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return 0;
		}

		// The end of GossipProtocol class. Do not add any code below this line.
	}    
	}
