***************************************************************************
* ATTENTION!
* 1: Use Eclipse to run this test.
* 2: In Eclipse, File-Import-General-Existing Projects into Workspace-Next
*    Select root directory: Browse the "GossipProtocolTest" path.
* 3: In left part of the Eclipse window, right click "GossipProtocolTest" and select
*    Properties-Libraries-Add Library...-AWS SDK for Java-Next-AWS SDK for
*    Java 1.9.25 (I think other version would also works.)
*    
***************************************************************************

***************************************************************************
Xiaohan Liu @2015.03.25
Use Eclipse with AWS SDK for java to debug GossipProtocolTest.java. Here is
the log. Firstly set mySvrID = 192.168.100.200 as my IP address.

===========================================
Start testing gossip protocol!
===========================================
2015-3-25 6:19:19 com.amazonaws.http.AmazonHttpClient <clinit>
警告: Detected a possible problem with the current JVM version (1.6.0_65).  If you experience XML parsing problems using the SDK, try upgrading to a more recent JVM update.
===========================================
Getting Started with Amazon SimpleDB
===========================================

Checking if the domain Views exists in your account:

Sorry, the domain Views does not exist in simpleDB.

Don't worry!, I will create it and add the local server status to it.

Creating domain called Views.

Putting you into Views domain.

gossip with simpleDB.
Use the Gossip Protocol to update local and simpleDB.

Selecting: select * from `Views`

Use myView for 192.168.100.200
Update to simpleDB.

Updating 192.168.100.200 status and time in the simpleDB.

gossip with yourself.
gossip with simpleDB.
Use the Gossip Protocol to update local and simpleDB.

Selecting: select * from `Views`

Use myView for 192.168.100.200
Update to simpleDB.

Updating 192.168.100.200 status and time in the simpleDB.

gossip with simpleDB.
Use the Gossip Protocol to update local and simpleDB.

Selecting: select * from `Views`

Use myView for 192.168.100.200
Update to simpleDB.

Updating 192.168.100.200 status and time in the simpleDB.

******* Change mySvrID to 192.168.100.201 and then run again. *******
===========================================
Start testing gossip protocol!
===========================================
2015-3-25 6:30:35 com.amazonaws.http.AmazonHttpClient <clinit>
警告: Detected a possible problem with the current JVM version (1.6.0_65).  If you experience XML parsing problems using the SDK, try upgrading to a more recent JVM update.
===========================================
Getting Started with Amazon SimpleDB
===========================================

Checking if the domain Views exists in your account:

Cong! The domain Views exists in simpleDB.

Use the Gossip Protocol to update local and simpleDB.

Selecting: select * from `Views`

Hello, fresh server! I would add you to simpleDB.

Putting you into Views domain.

***Current Time: Wed Mar 25 06:30:40 HKT 2015
gossip with simpleDB.
Use the Gossip Protocol to update local and simpleDB.

Selecting: select * from `Views`

Use myView for 192.168.100.201
Use myView for 192.168.100.201
Update to simpleDB.

Updating 192.168.100.201 status and time in the simpleDB.

Updating 192.168.100.200 status and time in the simpleDB.

***Current Time: Wed Mar 25 06:31:59 HKT 2015
gossip with another server.
***Current Time: Wed Mar 25 06:33:22 HKT 2015
gossip with another server.
***Current Time: Wed Mar 25 06:34:26 HKT 2015
gossip with yourself.

******* Change GOSSIP_SECS to 10*1000 to speed up. *******

