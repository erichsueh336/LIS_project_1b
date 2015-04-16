package com.test.udp.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UDPServer implements Runnable{
	protected DatagramSocket serverSocket = null;
	protected boolean listen = true;
	
	public UDPServer(int port) throws IOException {
        super();
        serverSocket = new DatagramSocket(port);
    }
	
	@Override
	public void run() {
		byte[] receiveData = new byte[1024];
		byte[] sendData = new byte[1024];
		DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
		while(listen) {
	        try {
	        	System.out.println("yolo");
		        serverSocket.receive(receivePacket);
		        String sentence = new String( receivePacket.getData());
		        System.out.println("RECEIVED: " + sentence);
		        InetAddress IPAddress = receivePacket.getAddress();
		        int port = receivePacket.getPort();
		        String capitalizedSentence = sentence.toUpperCase();
		        sendData = capitalizedSentence.getBytes();
		        DatagramPacket sendPacket =
		        new DatagramPacket(sendData, sendData.length, IPAddress, port);
		        serverSocket.send(sendPacket);
	        } catch (Exception e) {
	        	e.printStackTrace();
                listen = false;
	        }
        }
		serverSocket.close();
	}
}