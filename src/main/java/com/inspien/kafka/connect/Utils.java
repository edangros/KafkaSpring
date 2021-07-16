package com.inspien.kafka.connect;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {
    
    private Utils(){}

    /**
     * Check if the port is in use
     * @param port target port number
     * @return if port is accessable, return {@code true}. Return {@code false} if the port is in use or locked.
     */
    public static boolean validatePortNumber(int port){
        try (ServerSocket serverSocket = new ServerSocket()) {
            // setReuseAddress(false) is required only on OSX, 
            // otherwise the code will not work correctly on that platform          
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
	/**
	 * get local ip, which will be used as partition
	 * @return local ip address, or loopback address 127.0.0.1 if can't gain any address
	 */
	public static String getLocalIp(){
		String ip;
		try{
			ip = InetAddress.getLocalHost().getHostAddress();
		}
		catch (UnknownHostException e){
			log.error("cannot find localhost maybe due to security setting. localhost 127.0.0.1 is used",e);
			ip = "127.0.0.1";
		}
		return ip;
	}
}
