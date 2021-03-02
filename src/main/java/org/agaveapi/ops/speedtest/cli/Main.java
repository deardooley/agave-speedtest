package org.agaveapi.ops.speedtest.cli;

import java.io.IOException;

import org.iplantc.service.common.persistence.JndiSetup;
import org.springframework.shell.Bootstrap;

/**
 * Driver class to run the data movement performance tests. 
 * 
 * @author dooley
 *
 */
public class Main {

	/**
	 * Main class that delegates to Spring Shell's Bootstrap class in order to simplify debugging inside an IDE
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
//		try {
//			JndiSetup.init();
//			
//		}
//		catch(Throwable t) {
//			try { JndiSetup.close(); } catch (Exception e) {}
//		}
		Bootstrap.main(args);
	}

}
