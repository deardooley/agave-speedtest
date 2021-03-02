package org.agaveapi.ops.speedtest.cli.commands;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliOption;

public abstract class SpeedtestCommand<T> implements CommandMarker {

	public final static String[] RESULT_TABLE_HEADER = new String[]{"File Size", "Upload time(ms)", "Upload Rate(kB/s)","Download time(ms)","Download Rate(kB/s)"};
	
	private String username = null;
	private String hostname = null;
	private int bufferSize = 0;
	private int port = 22;
	private String publicKey = null;
	private String privateKey = null;
	private String password = null;
	private boolean verbose = false;
	private boolean debug = false;
	private boolean inMemory = false;
	private boolean streaming = false;
	private T client = null;
	
	public SpeedtestCommand() {
		super();
	}

	/**
	 * @return the publicKey
	 */
	public synchronized String getPublicKey() {
		return publicKey;
	}

	/**
	 * @param publicKey the publicKey to set
	 */
	public synchronized void setPublicKey(String publicKey) throws IOException {
		
		if (StringUtils.isBlank(publicKey)) {
			this.publicKey = null;
		}
		else {
			File f = new File(publicKey);
			this.publicKey = FileUtils.readFileToString(f);
		}
	}

	/**
	 * @return the privateKey
	 */
	public synchronized String getPrivateKey() {
		return privateKey;
	}

	/**
	 * @param privateKey the privateKey to set
	 * @throws IOException 
	 */
	public synchronized void setPrivateKey(String privateKey) throws IOException {
		
		if (StringUtils.isBlank(privateKey)) {
			this.privateKey = null;
			return;
		} 
		else {
			File f = new File(privateKey);
			this.privateKey = FileUtils.readFileToString(f);
		}
	}

	/**
	 * @return the password
	 */
	public synchronized String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public synchronized void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the username
	 */
	public synchronized String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public synchronized void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the hostname
	 */
	public synchronized String getHostname() {
		return hostname;
	}

	/**
	 * @param hostname the hostname to set
	 */
	public synchronized void setHostname(String hostname) {
		this.hostname = hostname;
	}

	/**
	 * @return the port
	 */
	public synchronized int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public synchronized void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the verbose
	 */
	public synchronized boolean isVerbose() {
		return verbose;
	}

	/**
	 * @param verbose the verbose to set
	 */
	public synchronized void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * @return the debug
	 */
	public boolean isDebug() {
		return debug;
	}

	/**
	 * @param debug the debug to set
	 */
	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	/**
	 * @return the inMemory
	 */
	public boolean isInMemory() {
		return inMemory;
	}

	/**
	 * @param inMemory the inMemory to set
	 */
	public void setInMemory(boolean inMemory) {
		this.inMemory = inMemory;
	}

	/**
	 * @return the streaming
	 */
	public boolean isStreaming() {
		return streaming;
	}

	/**
	 * @param streaming the streaming to set
	 */
	public void setStreaming(boolean streaming) {
		this.streaming = streaming;
	}

	/**
	 * @param client the client to set
	 */
	public void setClient(T client) {
		this.client = client;
	}

	/**
	 * @return the bufferSize
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	/**
	 * @param bufferSize the bufferSize to set
	 */
	public void setBufferSize(String bufferSize) {
		
		if (NumberUtils.isNumber(bufferSize)) {
			this.bufferSize = NumberUtils.toInt(bufferSize, 32768);
		}
		else {
			this.bufferSize = (int)parseHumanReadableNumber(bufferSize);
		}
	}
	
	/**
	 * @param bufferSize the bufferSize to set
	 */
	public void setBufferSize(int bufferSize) {
		
		this.bufferSize = bufferSize;
	}

	public abstract boolean isSimpleAvailable();

	public abstract String benchmark(
//			@CliOption(key = { "host" }, mandatory = false, help = "Hostname of the remote sftp server", unspecifiedDefaultValue="docker.example.com") final String host, 
//			@CliOption(key = { "port" }, mandatory = false, help = "Port of the remote sftp server", unspecifiedDefaultValue="10022") final int port, 
//			@CliOption(key = { "user" }, mandatory = false, help = "Username to authenticate to the remote host", unspecifiedDefaultValue="testuser") final String user,
//			@CliOption(key = { "pass" }, mandatory = false, help = "Password to authenticate to the remote host", unspecifiedDefaultValue="testuser") final String pass, 
//			@CliOption(key = { "public" }, mandatory = false, help = "Public key used to authenticate to the remote host") final String pubkey, 
//			@CliOption(key = { "private" }, mandatory = false, help = "Private key used to authenticate to the remote host") final String privkey,
			@CliOption(key = { "system" }, mandatory = true, help = "System id on which to perform the test") final String systemId,
			@CliOption(key = { "file" }, mandatory = false, help = "File to use for the test") final String testFilename, 
			@CliOption(key = { "upload" }, mandatory = false, help = "Include upload test if present.", unspecifiedDefaultValue="true") final boolean includeUploadTest,
			@CliOption(key = { "download" }, mandatory = false, help = "Include download test if present.", unspecifiedDefaultValue="true") final boolean includeDownloadTest, 
			@CliOption(key = { "size" }, mandatory = false, help = "Human readable size of the test file.", unspecifiedDefaultValue="2MB") final String fileSize, 
			@CliOption(key = { "iter" }, mandatory = false, help = "Number of times to repeat the transfer(s).", unspecifiedDefaultValue="1") final int iterations, 
			@CliOption(key = { "streaming" }, mandatory = false, help = "Should transfers be done using the streaming api?", unspecifiedDefaultValue="false") final boolean streaming, 
			@CliOption(key = { "bufferSize" }, mandatory = false, help = "Size of the buffer to use during transfer.", unspecifiedDefaultValue="32768") final int bufferSize,
			@CliOption(key = { "verbose" }, mandatory = false, help = "Enabled verbose output?", unspecifiedDefaultValue="false") final boolean verboseOutput,
			@CliOption(key = { "debug" }, mandatory = false, help = "Enabled debug output?", unspecifiedDefaultValue="false") final boolean debugOutput,
			@CliOption(key = { "inMemory" }, mandatory = false, help = "Use memory only transfers (/dev/zero <=> /dev/null) to identify optimal performance.", unspecifiedDefaultValue="false") final boolean inMemory) throws Exception;

	/**
	 * Prints the given column in equal sized columns.
	 * 
	 * @param string
	 * @param string2
	 * @param string3
	 * @param string4
	 * @param string5
	 * @return
	 */
	protected String formatOutput(String[] row) {
		String out = "";
		for (String col: row) {
			out += org.apache.commons.lang.StringUtils.rightPad(col, 20);
		}
		return out;
	}

	/**
	 * Creates test data us use in the io tests.
	 * @param testFilename
	 * @param fileSize the human readable size of the file to generate
	 * @return the generated test file to use
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public File maybeGenerateTestData(final String testFilename, final String fileSize)
	throws IOException, FileNotFoundException {
		
		// if in memory, skip creating test data.
		if (isInMemory()) return new File("/dev/zero");
		
		/**
		 * Generate a temporary file for uploading/downloading if not 
		 * provided by the user
		 */
		File testFile = null;
		if (StringUtils.isEmpty(testFilename)) {
			testFile = new File(System.getProperty("user.home"), "sftp-file");
		}
		else {
			testFile = new File(testFilename);
		}
		
	
		/**
		 * Generate test data if the file does not exist or the 
		 * file is not of the size specified by the user.
		 */
		long bytes = parseHumanReadableNumber(fileSize);
		if (testFile.exists()) {
			if (testFile.length() != bytes) {
				throw new IOException("Test file " + testFile.getAbsolutePath() + " does not match the given test file size.");
			}
		}
		// create the file data
		else { 
			java.util.Random rnd = new java.util.Random();
	
			FileOutputStream out = new FileOutputStream(testFile);
			byte[] buf = new byte[4096];
			for (int i = 0; i < Math.ceil(bytes/4096); i++) {
				rnd.nextBytes(buf);
				out.write(buf);
			}
			out.close();
		}
		return testFile;
	}

	/**
	 * Generates a tab delimited line specifying the file size, upload time, upload rate, download time, download rate
	 * given the timestamps and file length. 
	 * @param uend
	 * @param ustart
	 * @param dend
	 * @param dstart
	 * @param length
	 * @return
	 */
	protected String calculateTime(long uend, long ustart, long dend, long dstart, long length) {
		long ue = uend - ustart;
		float ukbs = 0;
		if (ue >= 1) {
			ukbs = ((float) length / 1024) / ((float) ue / 1000);
		}
		
		long de = dend - dstart;
		float dkbs = 0;
		if (de >= 1) {
			dkbs = ((float) length / 1024) / ((float) de / 1000);
		}
		
		return formatOutput(new String[] {
				String.valueOf(length),
				String.valueOf(ue), 
				String.valueOf(ukbs),
				String.valueOf(de), 
				String.valueOf(dkbs)});
	}

	/**
	 * Converts a human readable number to a byte integer value.
	 * 
	 * @param value
	 * @return
	 * @throws NumberFormatException
	 */
	public long parseHumanReadableNumber(String value)
	throws NumberFormatException {
		if (value == null) {
			throw new NumberFormatException("Cannot parse a null value.");
		} 
		
		String formattedValue = value.toUpperCase()
				.replaceAll(",", "")
				.replaceAll(" ", "");
		
		long returnValue = -1;
	    Pattern patt = Pattern.compile("([\\d.-]+)([XPTGMK]B)", Pattern.CASE_INSENSITIVE);
	    Matcher matcher = patt.matcher(formattedValue);
	    Map<String, Integer> powerMap = new HashMap<String, Integer>();
	    powerMap.put("XB", 6);
	    powerMap.put("PB", 5);
	    powerMap.put("TB", 4);
	    powerMap.put("GB", 3);
	    powerMap.put("MB", 2);
	    powerMap.put("KB", 1);
	    powerMap.put("B", 0);
	    if (matcher.find()) {
			String number = matcher.group(1);
			int pow = powerMap.get(matcher.group(2).toUpperCase());
			BigDecimal bytes = new BigDecimal(number);
			bytes = bytes.multiply(BigDecimal.valueOf(1024).pow(pow));
			returnValue = bytes.longValue();
	    } else {
	    	throw new NumberFormatException("Invalid number format.");
	    }
	    return returnValue;
	}

	protected abstract void authenticate() throws AuthenticationException;

	protected abstract T getClient() throws RemoteDataException;
	
}