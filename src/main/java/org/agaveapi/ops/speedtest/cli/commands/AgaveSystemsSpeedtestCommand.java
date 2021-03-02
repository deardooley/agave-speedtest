package org.agaveapi.ops.speedtest.cli.commands;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.AuthConfigException;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.model.TransferTask;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class AgaveSystemsSpeedtestCommand<T extends RemoteDataClient> extends SpeedtestCommand implements CommandMarker {
	
	public final static String[] RESULT_TABLE_HEADER = new String[]{"File Size", "Upload time(ms)", "Upload Rate","Download time(ms)","Download Rate"};
	
	private SystemDao systemDao = new SystemDao();
	private RemoteSystem system = null;
	private String systemId = null;
	private ObjectMapper mapper = new ObjectMapper();
	private String username = null;
	
	@CliAvailabilityIndicator({"agave"})
	public boolean isSimpleAvailable() {
		//always available
		return true;
	}
	
	@CliCommand(value = "agave", help = "Test upload and download speed to registered systems through the internal service adaptors")
	public String benchmark(
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
			@CliOption(key = { "inMemory" }, mandatory = false, help = "Use memory only transfers (/dev/zero <=> /dev/null) to identify optimal performance.", unspecifiedDefaultValue="false") final boolean inMemory)
	throws Exception {	
		
		/**
		 *  set the systemId from the cli options
		 */
		setSystemId(systemId);
		
		/**
		 *  set the bufferSize from the cli options
		 */
		setBufferSize(bufferSize);
		
		/**
		 *  set the verbose from the cli options
		 */
		setVerbose(verboseOutput);
		
		/**
		 *  set the inMemory from the cli options
		 */
		setStreaming(streaming);
		
		/**
		 *  set the inMemory from the cli options
		 */
		setInMemory(inMemory);
		
		/**
		 *  set the debug from the cli options
		 */
		setDebug(debugOutput);
		
		/**
		 *  load the auth context from disk
		 */
		authenticate();
		
		/**
		 *  Create test data to use
		 */
		File testFile = maybeGenerateTestData(testFilename, fileSize);
		
		long t1=0,t2=0,t3=0,t4=0;
		long length = testFile.length();
		
		/**
		 *  Run the tests
		 */
		StringBuilder sb = new StringBuilder();
		sb.append(formatOutput(RESULT_TABLE_HEADER) + "\n");
		String remoteUploadDirectory = username + "/speedtest-" + UUID.randomUUID().toString();
		
		try {
			
			if (isVerbose()) System.out.println("Authenticating to " + getSystem().getStorageConfig().getDefaultAuthConfig().getUsername() + 
					"@" + getSystem().getStorageConfig().getHost() + ":" +  getSystem().getStorageConfig().getPort() + 
					getClient().resolvePath(remoteUploadDirectory) );
			
			if (isVerbose()) System.out.println("Test file size: " + testFile.length());
			
			getClient().authenticate();
			
			getClient().mkdirs(remoteUploadDirectory);
			
			for (int z=0; z<iterations; z++) {
				if (includeUploadTest) {
					if (isVerbose()) System.out.println("Starting upload test...");
					if (isVerbose()) System.out.println("\tTransfer: file:///" + testFile.getAbsoluteFile() + " => agave://" + getSystem().getSystemId() + "/" + testFile.getName());
					t1 = System.currentTimeMillis();
					
					if (isInMemory()) {
						
						InputStream in = null;
						OutputStream out = null;
						try {
							out = getClient().getOutputStream("/dev/null", true, false);
							in = new FileInputStream("/dev/zero");
							byte[] buf = new byte[bufferSize];
						    int bytesRead = 0;
						    for (int i=0;i<Math.ceil(length / bufferSize); i++) {
						    	bytesRead = in.read(buf);
						    	out.write(buf, 0, bytesRead);
						    }
						    out.flush();
						}
						catch (Exception e) {
							try { in.close();} catch (Exception e1){}
							try { out.close();} catch (Exception e1){}
						}
					}
					else if (isStreaming()) {
						InputStream in = null;
						OutputStream out = null;
						try {
							out = getClient().getOutputStream(remoteUploadDirectory + "/" + testFile.getName(), true, false);
							in = new FileInputStream(testFile);
							byte[] buf = new byte[bufferSize];
						    int bytesRead = in.read(buf);
						    while (bytesRead != -1) {
								out.write(buf, 0, bytesRead);
						      bytesRead = in.read(buf);
						    }
						    out.flush();
						}
						catch (Exception e) {
							try { in.close();} catch (Exception e1){}
							try { out.close();} catch (Exception e1){}
						}
					}
					else {
						getClient().put(testFile.getAbsolutePath(), remoteUploadDirectory);
	//					client.put(testFile.getAbsolutePath(), remoteUploadDirectory, new RemoteTransferListener(null) {
	//						protected synchronized void setTransferTask(TransferTask transferTask){}
	//					});
					}
					
					t2 = System.currentTimeMillis();
					long duration = ((t2 - t1)/1000);
					if (isVerbose()) System.out.println("\tPerf: " + ((t2 - t1)) + "ms " + TransferTask.formatMaxMemory(testFile.length() / duration) + "/s ");
					
				}
				
				if (includeDownloadTest) {
					if (isVerbose()) System.out.println("Starting download test...");
					if (isVerbose()) System.out.println("\tTransfer: agave://" + getSystem().getSystemId() + "/" + testFile.getName() + " => file:///" + testFile.getAbsoluteFile());
					t3 = System.currentTimeMillis();
					
					if (isInMemory()) {
						
						InputStream in = null;
						OutputStream out = null;
						BufferedOutputStream bout = null;
						try {
							in = getClient().getInputStream("/dev/zero", false);
							out = new FileOutputStream("/dev/null");
							bout = new BufferedOutputStream(out, 2 * 32 * getBufferSize());
							byte[] buf = new byte[getBufferSize()];
						    int bytesRead = 0;
						    for (int i=0;i<Math.ceil(length / getBufferSize()); i++) {
						    	bytesRead = in.read(buf);
						    	bout.write(buf, 0, bytesRead);
						    }
						    out.flush();
						}
						catch (Exception e) {
							try { in.close();} catch (Exception e1){}
							try { out.close();} catch (Exception e1){}
							try { bout.close();} catch (Exception e1){}
						}
					}
					else if (isStreaming()) {
						InputStream in = null;
						OutputStream out = null;
						BufferedOutputStream bout = null;
						try {
							in = getClient().getInputStream(remoteUploadDirectory + "/" + testFile.getName(), false);
							out = new FileOutputStream(testFile);
							bout = new BufferedOutputStream(out, 2 * 32 * getBufferSize());
							byte[] buf = new byte[getBufferSize()];
						    int bytesRead = in.read(buf);
						    
						    while (bytesRead != -1) {
						    	bout.write(buf, 0, bytesRead);
						    	bytesRead = in.read(buf);
						    }
						    bout.flush();								
						}
						catch (Exception e) {
							try { in.close();} catch (Exception e1){}
							try { out.close();} catch (Exception e1){}
							try { bout.close();} catch (Exception e1){}
						}
					}
					else {
						getClient().get(remoteUploadDirectory + "/" + testFile.getName(), testFile.getAbsolutePath());
					}
					
//					client.get(remoteUploadDirectory + "/" + testFile.getName(), testFile.getAbsolutePath(), new RemoteTransferListener(null) {
//						protected synchronized void setTransferTask(TransferTask transferTask){}
//					});
					
					t4 = System.currentTimeMillis();
					long duration = ((t4 - t3)/1000);
					if (isVerbose()) System.out.println("\tPerf: " + ((t4 - t3)) + "ms " + TransferTask.formatMaxMemory(testFile.length() / duration) + "/s");
					
//					client.delete(remoteUploadDirectory + "/" + testFile.getName());
				}
			}
			
			sb.append(calculateTime(t2, t1, t4, t3, length) + "\n");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteDataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {getClient().delete(remoteUploadDirectory); } catch (Exception e) {}
			try {getClient().disconnect();} catch (Exception e) {}
			
			if (StringUtils.isEmpty(testFilename) && !isInMemory()) {
				if (isVerbose()) System.out.println("Cleaning up local directory");
				FileUtils.deleteQuietly(testFile);
			}
		}
		
		return sb.toString();
	}
	
//	private String getSystemSalt() {
//		return getSystem().getSystemId() + 
//				getSystem().getStorageConfig().getHost() + 
//    			getSystem().getStorageConfig().getDefaultAuthConfig().getUsername();
//	}

	/**
	 * Reads in the auth context from a agave cache file on 
	 * disk and initializes the user's context.
	 * @param tenantId
	 * @param testUsername
	 * @param systemId
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws JsonProcessingException
	 * @throws AuthConfigException
	 * @throws TenantException
	 * @throws Exception
	 * @throws SystemUnknownException
	 * @throws SystemException
	 */
	@Override
	protected void authenticate() throws AuthenticationException {
		
		try {
	//		Logger logger = Logger.getLogger("org.iplantc.service.transfer.RemoteTransferListener");
	//		logger.setLevel(Level.DEBUG);
	//		
			String cacheDirectory = System.getenv("AGAVE_CACHE_DIR");
			
			if (StringUtils.isEmpty(cacheDirectory)) {
				cacheDirectory = System.getProperty("agave.cache.dir");
				
				if (StringUtils.isEmpty(cacheDirectory)) {
					cacheDirectory = System.getProperty("user.home") + "/.agave";
				}
			}
			
			File agaveCacheFile = new File(cacheDirectory, "current");
			if (!agaveCacheFile.exists()) {
				throw new FileNotFoundException("No agave config file found at " + agaveCacheFile.getAbsolutePath());
			}
			
			JsonNode cache = mapper.readTree(agaveCacheFile);
			
			// config current username
			username = cache.get("username").asText();;
			if (StringUtils.isEmpty(username)) {
				throw new AuthenticationException("No username found in agave config file at " + agaveCacheFile.getAbsolutePath());
			}
			
			// config current tenant
			try {
				String tenantCode = cache.get("tenantid").asText();
				if (StringUtils.isEmpty(tenantCode)) {
					throw new TenantException("No tenant id found in agave config file at " + agaveCacheFile.getAbsolutePath());
				}
				else if (new TenantDao().exists(tenantCode)) {
					TenancyHelper.setCurrentTenantId(tenantCode);
					TenancyHelper.setCurrentEndUser(username);
				}
				else {
					throw new TenantException("No tenant found matching " + tenantCode);
				}
			}
			catch (TenantException e) {
				throw e;
			}
			catch (Exception e) {
				throw new TenantException("Unable to verify tenant id.", e);
			}
			
			// get a handle on the specified system or the default system for the tenant.
			if (!StringUtils.isEmpty(getSystemId())) {
				setSystem(systemDao.findBySystemId(getSystemId()));
				if (getSystem() == null) {
					throw new SystemUnknownException("No system found for id " + getSystemId());
				}
			}
			else {
				setSystem(new SystemManager().getUserDefaultStorageSystem(username, RemoteSystemType.STORAGE));
				if (getSystem() == null) {
					throw new SystemUnknownException("No default system found for user. Please specify a system");
				}
			}
		} 
		catch (AuthenticationException e) {
			throw e;
		}
		catch (SystemUnknownException | TenantException e) {
			throw new AuthenticationException(e);
		}
		catch (Exception e) {
			throw new AuthenticationException("Unable to authenticate to " + getSystemId(), e);
		}
	}

	/**
	 * @return the systemId
	 */
	public String getSystemId() {
		return systemId;
	}

	/**
	 * @param systemId the systemId to set
	 */
	public void setSystemId(String systemId) {
		this.systemId = systemId;
	}

	/**
	 * @return the system
	 */
	public RemoteSystem getSystem() {
		return system;
	}

	/**
	 * @param system the system to set
	 */
	public void setSystem(RemoteSystem system) {
		this.system = system;
	}

	@Override
	protected RemoteDataClient getClient() throws RemoteDataException {
		try {
			if (getSystem() != null) {
				return getSystem().getRemoteDataClient();
			}
			else {
				throw new RemoteDataException("No system found for " + getSystemId());
			}
		} 
		catch (RemoteDataException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RemoteDataException(e);
		}
	}
}
