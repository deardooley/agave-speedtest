package org.agaveapi.ops.speedtest.cli.commands;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.AuthConfigException;
import org.iplantc.service.systems.exceptions.EncryptionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.model.TransferTask;
import org.mockito.internal.util.io.IOUtil;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.iplantc.service.transfer.RemoteDataClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;

@Component
public class NativeSystemsSpeedtestCommand<RemoteDataClient> extends SpeedtestCommand implements CommandMarker {
	
	public final static String[] RESULT_TABLE_HEADER = new String[]{"File Size", "Upload time(ms)", "Upload Rate","Download time(ms)","Download Rate"};
	
	private SystemDao systemDao = new SystemDao();
	private RemoteSystem system = null;
	private String systemId = null;
	private ObjectMapper mapper = new ObjectMapper();
	private String username = null;
	
	private boolean verbose = false;
	
	@CliAvailabilityIndicator({"native"})
	public boolean isSimpleAvailable() {
		//always available
		return true;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@CliCommand(value = "native", help = "Test upload and download speed to registered systems through the native system utilities")
	public String nativeSpeedtest(
		@CliOption(key = { "system" }, mandatory = false, help = "System id on which to perform the test", unspecifiedDefaultValue="data.agaveapi.co") final String systemId,
		@CliOption(key = { "file" }, mandatory = false, help = "File to use for the test") final String testFilename,
		@CliOption(key = { "upload" }, mandatory = false, help = "Include upload test if present.", unspecifiedDefaultValue="true") final String includeUploadTest,
		@CliOption(key = { "download" }, mandatory = false, help = "Include download test if present.", unspecifiedDefaultValue="true") final String includeDownloadTest,
		@CliOption(key = { "size" }, mandatory = false, help = "Number of 4k blocks to transfer.", unspecifiedDefaultValue="2MB") final String fileSize,
		@CliOption(key = { "iter" }, mandatory = false, help = "Number of times to repeat the transfer(s).", unspecifiedDefaultValue="1") final int iterations,
		@CliOption(key = { "verbose" }, mandatory = false, help = "Verbose output.", unspecifiedDefaultValue="true") final boolean verbose) 
	throws Exception {	
		
		this.verbose = verbose;
		
		setSystemId(systemId);
		
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
		RemoteDataClient client = null; 
		StringBuilder sb = new StringBuilder();
		sb.append(formatOutput(RESULT_TABLE_HEADER) + "\n");
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ByteArrayOutputStream inputStream = new ByteArrayOutputStream();
		
		
		String uuid = UUID.randomUUID().toString();
		
		String remoteUploadDirectory = username + "/speedtest-" + uuid;
		String remoteUploadDirectoryAbsolutePath = null;
		
		File tmpdir = Files.createTempDir();
		File batchFileUpload = new File(tmpdir, "up-" + uuid + ".batch");
		File batchFileDownload = new File(tmpdir, "down-" + uuid + ".batch");
		File askpassFile = createAskPassFile(tmpdir, system);
		File pubKey = new File(tmpdir, "key-cert.pub");
		File privateKey = new File(tmpdir, "key");
		String salt = getSystemSalt();
		
		if (system.getStorageConfig().getDefaultAuthConfig().getType() == AuthConfigType.SSHKEYS) {
			IOUtil.writeText(system.getStorageConfig().getDefaultAuthConfig().getClearTextPublicKey(salt), pubKey);
			IOUtil.writeText(system.getStorageConfig().getDefaultAuthConfig().getClearTextPrivateKey(salt), privateKey);
			privateKey.setReadable(false, false);
			privateKey.setWritable(false, false);
			privateKey.setExecutable(false, false);
			
			privateKey.setReadable(true, true);
			privateKey.setWritable(true, true);
			privateKey.setExecutable(false, true);
		}
		
		try {
			
			client = system.getRemoteDataClient();
			
			remoteUploadDirectoryAbsolutePath = client.resolvePath(remoteUploadDirectory);
			
			if (verbose) System.out.println("Authenticating to " + system.getStorageConfig().getDefaultAuthConfig().getUsername() + 
					"@" + system.getStorageConfig().getHost() + ":" +  system.getStorageConfig().getPort() + 
					remoteUploadDirectoryAbsolutePath );
			
			if (verbose) System.out.println("Test file size: " + testFile.length());
			
			client.authenticate();
			
			if (verbose) System.out.println("Creating remote upload directory: " + remoteUploadDirectory);
			
			if (!client.mkdirs(remoteUploadDirectory)) {
				if (verbose) System.out.println("Failed to crate remote upload directory: " + remoteUploadDirectory);
			}
			
			FileWriter writer = new FileWriter(batchFileUpload);
			writer.append("lcd " + testFile.getParentFile().getAbsolutePath() + "\n");
//			writer.append("mkdir -P " + client.resolvePath(remoteUploadDirectory) + "\n");
			writer.append("put " + testFile.getName() + " " + remoteUploadDirectoryAbsolutePath + "/" + testFile.getName() + " \n");
			writer.append("quit");
			writer.flush();
			writer.close();
			
			writer = new FileWriter(batchFileDownload);
			writer.append("lcd " + testFile.getParentFile().getAbsolutePath() + "\n");
			writer.append("get " + remoteUploadDirectoryAbsolutePath + "/" + testFile.getName() + " " + testFile.getAbsolutePath() + " \n");
			writer.append("quit");
			writer.flush();
			writer.close();
			
			for (int i=0; i<iterations; i++) {
				if (Boolean.parseBoolean(includeUploadTest)) {
					if (verbose) System.out.println("Starting upload test...");
					if (verbose) System.out.println("\tTransfer: file:///" + testFile.getAbsoluteFile() + " => agave://" + system.getSystemId() + "/" + remoteUploadDirectoryAbsolutePath + "/" + testFile.getName());
					
					
//					String cmd = String.format("sftp -p %d –B %s %s@%s", 
//							system.getStorageConfig().getPort(),  
//							batchFileUpload.getAbsolutePath(),
//							system.getStorageConfig().getDefaultAuthConfig().getUsername(),
//							system.getStorageConfig().getHost());
//					
					CommandLine cmdLine = getSFTPCommandLine(privateKey, pubKey, system, batchFileUpload);
					
					DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

					outputStream = new ByteArrayOutputStream();
					
				    ExecuteWatchdog watchdog = new ExecuteWatchdog(10 * 60 * 1000);
					Executor executor = new DefaultExecutor();
					executor.setExitValue(0);
					executor.setWatchdog(watchdog);
					PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
					
					// add any needed environment variables for the auth and/or command
					Map<String,String> envMap = new HashMap<String,String>();
					if (system.getStorageConfig().getDefaultAuthConfig().getType() == AuthConfigType.PASSWORD) {
						envMap.put("SSHPASS",system.getStorageConfig().getDefaultAuthConfig().getClearTextPassword(getSystemSalt()));
					}
					envMap.put("SSH_ASKPASS",askpassFile.getAbsolutePath());
					envMap.put("DISPLAY",":");
					envMap.put("PATH", "/usr/bin:/bin:/usr/sbin:/sbin");
					envMap.put("HOME", System.getProperty("user.home"));
					envMap.put("SHELL", "/bin/bash");
					envMap.put("SSH_AUTH_SOCK", "");
					
					executor.setStreamHandler(streamHandler);
					
					t1 = System.currentTimeMillis();
//					executor.execute(cmdLine, envMap, resultHandler);
					String[] args = new String[] {"sftp", 
							"-i", privateKey.getAbsolutePath(), 
							"-P", String.valueOf(system.getStorageConfig().getPort()), 
							"-o", "UserKnownHostsFile=/dev/null", 
							"-o", "StrictHostKeyChecking=false", 
							"-b", 
							batchFileUpload.getAbsolutePath(),
							system.getStorageConfig().getDefaultAuthConfig().getUsername() + "@" + system.getStorageConfig().getHost()};
					ProcessBuilder pb = new ProcessBuilder(args)
			        .directory(tmpdir)
			        .redirectErrorStream(true)
			        .redirectOutput(java.lang.ProcessBuilder.Redirect.INHERIT);
					pb.environment().putAll(envMap);
			        Process pp = pb.start();
			        
					int exitValue = pp.waitFor();
					// some time later the result handler callback was invoked so we
					// can safely request the exit value
//					resultHandler.waitFor();
					
					t2 = System.currentTimeMillis();
					
//					int exitValue = resultHandler.getExitValue();
					
					if (exitValue != 0) {
						throw new RemoteDataException("Failed to fork upload command " + cmdLine.toString() + "\n" + outputStream.toString(), resultHandler.getException() );
					}
					else {
						if (verbose) System.out.println(outputStream.toString());
					}
					
					long duration = ((t2 - t1)/1000);
					if (verbose) System.out.println("\tPerf: " + ((t2 - t1)) + "ms " + TransferTask.formatMaxMemory(testFile.length() / duration) + "/s ");
					
					outputStream.close();
				}
				
				if (Boolean.parseBoolean(includeDownloadTest)) {
					if (verbose) System.out.println("Starting download test...");
					if (verbose) System.out.println("\tTransfer: agave://" + system.getSystemId() + "/" + testFile.getName() + " => file:///" + testFile.getAbsoluteFile());
					
//					String cmd = String.format("sftp -p %d –B %s %s@%s", 
//							system.getStorageConfig().getPort(),  
//							batchFileDownload.getAbsolutePath(),
//							system.getStorageConfig().getDefaultAuthConfig().getUsername(),
//							system.getStorageConfig().getHost());
					
					CommandLine cmdLine = getSFTPCommandLine(privateKey, pubKey, system, batchFileDownload);

					DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

					outputStream = new ByteArrayOutputStream();
					inputStream = new ByteArrayOutputStream();
				    ExecuteWatchdog watchdog = new ExecuteWatchdog(10 * 60 * 1000);
					Executor executor = new DefaultExecutor();
					executor.setExitValue(0);
					executor.setWatchdog(watchdog);
					PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
					
					// add any needed environment variables for the auth and/or command
					Map<String,String> envMap = new HashMap<String,String>();
					if (system.getStorageConfig().getDefaultAuthConfig().getType() == AuthConfigType.PASSWORD) {
						envMap.put("SSHPASS",system.getStorageConfig().getDefaultAuthConfig().getClearTextPassword(getSystemSalt()));
					}
					envMap.put("SSH_ASKPASS",askpassFile.getAbsolutePath());
					envMap.put("DISPLAY",":");
					envMap.put("PATH", "/usr/bin:/bin:/usr/sbin:/sbin");
					envMap.put("HOME", System.getProperty("user.home"));
					envMap.put("SHELL", "/bin/bash");
//					envMap.put("SSH_AUTH_SOCK", "");
					
					executor.setStreamHandler(streamHandler);
					
					t3 = System.currentTimeMillis();
					
					executor.execute(cmdLine, resultHandler);
					
					// some time later the result handler callback was invoked so we
					// can safely request the exit value
					resultHandler.waitFor();
					
					t4 = System.currentTimeMillis();
					
					int exitValue = resultHandler.getExitValue();
					
					if (exitValue != 0) {
						throw new RemoteDataException("Failed to fork download command " + cmdLine.toString() + "\n" + outputStream.toString(), resultHandler.getException());
					}
					else {
						if (verbose) System.out.println(outputStream.toString());
					}
					
					long duration = ((t4 - t3)/1000);
					if (verbose) System.out.println("\tPerf: " + ((t4 - t3)) + "ms " + TransferTask.formatMaxMemory(testFile.length() / duration) + "/s");
					
					outputStream.close();
					inputStream.close();
				}
			}
			
			sb.append(calculateTime(t2, t1, t4, t3, length) + "\n");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteDataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteCredentialException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try { outputStream.close();} catch (Exception e) {}
			try { inputStream.close(); } catch (Exception e) {}
			try { client.delete(remoteUploadDirectory); }  catch (Exception e) {}
			try { client.disconnect();} catch (Exception e) {}
			try { uninstallSshKeys(privateKey, pubKey);} catch (Exception e) {}
			FileUtils.deleteQuietly(tmpdir);
			FileUtils.deleteQuietly(batchFileDownload);
			
//			if (StringUtils.isEmpty(testFilename)) {
//				System.out.println("Cleaning up local directory");
//				FileUtils.deleteQuietly(testFile);
//			}
		}
		
		return sb.toString();
	}

	private File createAskPassFile(File tempdir, RemoteSystem system) 
	throws EncryptionException, IOException 
	{
		File askpassFile = new File(tempdir, "askpass");
		String askPassScript = String.format("#!/bin/bash \necho '%s';\n\n", 
				system.getStorageConfig().getDefaultAuthConfig().getClearTextPassword(getSystemSalt()));
		FileWriter writer = new FileWriter(askpassFile);
		writer.append(askPassScript);
		writer.flush();
		writer.close();
		
		askpassFile.setReadable(false, false);
		askpassFile.setWritable(false, false);
		askpassFile.setExecutable(false, false);
		
		askpassFile.setReadable(true, true);
		askpassFile.setWritable(true, true);
		askpassFile.setExecutable(true, true);
		
		return askpassFile;
	}

	private CommandLine getSFTPCommandLine(File privateKey, File pubKey, RemoteSystem system, File sftpCommandFile) 
	throws RemoteDataException, EncryptionException, IOException, PermissionException 
	{
		
		CommandLine cmdLine = null;
		
		if (system.getStorageConfig().getProtocol() == StorageProtocolType.SFTP) {
			
			if (system.getStorageConfig().getDefaultAuthConfig().getType() == AuthConfigType.SSHKEYS) {
				
				// if we have password protected keys, we need the ssh-agent to handle the prompt
				if ( ! StringUtils.isEmpty(system.getStorageConfig().getDefaultAuthConfig().getPassword()) ) {
					String decryptedPassword = system.getStorageConfig().getDefaultAuthConfig().getClearTextPassword(getSystemSalt());
					installSshKeys(privateKey, pubKey, decryptedPassword);
				}
				
//				cmdLine = new CommandLine("sftp");
//				cmdLine.addArgument("${key}");
				
//				cmdLine = new CommandLine("sh");
//				cmdLine.addArgument("-l");
//				cmdLine.addArgument("-c");
				
				cmdLine = CommandLine.parse(String.format("sftp -i %s -P %d -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=false -b %s %s@%s",
						privateKey.getAbsolutePath(),
						system.getStorageConfig().getPort(),
						sftpCommandFile.getAbsolutePath(),
						system.getStorageConfig().getDefaultAuthConfig().getUsername(),
						system.getStorageConfig().getHost()));
//				cmdLine = CommandLine.parse("sh -l -c \"env\"");
			}
			// otherwise we're using password login, so we need to 
			else if (system.getStorageConfig().getDefaultAuthConfig().getType() == AuthConfigType.PASSWORD) {
//				cmdLine = CommandLine.parse(String.format("/usr/local/bin/sshpass -e sftp"));
				
//				cmdLine = new CommandLine("sh");
//				cmdLine.addArgument("-l");
//				cmdLine.addArgument("-c");
//				cmdLine.addArgument(String.format("/usr/local/bin/sshpass -e sftp -v -i %s -P %d -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=false -b %s %s@%s",
//						privateKey.getAbsolutePath(),
//						system.getStorageConfig().getPort(),
//						sftpCommandFile.getAbsolutePath(),
//						system.getStorageConfig().getDefaultAuthConfig().getUsername(),
//						system.getStorageConfig().getHost()));
			}
			// no idea what else this could be
			else {
				throw new RemoteDataException("Unknown auth type for SFTP: " + system.getStorageConfig().getDefaultAuthConfig().getType());
			}
		}
//		cmdLine.addArgument("-v");
//		cmdLine.addArgument("-P");
//		cmdLine.addArgument("${port}");
//		cmdLine.addArgument("-o");
//		cmdLine.addArgument("UserKnownHostsFile=/dev/null");
//		cmdLine.addArgument("-o");
//		cmdLine.addArgument("StrictHostKeyChecking=false");
		
//		cmdLine.addArgument("-b");
//		cmdLine.addArgument("${uploadFile}");
//		cmdLine.addArgument("${username}@${hostname}");
//		HashMap<String,String> map = new HashMap<String,String>();
//		map.put("port", String.valueOf(system.getStorageConfig().getPort()));
//		map.put("uploadFile", sftpCommandFile.getAbsolutePath());
//		map.put("username", system.getStorageConfig().getDefaultAuthConfig().getUsername());
//		map.put("hostname", system.getStorageConfig().getHost());
//		if (system.getStorageConfig().getDefaultAuthConfig().getType() == AuthConfigType.SSHKEYS) {
//			map.put("key", privateKey.getAbsolutePath());
//		}
//		cmdLine.setSubstitutionMap(map);
		
		return cmdLine;
	}

	private String getSystemSalt() {
		return system.getSystemId() + 
				system.getStorageConfig().getHost() + 
    			system.getStorageConfig().getDefaultAuthConfig().getUsername();
	}

	
	public boolean uninstallSshKeys(File privateKeyPath, File publicKey) 
	throws PermissionException, IOException 
	{
		CommandLine cmdLine = new CommandLine("ssh-add");
		cmdLine.addArgument("-d");
		cmdLine.addArgument("${publicKey}");
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("publicKey", publicKey.getAbsolutePath());
		cmdLine.setSubstitutionMap(map);
		
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		try {
			outputStream = new ByteArrayOutputStream();
			
		    ExecuteWatchdog watchdog = new ExecuteWatchdog(10 * 60 * 1000);
			Executor executor = new DefaultExecutor();
			executor.setExitValue(0);
			executor.setWatchdog(watchdog);
			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			
			executor.setStreamHandler(streamHandler);
			executor.execute(cmdLine, resultHandler);
			
			// some time later the result handler callback was invoked so we
			// can safely request the exit value
			resultHandler.waitFor();
			
			int exitValue = resultHandler.getExitValue();
			
			if (exitValue != 0) {
				throw new IOException("Failed to remove ssh key at " + privateKeyPath.getAbsolutePath() +
						" to the local ssh agent.\n" + outputStream.toString(), resultHandler.getException() );
			}
			else {
				if (this.verbose) System.out.println(outputStream.toString());
				return true;
			}
		}
		catch (InterruptedException e) {
			throw new PermissionException("Process timed out while attempting to remove the ssh key for " + privateKeyPath.getAbsolutePath() +
						" from the local ssh agent.", e);
		}
		finally {
			try { outputStream.close();} catch (Exception e) {}
		}
	}
	
	public void installSshKeys(File privateKeyPath, File publicKey, String passPhrase) 
	throws IOException, PermissionException 
	{
		File expectFile = new File(privateKeyPath.getParentFile(), "expect.stdin");
		String expectPhrase = String.format("#!/usr/bin/expect -f\nspawn ssh-add \"%s\"\nexpect \"Enter passphrase for %s:\"\nsend \"%s\n\";\ninteract\n\n", 
				privateKeyPath.getAbsolutePath(),
				privateKeyPath.getAbsolutePath(),
				passPhrase);
		FileWriter writer = new FileWriter(expectFile);
		writer.append(expectPhrase);
		writer.flush();
		writer.close();
		
		expectFile.setReadable(false, false);
		expectFile.setWritable(false, false);
		expectFile.setExecutable(false, false);
		
		expectFile.setReadable(true, true);
		expectFile.setWritable(true, true);
		expectFile.setExecutable(true, true);
		
		Process pp = new ProcessBuilder(new String[]{"whoami"})
	        .directory(privateKeyPath.getParentFile())
	        .redirectErrorStream(true)
	        .redirectOutput(java.lang.ProcessBuilder.Redirect.INHERIT)
	        .start();
		
		
		pp = new ProcessBuilder(new String[]{"pwd"})
	        .directory(privateKeyPath.getParentFile())
	        .redirectErrorStream(true)
	        .redirectOutput(java.lang.ProcessBuilder.Redirect.INHERIT)
	        .start();
		
		pp = new ProcessBuilder(new String[]{"env"})
	        .directory(privateKeyPath.getParentFile())
	        .redirectErrorStream(true)
	        .redirectOutput(java.lang.ProcessBuilder.Redirect.INHERIT)
	        .start();
		
		CommandLine cmdLine = new CommandLine("${expectFile}");
		HashMap<String,String> map = new HashMap<String,String>();
		map.put("expectFile", expectFile.getAbsolutePath());
		cmdLine.setSubstitutionMap(map);
		
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		try {
			outputStream = new ByteArrayOutputStream();
			
		    ExecuteWatchdog watchdog = new ExecuteWatchdog(10 * 60 * 1000);
			Executor executor = new DefaultExecutor();
			executor.setExitValue(0);
			executor.setWatchdog(watchdog);
			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			
			executor.setStreamHandler(streamHandler);
			executor.execute(cmdLine, resultHandler);
			
			// some time later the result handler callback was invoked so we
			// can safely request the exit value
			resultHandler.waitFor();
			
			int exitValue = resultHandler.getExitValue();
			
			if (exitValue != 0) {
				if (this.verbose) System.out.println(outputStream.toString());
				throw new IOException("Failed to add ssh key at " + privateKeyPath.getAbsolutePath() +
						" to the local ssh agent.\n" + outputStream.toString(), resultHandler.getException() );
			}
			else {
				if (this.verbose) System.out.println(outputStream.toString());
			}
		}
		catch (InterruptedException e) {
			throw new PermissionException("Process timed out while attempting to add the ssh key for " + privateKeyPath.getAbsolutePath() +
					" to the local ssh agent.\n" + outputStream.toString(), e );
		}
		finally {
			try { outputStream.close();} catch (Exception e) {}
			
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

	

	@Override
	protected void authenticate() throws AuthenticationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Object getClient() throws RemoteDataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String benchmark(String systemId, String testFilename,
			boolean includeUploadTest, boolean includeDownloadTest,
			String fileSize, int iterations, boolean streaming, int bufferSize,
			boolean verboseOutput, boolean debugOutput, boolean inMemory)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
