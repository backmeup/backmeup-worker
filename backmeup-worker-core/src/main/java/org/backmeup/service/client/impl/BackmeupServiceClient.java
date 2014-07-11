package org.backmeup.service.client.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.backmeup.model.dto.BackupJobDTO;
import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackmeupServiceClient implements BackmeupServiceFacade {
	private final Logger logger = LoggerFactory.getLogger(BackmeupServiceClient.class);

	private final String scheme;

	private final String host;

	private final String port;

	private final String basePath;
	
	// Constructors -----------------------------------------------------------
	
	public BackmeupServiceClient(String scheme, String host, String port, String basePath) {
		this.scheme = scheme;
		this.host = host;
		this.port = port;
		this.basePath = basePath;
	}
	
	// Public methods ---------------------------------------------------------

	@Override
	public BackupJobDTO getBackupJob(Long jobId) {
		Result r = execute("/backupjobs/" + jobId + "?expandUser=true&expandToken=true&expandProfiles=true&expandProtocol=true", ReqType.GET, null, "7;password1");
		if (r.response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
			throw new BackMeUpException("Failed to retrieve BackupJob: " + r.content);
		}
		logger.debug("getBackupJob: " + r.content);
		
		try {
			ObjectMapper mapper = createJsonMapper();
			return mapper.readValue(r.content, BackupJobDTO.class);
		}  catch (IOException e) {
			logger.error("", e);
			throw new BackMeUpException("Failed to retrieve BackupJob: " + e);
		}
	}

	@Override
	public BackupJobDTO updateBackupJob(BackupJobDTO backupJob) {	
		try {
			ObjectMapper mapper = createJsonMapper();
			String json = mapper.writeValueAsString(backupJob);

			Result r = execute("/backupjobs/" + backupJob.getJobId(), ReqType.PUT, json);
			if (r.response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new BackMeUpException("Failed to update BackupJob: " + r.content);
			}

			logger.debug("saveBackupJob: " + r.content);
			return mapper.readValue(r.content, BackupJobDTO.class);

		} catch (IOException e) {
			logger.error("", e);
			throw new BackMeUpException("Failed to update BackupJob: " + e);
		}
	}
	
	// Private methods --------------------------------------------------------

	private DefaultHttpClient createClient() {
		return new DefaultHttpClient();
	}
	
	private Result execute(String path, ReqType type, String jsonParams) {
		return execute(path, type, jsonParams, "");
	}

	private Result execute(String path, ReqType type, String jsonParams, String authToken) {
		HttpClient client = createClient();

		int rPort = Integer.parseInt(port);
		String rPath = basePath + path;
		String rHost = host;
		if (host.contains(":")) {
			String[] sp = host.split(":");
			rHost = sp[0];
			try {
				rPort = Integer.parseInt(sp[1]);
			} catch (Exception ex) {
				logger.error("", ex);
			}
		}

		try {
			URI registerUri = new URI(scheme, null, rHost, rPort, rPath, null, null);
			HttpUriRequest request;

			switch (type) {
			case PUT:
				request = new HttpPut(registerUri);
				break;
			case DELETE:
				request = new HttpDelete(registerUri);
				break;
			case GET:
				request = new HttpGet(registerUri);
				break;
			default:
				HttpPost post = new HttpPost(registerUri);
				if (jsonParams != null) {
					StringEntity entity = new StringEntity(jsonParams, "UTF-8");					
					post.setEntity(entity);

					post.setHeader("Accept", "application/json");
					post.setHeader("Content-type", "application/json");
				}
				request = post;
				break;
			}

			if(!authToken.isEmpty()) {
				request.setHeader("Authorization", authToken);
			}
			
			HttpResponse response = client.execute(request);
			Result r = new Result();
			r.response = response;
			if (response.getEntity() != null) {
				try {
					r.content = new Scanner(response.getEntity().getContent()).useDelimiter("\\A").next();
				} catch (NoSuchElementException nee) {
					logger.debug("", nee);
				}
			}
			return r;
		} catch (URISyntaxException e) {
			throw new BackMeUpException(e);
		} catch (ClientProtocolException e) {
			throw new BackMeUpException(e);
		} catch (IOException e) {
			throw new BackMeUpException(e);
		}
	}
	
	private ObjectMapper createJsonMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
    	objectMapper.setSerializationInclusion(Inclusion.NON_NULL);
    	objectMapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    	objectMapper.configure(Feature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    	return objectMapper;
	}
	
	// Private classes and enums ----------------------------------------------
	
	private static class Result {
		public HttpResponse response;

		public String content;
	}

	private enum ReqType {
		GET, DELETE, PUT, POST
	}
}
