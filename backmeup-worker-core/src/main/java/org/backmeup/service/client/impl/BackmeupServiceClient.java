package org.backmeup.service.client.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.backmeup.model.BackupJob;
import org.backmeup.model.JobProtocol;
import org.backmeup.model.Status;
import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class BackmeupServiceClient implements BackmeupServiceFacade {
	private final Logger logger = LoggerFactory.getLogger(BackmeupServiceClient.class);
	
	private String scheme;
	
	private String host;
	
	private String port;

	private String basePath;
	
	private DefaultHttpClient createClient() {
			return new DefaultHttpClient();
	}

	private Result execute(String path, ReqType type, String jsonParams) {
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
				HttpPost post;
				request = post = new HttpPost(registerUri);
				if (jsonParams != null) {
					StringEntity entity = new StringEntity(jsonParams, "UTF-8");
					BasicHeader header = new BasicHeader(HTTP.CONTENT_TYPE, "application/json");
					entity.setContentType(header);
					post.setEntity(entity);
				}
				break;
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
	
	private static class Result {
		public HttpResponse response;
		
		public String content;
	}

	private enum ReqType {
		GET, DELETE, PUT, POST
	}

	@Override
	public Status saveStatus(Status status) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteStatusBefore(Long jobId, Date timeStamp) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BackupJob findBackupJobById(String username, Long jobId) {
		Gson g = new Gson();
		Result r = execute("/jobs/" + username + "/" + jobId, ReqType.GET, null);
		if (r.response.getStatusLine().getStatusCode() == 200) {
			return g.fromJson(r.content, BackupJob.class);
		}
		throw new BackMeUpException("Failed to retrieve BackupJob: " + r.content);
	}

	@Override
	public BackupJob saveBackupJob(BackupJob backupJob) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobProtocol saveJobProtocol(JobProtocol protocol) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteJobProtocolByUsername(String username) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendEmail(String to, String subject, String message) {
		// TODO Auto-generated method stub
		
	}

}
