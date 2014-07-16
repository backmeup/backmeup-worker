package org.backmeup.keyserver.client.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.keyserver.model.AuthDataResult;
import org.backmeup.model.Token;
import org.backmeup.model.exceptions.BackMeUpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class KeyserverClient implements KeyserverFacade {
	private final Logger logger = LoggerFactory.getLogger(KeyserverClient.class);

	private static class Result {
		public HttpResponse response;
		public String content;
	}

	private enum ReqType {
		GET, DELETE, PUT, POST
	}

	private final String scheme;

	private final String host;

	private final String path;

//	private String keystore;
//
//	private String keystoreType;
//
//	private String keystorePwd;
//
//	private String truststore;
//
//	private String truststoreType;
//
//	private String truststorePwd;
//
//	private Boolean allowAllHostnames;
//
//	private SchemeRegistry schemeRegistry;

	public KeyserverClient(String scheme, String host, String path) {
		this.scheme = scheme;
		this.host = host;
		this.path = path;
	}

	private HttpClient createClient() {
		if (scheme.equals("http")) {
			return HttpClientBuilder.create().build();
		} else if (scheme.equals("https")) {
			/*
			if (schemeRegistry == null) {
				try {
					KeyStore keystore = KeyStore.getInstance(keystoreType != null ? keystoreType : KeyStore.getDefaultType());
					InputStream keystoreInput = getClass().getClassLoader().getResourceAsStream(this.keystore);
					if (keystoreInput == null) {
						logger.error("Can't find keystore file: %s",this.keystore);
						throw new BackMeUpException(String.format("Configuration specifies keystore \"%s\", but the file does not exist. Please copy the keystore to the resources folder of the project.",this.keystore));
					}

					keystore.load(keystoreInput, keystorePwd != null ? keystorePwd.toCharArray() : null);
					keystoreInput.close();

					// Don't throw nullpoint if one of the keystore files couldn't be loaded
					// Instead, load the truststore, leave it null to rely on cacerts
					// distributed with the JVM
					KeyStore truststore = null;
					if (this.truststore != null) {
						truststore = KeyStore.getInstance(truststoreType != null ? truststoreType : KeyStore.getDefaultType());
						InputStream truststoreInput = getClass().getClassLoader().getResourceAsStream(this.truststore);
						if (truststoreInput == null) {
							logger.error("Can't find truststore file: %s",this.truststore);
							throw new BackMeUpException(String.format("Configuration specifies to use truststore \"%s\", but the file does not exist. Please copy the truststore to the resources folder of the project.", this.truststore));
						}

						truststore.load(truststoreInput, truststorePwd != null ? truststorePwd.toCharArray() : null);
						truststoreInput.close();
					}
					schemeRegistry = new SchemeRegistry();
					SSLSocketFactory lSchemeSocketFactory = new SSLSocketFactory(
							SSLSocketFactory.TLS,
							keystore,
							keystorePwd,
							truststore,
							(SecureRandom) null,
							allowAllHostnames ? SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER : SSLSocketFactory.STRICT_HOSTNAME_VERIFIER);
					schemeRegistry.register(new Scheme("https", 443, lSchemeSocketFactory));
				} catch (Exception e) {
					throw new BackMeUpException(e);
				}
			}
			
			final HttpParams httpParams = new BasicHttpParams();
			return new DefaultHttpClient( new SingleClientConnManager(schemeRegistry), httpParams);
			*/
			throw new BackMeUpException(String.format("Keyserver scheme \"%s\" not supported", scheme));
		} else {
			logger.error("Keyserver scheme not supported: %s", scheme);
			throw new BackMeUpException(String.format("Keyserver scheme \"%s\" not supported", scheme));
		}
	}

	private Result execute(String path, ReqType type, String jsonParams) {
		HttpClient client = createClient();
		int port = "http".equals(scheme) ? 80 : 443;
		String rHost = host;
		if (host.contains(":")) {
			String[] sp = host.split(":");
			rHost = sp[0];
			try {
				port = Integer.parseInt(sp[1]);
			} catch (Exception ex) {
				logger.error("", ex);
			}
		}

		try {
			URI registerUri = new URI(scheme, null, rHost, port, path, null, null);
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

	@Override
	public AuthDataResult getData(Token token) {
		Gson g = new Gson();
		String json = g.toJson(token);
		Result r = execute(path + "/tokens/data", ReqType.POST, json);
		if (r.response.getStatusLine().getStatusCode() == 200) {
			logger.debug("Received token data: " + r.content);
			return g.fromJson(r.content, AuthDataResult.class);
		}
		throw new BackMeUpException("Failed to retrieve token data: " + r.content);
	}
}
