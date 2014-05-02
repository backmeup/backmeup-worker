package org.backmeup.keyserver.model;

import java.util.Arrays;
import java.util.Properties;

import org.backmeup.model.Token;

public class AuthDataResult {
	private Token newToken;
	private String encryption_pwd;
	private UserData user;
	private AuthData[] authinfos;

	public AuthDataResult() {
	}

	public AuthDataResult(UserData user, AuthData[] authInformation) {
		this.user = user;

		if (authInformation == null) {
			this.authinfos = new AuthData[0];
		} else {
			this.authinfos = Arrays.copyOf(authInformation,
					authInformation.length);
		}
	}

	public UserData getUser() {
		return user;
	}

	public void setUser(UserData user) {
		this.user = user;
	}

	public AuthData[] getAuthinfos() {
		return authinfos;
	}

	public void setAuthinfos(AuthData[] authInformation) {
		if (authInformation == null) {
			this.authinfos = new AuthData[0];
		} else {
			this.authinfos = Arrays.copyOf(authInformation,	authInformation.length);
		}
	}

	public Token getNewToken() {
		return newToken;
	}

	public void setNewToken(Token newToken) {
		this.newToken = newToken;
	}

	public String getEncryptionPwd() {
		return encryption_pwd;
	}

	public void setEncryptionPwd(String encryptionPwd) {
		this.encryption_pwd = encryptionPwd;
	}

	public Properties getByProfileId(Long profileId) {
		for (int i = 0; i < authinfos.length; i++) {
			if (authinfos[i].getBmuAuthinfoId().equals(profileId)) {
				return authinfos[i].getAiDataProperties();
			}
		}
		return new Properties();
	}
}
