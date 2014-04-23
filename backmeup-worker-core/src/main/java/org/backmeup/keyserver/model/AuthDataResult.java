package org.backmeup.keyserver.model;

import java.util.Arrays;
import java.util.Properties;

import org.backmeup.model.Token;


public class AuthDataResult {

  private Token newToken;  
  private String encryptionPwd;
  
  public static class UserData {
    private Long bmuUserId;
    

    public Long getBmuUserId() {
      return bmuUserId;
    }

    public void setBmuUserId(Long bmuUserId) {
      this.bmuUserId = bmuUserId;
    }

    public UserData() {
      super();
    }

    public UserData(Long bmuUserId) {
      super();
      this.bmuUserId = bmuUserId;
    }
  }

  private UserData user;
  private AuthData[] authinfos;

  public AuthDataResult(UserData user, AuthData[] authinfos) {
    super();
    this.user = user;
    
    if(authinfos == null) { 
    	this.authinfos = new AuthData[0]; 
    } else { 
    	this.authinfos = Arrays.copyOf(authinfos, authinfos.length); 
    } 
  }

  public AuthDataResult() {
    super();
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

  public void setAuthinfos(AuthData[] authinfos) {
	  if(authinfos == null) { 
		  this.authinfos = new AuthData[0]; 
	  } else { 
		  this.authinfos = Arrays.copyOf(authinfos, authinfos.length); 
	  }
  }

  
  public Token getNewToken() {
    return newToken;  
  }

  public void setNewToken(Token newToken) {
    this.newToken = newToken;
  }

  public Properties getByProfileId(Long profileId) {
    for (int i = 0; i < authinfos.length; i++) {
      if (authinfos[i].getBmuAuthinfoId().equals(profileId)) {
        return authinfos[i].getAiDataProperties();
      }
    }
    return new Properties();
  }

  public String getEncryptionPwd() {
    return encryptionPwd;
  }

  public void setEncryptionPwd(String encryptionPwd) {
    this.encryptionPwd = encryptionPwd;
  }
}
