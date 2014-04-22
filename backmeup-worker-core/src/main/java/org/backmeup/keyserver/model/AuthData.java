package org.backmeup.keyserver.model;

import java.util.HashMap;
import java.util.Properties;

public class AuthData {
  private Long bmuAuthinfoId;
  private Long bmuUserId;
  private Long bmuServiceId;
  private HashMap<String, String> aiData = new HashMap<String, String>();

  public AuthData() {
    super();
  }

  public AuthData(Long bmuAuthinfoId, Long bmuUserId, Long bmuServiceId) {
    super();
    this.bmuAuthinfoId = bmuAuthinfoId;
    this.bmuUserId = bmuUserId;
    this.bmuServiceId = bmuServiceId;
  }

  public Long getBmuAuthinfoId() {
    return bmuAuthinfoId;
  }

  public void setBmuAuthinfoId(Long bmuAuthinfoId) {
    this.bmuAuthinfoId = bmuAuthinfoId;
  }
  
  public Long getBmuUserId() {
    return bmuUserId;
  }

  public void setBmuUserId(Long bmuUserId) {
    this.bmuUserId = bmuUserId;
  }

  public Long getBmuServiceId() {
    return bmuServiceId;
  }

  public void setBmuServiceId(Long bmuServiceId) {
    this.bmuServiceId = bmuServiceId;
  }

  public HashMap<String, String> getAiData() {
    return aiData;
  }

  public void setAiData(HashMap<String, String> aiData) {
    this.aiData = aiData;
  }
  
  public Properties getAiDataProperties() {
    Properties props = new Properties();
    props.putAll(this.aiData);
    return props;
  }

}
