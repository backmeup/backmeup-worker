package org.backmeup.keyserver.model;

import java.util.HashMap;
import java.util.Properties;

public class AuthData {
  private Long bmu_authinfo_id;
  private Long bmu_user_id;
  private Long bmu_service_id;
  private HashMap<String, String> ai_data = new HashMap<>();

  public AuthData() {
    super();
  }

  public AuthData(Long bmuAuthinfoId, Long bmuUserId, Long bmuServiceId) {
    super();
    this.bmu_authinfo_id = bmuAuthinfoId;
    this.bmu_user_id = bmuUserId;
    this.bmu_service_id = bmuServiceId;
  }

  public Long getBmuAuthinfoId() {
    return bmu_authinfo_id;
  }

  public void setBmuAuthinfoId(Long bmuAuthinfoId) {
    this.bmu_authinfo_id = bmuAuthinfoId;
  }
  
  public Long getBmuUserId() {
    return bmu_user_id;
  }

  public void setBmuUserId(Long bmuUserId) {
    this.bmu_user_id = bmuUserId;
  }

  public Long getBmuServiceId() {
    return bmu_service_id;
  }

  public void setBmuServiceId(Long bmuServiceId) {
    this.bmu_service_id = bmuServiceId;
  }

  public HashMap<String, String> getAiData() {
    return ai_data;
  }

  public void setAiData(HashMap<String, String> aiData) {
    this.ai_data = aiData;
  }
  
  public Properties getAiDataProperties() {
    Properties props = new Properties();
    props.putAll(this.ai_data);
    return props;
  }

}
