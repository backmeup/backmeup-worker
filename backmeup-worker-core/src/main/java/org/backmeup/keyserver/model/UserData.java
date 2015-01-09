package org.backmeup.keyserver.model;

public class UserData {
    private Long bmu_user_id;

    public UserData() {
    }

    public UserData(Long bmuUserId) {
        this.bmu_user_id = bmuUserId;
    }

    public Long getBmuUserId() {
        return bmu_user_id;
    }

    public void setBmuUserId(Long bmuUserId) {
        this.bmu_user_id = bmuUserId;
    }
}
