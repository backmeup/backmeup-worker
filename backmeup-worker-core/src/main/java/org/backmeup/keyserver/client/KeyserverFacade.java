package org.backmeup.keyserver.client;

import org.backmeup.keyserver.model.AuthDataResult;
import org.backmeup.model.Token;

public interface KeyserverFacade {
    AuthDataResult getData(Token token);
}
