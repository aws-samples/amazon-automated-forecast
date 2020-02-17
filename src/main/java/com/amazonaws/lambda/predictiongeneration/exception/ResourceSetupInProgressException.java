package com.amazonaws.lambda.predictiongeneration.exception;

public class ResourceSetupInProgressException extends RuntimeException {

    public static final long serialVersionUID = -7480718769026135873L;

    public ResourceSetupInProgressException(String message) {
        super(message);
    }
}
