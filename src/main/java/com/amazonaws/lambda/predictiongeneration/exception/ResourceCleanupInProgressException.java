package com.amazonaws.lambda.predictiongeneration.exception;

public class ResourceCleanupInProgressException extends RuntimeException {

    public static final long serialVersionUID = 3032649540135073597L;

    public ResourceCleanupInProgressException(String message) {
        super(message);
    }
}
