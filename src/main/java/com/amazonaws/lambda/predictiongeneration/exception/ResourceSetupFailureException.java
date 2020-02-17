package com.amazonaws.lambda.predictiongeneration.exception;

public class ResourceSetupFailureException extends RuntimeException {

    public static final long serialVersionUID = 457081345023074308L;

    public ResourceSetupFailureException(String message) {
        super(message);
    }
}
