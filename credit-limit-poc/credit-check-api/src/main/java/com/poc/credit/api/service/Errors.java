package com.poc.credit.api.service;

/** Business exceptions mapped to HTTP statuses by the API exception handler. */
public final class Errors {

    private Errors() {
    }

    /** 404 */
    public static class NotFoundException extends RuntimeException {
        public NotFoundException(String message) {
            super(message);
        }
    }

    /** 409: order ID reused for a different order, order closed, or amend already pending. */
    public static class ConflictException extends RuntimeException {
        public ConflictException(String message) {
            super(message);
        }
    }

    /** 422: the account or its group has no limit loaded. */
    public static class NoLimitException extends RuntimeException {
        public NoLimitException(String message) {
            super(message);
        }
    }

    /** 503: fail closed (rebuild in progress, dependency down). */
    public static class ServiceUnavailableException extends RuntimeException {
        public ServiceUnavailableException(String message) {
            super(message);
        }
    }
}
