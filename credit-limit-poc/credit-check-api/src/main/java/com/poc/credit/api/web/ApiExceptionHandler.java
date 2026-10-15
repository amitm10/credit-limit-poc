package com.poc.credit.api.web;

import com.poc.credit.api.service.Errors;
import com.poc.credit.api.service.PublishFailedException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Fail closed: any dependency problem becomes 503 with Retry-After, never a guess.
 * Rejections are not errors; they are 200 responses with decision REJECTED.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler({Errors.ServiceUnavailableException.class, PublishFailedException.class,
            CallNotPermittedException.class, DataAccessException.class})
    ResponseEntity<ProblemDetail> unavailable(RuntimeException e) {
        if (e instanceof DataAccessException) {
            log.warn("Failing closed: {}", e.getMessage(), e);
        } else {
            log.warn("Failing closed: {}", e.getMessage());
        }
        ProblemDetail body = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Credit check unavailable, trade must not proceed: " + e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(body);
    }

    @ExceptionHandler(Errors.NotFoundException.class)
    ResponseEntity<ProblemDetail> notFound(Errors.NotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(Errors.ConflictException.class)
    ResponseEntity<ProblemDetail> conflict(Errors.ConflictException e) {
        return problem(HttpStatus.CONFLICT, e.getMessage());
    }

    @ExceptionHandler(Errors.NoLimitException.class)
    ResponseEntity<ProblemDetail> noLimit(Errors.NoLimitException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> invalid(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + " " + f.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("Invalid request");
        return problem(HttpStatus.BAD_REQUEST, detail);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, IllegalArgumentException.class,
            MissingServletRequestParameterException.class})
    ResponseEntity<ProblemDetail> badRequest(Exception e) {
        return problem(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String detail) {
        return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, detail));
    }
}
