package com.poc.credit.processor.web;

import com.poc.credit.processor.service.RebuildService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Operator endpoints. POC: unauthenticated and meant for the internal network only; in
 * production these sit behind the bank's ops tooling with their own authorization.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final RebuildService rebuild;

    public AdminController(RebuildService rebuild) {
        this.rebuild = rebuild;
    }

    /** Synchronous: returns when the rebuild has finished (or failed). */
    @PostMapping("/rebuild")
    public ResponseEntity<RebuildService.RebuildReport> rebuild() {
        RebuildService.RebuildReport report = rebuild.rebuild("manual");
        return ResponseEntity.status(report.success() ? HttpStatus.OK : HttpStatus.INTERNAL_SERVER_ERROR).body(report);
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> status = new HashMap<>();
        status.put("rebuilding", rebuild.isRunning());
        status.put("lastRebuild", rebuild.lastReport());
        return status;
    }

    @ExceptionHandler(IllegalStateException.class)
    ResponseEntity<Map<String, String>> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }
}
