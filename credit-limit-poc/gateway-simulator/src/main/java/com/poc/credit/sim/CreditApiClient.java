package com.poc.credit.sim;

import com.poc.credit.common.Json;
import com.poc.credit.common.api.AmendRequest;
import com.poc.credit.common.api.AmendResponse;
import com.poc.credit.common.api.CreditCheckRequest;
import com.poc.credit.common.api.CreditCheckResponse;
import com.poc.credit.common.api.ExposureView;
import com.poc.credit.common.security.DevTokens;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

/** Minimal HTTP client for the credit API, as a gateway would call it. */
public class CreditApiClient {

    public record Result<T>(int status, T body, long latencyMicros, String error) {
        public boolean ok() {
            return status == 200 && body != null;
        }
    }

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();
    private final String baseUrl;
    private final String gatewayToken;
    private final String officerToken;

    public CreditApiClient(String baseUrl, String jwtSecret) {
        this.baseUrl = baseUrl;
        this.gatewayToken = DevTokens.mint(jwtSecret, "gateway-simulator", List.of(DevTokens.ROLE_GATEWAY), Duration.ofHours(12));
        this.officerToken = DevTokens.mint(jwtSecret, "sim-officer", List.of(DevTokens.ROLE_CREDIT_OFFICER), Duration.ofHours(12));
    }

    public Result<CreditCheckResponse> check(CreditCheckRequest request) {
        return send(post("/api/v1/credit-checks", Json.write(request), gatewayToken), CreditCheckResponse.class);
    }

    public Result<AmendResponse> amend(String orderId, AmendRequest request) {
        return send(post("/api/v1/credit-checks/" + orderId + "/amend", Json.write(request), gatewayToken),
                AmendResponse.class);
    }

    public Result<ExposureView> exposure(String accountId) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/exposure/" + accountId))
                .timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer " + officerToken)
                .GET().build();
        return send(req, ExposureView.class);
    }

    private HttpRequest post(String path, String body, String token) {
        return HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private <T> Result<T> send(HttpRequest request, Class<T> type) {
        long start = System.nanoTime();
        try {
            HttpResponse<String> res = http.send(request, HttpResponse.BodyHandlers.ofString());
            long micros = (System.nanoTime() - start) / 1_000;
            if (res.statusCode() == 200) {
                return new Result<>(200, Json.read(res.body(), type), micros, null);
            }
            return new Result<>(res.statusCode(), null, micros, res.body());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new Result<>(-1, null, (System.nanoTime() - start) / 1_000, e.toString());
        }
    }
}
