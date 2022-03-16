package com.example.connectionpool;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.internal.shaded.reactor.pool.PoolAcquirePendingLimitException;
import reactor.netty.resources.ConnectionProvider;
import reactor.test.StepVerifier;

import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class ConnectionPoolApplicationTests {

  private static final WireMockServer wireMockServer = new WireMockServer();

  @BeforeAll
  static void setup() {
    wireMockServer.start();
  }

  @Test
  void givenSmallConnectionPoolExpectProblemWithPending() {
    wireMockServer.stubFor(post(urlEqualTo("/v1/messages"))
            .willReturn(aResponse()
                    .withStatus(201)
                    .withBody("OK")
                    .withFixedDelay(5000))
    );

    ConnectionProvider provider =
            ConnectionProvider.builder("custom")
                    .maxConnections(5)
                    .pendingAcquireTimeout(Duration.ofSeconds(1))
                    .build();

    HttpClient httpClient = HttpClient.create(provider);
    WebClient webClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .build();

    Mono<String> performPost = webClient
            .post()
            .uri(wireMockServer.baseUrl() + "/v1/messages")
            .retrieve()
            .bodyToMono(String.class);

    StepVerifier.create(Flux.interval(Duration.ofMillis(100))
            .take(100)
            .doOnNext(requestNumber -> System.out.println("Request number: %s (thread: %s)"
                    .formatted(requestNumber, Thread.currentThread().getName())))
            .flatMap(ign -> performPost))
            .verifyErrorSatisfies(error -> assertThat(error.getCause()).isInstanceOf(PoolAcquirePendingLimitException.class));
  }

  @AfterAll
  static void cleanup() {
    wireMockServer.stop();
  }

}
