
List<CompletableFuture<Void>> orgFutures = new ArrayList<>();

for (MajescoParty majescoParty : majescoOrgBatch) {
    try {
        CompletableFuture<Void> future = s3StorageManager
            .getMajescoOrgAsync(majescoParty.getS3Key())
            .thenAccept(org -> {
                if (org == null) {
                    logger.error("Error fetching organization for party {}", majescoParty.getId());
                    return;
                }
                synchronized (organizations) {
                    organizations.add(org);
                }
                synchronized (parties) {
                    parties.add(orgTransformer.execute(org));
                }
                logger.info("Added org for party {}", majescoParty.getId());
            })
            .exceptionally(ex -> {
                logger.error("Async error fetching org for party {}", majescoParty.getId(), ex);
                return null;
            });

        orgFutures.add(future);  // ✅ collect futures here

    } catch (Exception ex) {
        logger.error("Invalid s3 path {}", majescoParty.getS3Key(), ex);
        throw new RuntimeException("Invalid s3 path " + majescoParty.getS3Key(), ex);
    }
}

// ✅ Ensure all async work completes before returning
CompletableFuture.allOf(orgFutures.toArray(new CompletableFuture[0])).join();


project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>demo</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.3</version>
        <relativePath/>
    </parent>

    <dependencies>
        <!-- Web -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <!-- Feign -->
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-openfeign</artifactId>
        </dependency>

        <!-- OAuth2 -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-oauth2-client</artifactId>
        </dependency>

        <!-- Lombok -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
    </dependencies>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.cloud</groupId>
                <artifactId>spring-cloud-dependencies</artifactId>
                <version>2023.0.1</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>

package com.example.demo.config;

import feign.RequestInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;

@Configuration
public class OAuthFeignConfig {

    @Value("${oauth2.client-id}")
    private String clientId;

    @Value("${oauth2.client-secret}")
    private String clientSecret;

    @Value("${oauth2.token-url}")
    private String tokenUrl;

    @Value("${oauth2.scope}")
    private String scope;

    private String cachedToken;
    private Instant expiryTime;

    private final RestTemplate restTemplate = new RestTemplate();

    @Bean
    public RequestInterceptor requestInterceptor() {
        return template -> {
            String token = getValidToken();
            template.header("Authorization", "Bearer " + token);
        };
    }

    private synchronized String getValidToken() {
        if (cachedToken == null || Instant.now().isAfter(expiryTime)) {
            cachedToken = fetchNewToken();
        }
        return cachedToken;
    }

    private String fetchNewToken() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("grant_type", "client_credentials");
        body.add("client_id", clientId);
        body.add("client_secret", clientSecret);
        body.add("scope", scope);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);

        ResponseEntity<TokenResponse> response = restTemplate.exchange(
                tokenUrl, HttpMethod.POST, request, TokenResponse.class);

        if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
            TokenResponse tokenResponse = response.getBody();
            cachedToken = tokenResponse.getAccessToken();
            expiryTime = Instant.now().plusSeconds(tokenResponse.getExpiresIn() - 60);
            System.out.println("Fetched new OAuth2 token, valid until: " + expiryTime);
            return cachedToken;
        } else {
            throw new IllegalStateException("Failed to fetch OAuth2 token: " + response.getStatusCode());
        }
    }
}
package com.example.demo.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TokenResponse {
    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("expires_in")
    private int expiresIn;
}
package com.example.demo.feign;

import com.example.demo.model.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(
    name = "webClient",
    url = "${external.api.url}",
    configuration = com.example.demo.config.OAuthFeignConfig.class
)
public interface WebClient {
    @GetMapping("/your-api-path") // Replace with your actual API path
    ResponseEntity<ApiResponse> getMappings();
}

package com.example.demo.controller;

import com.example.demo.feign.WebClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MappingController {

    private final WebClient webClient;

    public MappingController(WebClient webClient) {
        this.webClient = webClient;
    }

    @GetMapping("/mappings")
    public Object getMappings() {
        return webClient.getMappings().getBody().getRecord().getMappings();
    }
}
package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ApiResponse {
    @JsonProperty("record")
    private Record record;
}

package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;

@Data
public class Record {
    @JsonProperty("mappings")
    private List<Mapping> mappings;
}

package com.example.demo.model;

import lombok.Data;

@Data
public class Mapping {
    private String mpgName;
    private String mpgDesc;
    private String strtDt;
    private String endDt;
    private String srcSysCd;
    private String srcSysMpgCdNm;
    private String srcSysMpgCdVal;
    private String srcSysMpgCdValDesc;
    private String tgtSysCd;
    private String tgtSysMpgCdNm;
    private String tgtSysMpgCdVal;
    private String tgtSysMpgCdValDesc;
    private String community;
    private String domain;
    private String sts;
}

external.api.url=



oauth2.client-id=YOUR_CLIENT_ID
oauth2.client-secret=YOUR_CLIENT_SECRET
oauth2.token-url=
oauth2.scope=YOUR_SCOPE
