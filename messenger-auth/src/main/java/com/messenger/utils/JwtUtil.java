package com.messenger.utils;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;

public class JwtUtil {
    private static final Logger logger = LoggerFactory.getLogger(JwtUtil.class);
    private static final Config config = ConfigFactory.load();
    private static final String SECRET = config.getString("jwt.secret");
    private static final long LIFE_TIME = config.getLong("jwt.life_time");

    private static final Algorithm ALGORITHM = Algorithm.HMAC256(SECRET);

    private static final JWTVerifier VERIFIER = JWT.require(ALGORITHM).build();

    public static boolean isValid(String token) {
        if (token == null || token.isBlank()) {
            return false;
        }
        try {
            VERIFIER.verify(token);
            return true;
        } catch (JWTVerificationException e) {
            logger.error("JWT verification failed: {}", e.getMessage());
            return false;
        }
    }

    public static String createToken(String username) {
        return JWT.create()
                .withSubject(username)
                .withExpiresAt(Date.from(Instant.now().plusMillis(LIFE_TIME)))
                .sign(ALGORITHM);
    }

    public static String updateToken(String token) {
        String username = getSubject(token);
        return createToken(username);
    }

    public static DecodedJWT getDecodedJWT(String token) {
        return VERIFIER.verify(token);
    }

    public static String getSubject(String token) {
        return VERIFIER.verify(token).getSubject();
    }

}
