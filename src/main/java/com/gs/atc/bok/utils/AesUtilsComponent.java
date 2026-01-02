package com.gs.atc.bok.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AesUtilsComponent {
    private static final Logger log = LoggerFactory.getLogger(AesUtilsComponent.class);

    @Value("${dec.key}")
    private String llave;

    @Value("${dec.iv}")
    private String vector;

    @Value("${dec.AESKEY}")
    private static String aeskey;

    @Value("${dec.ALGORITHM}")
    private static String algoritmo;

    private static final int GCM_IV_LENGTH = 12;
    private static final SecureRandom secureRandom = new SecureRandom();

    public String encrypt(String mensaje) {
        try {
            byte[] decodedKey;
            decodedKey = Base64.getDecoder().decode(llave);
            SecretKey secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
            byte[] iv = new byte[GCM_IV_LENGTH]; // NEVER REUSE THIS IV WITH SAME KEY
            byte[] associatedData = vector.getBytes(StandardCharsets.UTF_8);
            secureRandom.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv); // 128 bit auth tag length
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            cipher.updateAAD(associatedData);
            byte[] cipherText = cipher.doFinal(mensaje.getBytes(StandardCharsets.UTF_8));
            ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + cipherText.length);
            byteBuffer.put(iv);
            byteBuffer.put(cipherText);
            return Base64.getEncoder().encodeToString(byteBuffer.array());
        } catch (Exception ex) {
            log.error("Incidente al cifrar el parametro {} Message {} -cause {} :", mensaje, ex.getMessage(),
                    ex.getCause());
            return mensaje;
        }
    }

    public String decryptAesGcm(String encryptmensaje) {
        try {
            byte[] decodedKey;
            decodedKey = Base64.getDecoder().decode(llave);
            SecretKey secretKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
            byte[] cipherMessage = Base64.getDecoder().decode(encryptmensaje);
            byte[] associatedData = vector.getBytes(StandardCharsets.UTF_8);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            // use first 12 bytes for iv
            AlgorithmParameterSpec gcmIv = new GCMParameterSpec(128, cipherMessage, 0, GCM_IV_LENGTH);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmIv);
            cipher.updateAAD(associatedData);
            // use everything from 12 bytes on as ciphertext
            byte[] plainText = cipher.doFinal(cipherMessage, GCM_IV_LENGTH, cipherMessage.length - GCM_IV_LENGTH);

            return  new String(plainText, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            log.error("Incidente al descifrar el parametro {} Message {} -cause {} :", encryptmensaje, ex.getMessage(),
                    ex.getCause());
            return encryptmensaje;
        }
    }

}