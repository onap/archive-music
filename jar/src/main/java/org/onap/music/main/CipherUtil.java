/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */

package org.onap.music.main;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Scanner;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.onap.music.eelf.logging.EELFLoggerDelegate;

public class CipherUtil {


    /**
     * Default key.
     */
    private static String keyString = null;

    private static final String ALGORITHM = "AES";
    private static final String ALGORYTHM_DETAILS = ALGORITHM + "/CBC/PKCS5PADDING";
    private static final int BLOCK_SIZE = 128;
    @SuppressWarnings("unused")
    private static SecretKeySpec secretKeySpec;
    private static IvParameterSpec ivspec;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CipherUtil.class);
    /**
     * @deprecated Please use {@link #encryptPKC(String)} to encrypt the text.
     * 
     *             Encrypts the text using the specified secret key.
     * 
     * @param plainText
     *            Text to encrypt
     * @param secretKey
     *            Key to use for encryption
     * @return encrypted version of plain text.
     * @
     *             if any encryption step fails
     *
     */
    @Deprecated
    public static String encrypt(String plainText, String secretKey)  {
        String encryptedString = null;
        try {
            byte[] encryptText = plainText.getBytes("UTF-8");
            byte[] rawKey = Base64.decodeBase64(secretKey);
            SecretKeySpec sKeySpec = new SecretKeySpec(rawKey, "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, sKeySpec);
            encryptedString = Base64.encodeBase64String(cipher.doFinal(encryptText));
        } catch (BadPaddingException | IllegalBlockSizeException | InvalidKeyException | NoSuchAlgorithmException
                | NoSuchPaddingException | UnsupportedEncodingException ex) {
        }
        return encryptedString;
    }

    /**
     * @deprecated Please use {@link #encryptPKC(String)} to encrypt the text.
     *             Encrypts the text using the secret key in key.properties file.
     * 
     * @param plainText
     *            Text to encrypt
     * @return Encrypted Text
     * @
     *             if any decryption step fails
     */
    @Deprecated
    public static String encrypt(String plainText)  {
        return CipherUtil.encrypt(plainText, keyString);
    }

    /**
     * Encrypts the text using a secret key.
     * 
     * @param plainText
     *            Text to encrypt
     * @return Encrypted Text
     * @
     *             if any decryption step fails
     */
    public static String encryptPKC(String plainText)  {
        return CipherUtil.encryptPKC(plainText, keyString);
    }

    /**
     * 
     * @deprecated Please use {@link #decryptPKC(String)} to Decryption the text.
     * 
     *             Decrypts the text using the specified secret key.
     * 
     * @param encryptedText
     *            Text to decrypt
     * @param secretKey
     *            Key to use for decryption
     * @return plain text version of encrypted text
     * @
     *             if any decryption step fails
     * 
     */
    @Deprecated
    public static String decrypt(String encryptedText, String secretKey)  {
        String encryptedString = null;
        try {
            byte[] rawKey = Base64.decodeBase64(secretKey);
            SecretKeySpec sKeySpec = new SecretKeySpec(rawKey, "AES");
            byte[] encryptText = Base64.decodeBase64(encryptedText.getBytes("UTF-8"));
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, sKeySpec);
            encryptedString = new String(cipher.doFinal(encryptText));
        } catch (BadPaddingException | IllegalBlockSizeException | InvalidKeyException | NoSuchAlgorithmException
                | NoSuchPaddingException | UnsupportedEncodingException ex) {
        }
        return encryptedString;
    }

    private static SecretKeySpec getSecretKeySpec() {
        byte[] key = Base64.decodeBase64(keyString);
        return new SecretKeySpec(key, ALGORITHM);
    }

    private static SecretKeySpec getSecretKeySpec(String keyString) {
        byte[] key = Base64.decodeBase64(keyString);
        return new SecretKeySpec(key, ALGORITHM);
    }

    /**
     * Encrypt the text using the secret key in key.properties file
     * 
     * @param value
     * @return The encrypted string
     * @throws BadPaddingException
     * @
     *             In case of issue with the encryption
     */
    public static String encryptPKC(String value, String skey)  {
        Cipher cipher = null;
        byte[] iv = null, finalByte = null;

        try {
            cipher = Cipher.getInstance(ALGORYTHM_DETAILS, "SunJCE");

            SecureRandom r = SecureRandom.getInstance("SHA1PRNG");
            iv = new byte[BLOCK_SIZE / 8];
            r.nextBytes(iv);
            ivspec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, getSecretKeySpec(skey), ivspec);
            finalByte = cipher.doFinal(value.getBytes());

        } catch (Exception ex) {
            
        }
        return Base64.encodeBase64String(ArrayUtils.addAll(iv, finalByte));
    }

    /**
     * Decrypts the text using the secret key in key.properties file.
     * 
     * @param message
     *            The encrypted string that must be decrypted using the ecomp
     *            Encryption Key
     * @return The String decrypted
     * @
     *             if any decryption step fails
     */
    public static String decryptPKC(String message, String skey)  {
        byte[] encryptedMessage = Base64.decodeBase64(message);
        Cipher cipher;
        byte[] decrypted = null;
        try {
            cipher = Cipher.getInstance(ALGORYTHM_DETAILS, "SunJCE");
            ivspec = new IvParameterSpec(ArrayUtils.subarray(encryptedMessage, 0, BLOCK_SIZE / 8));
            byte[] realData = ArrayUtils.subarray(encryptedMessage, BLOCK_SIZE / 8, encryptedMessage.length);
            cipher.init(Cipher.DECRYPT_MODE, getSecretKeySpec(skey), ivspec);
            decrypted = cipher.doFinal(realData);

        } catch (Exception ex) {
            
            
        }

        return new String(decrypted);
    }

    /**
     * @deprecated Please use {@link #decryptPKC(String)} to Decrypt the text.
     * 
     *             Decrypts the text using the secret key in key.properties file.
     * 
     * @param encryptedText
     *            Text to decrypt
     * @return Decrypted text
     * @
     *             if any decryption step fails
     */
    @Deprecated
    public static String decrypt(String encryptedText)  {
        return CipherUtil.decrypt(encryptedText, keyString);
    }

    /**
     * 
     * Decrypts the text using the secret key in key.properties file.
     * 
     * @param encryptedText
     *            Text to decrypt
     * @return Decrypted text
     * @
     *             if any decryption step fails
     */
    public static String decryptPKC(String encryptedText)  {
        return CipherUtil.decryptPKC(encryptedText, keyString);
    }

    
    public static void readAndSetKeyString() {
        try {
            Scanner in = new Scanner(new FileReader("/opt/app/music/etc/properties.txt"));
            StringBuilder sb = new StringBuilder();
            while(in.hasNext()) {
                sb.append(in.next());
            }
            in.close();
            keyString = sb.toString();
        } catch (FileNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
        }
    }

    /*public static void main(String[] args)  {

        System.out.println("Encrypted password: "+encryptPKC("cassandra"));

        System.out.println("Decrypted password: "+decryptPKC("dDhqAp5/RwZbl9yRSZg15fN7Qul9eiE/JFkKemtTib0="));
        System.out.println("Decrypted password: "+decryptPKC("I/dOtD/YYzBStbtOYhKuUUyPHSW2G9ZzdSyB8bJp4vk="));
        System.out.println("Decrypted password: "+decryptPKC("g7zJqg74dLsH/fyL7I75b4eySy3pbMS2xVqkrB5lDl8="));
    }*/

}
