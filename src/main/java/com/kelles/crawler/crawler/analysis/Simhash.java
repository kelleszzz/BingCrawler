package com.kelles.crawler.crawler.analysis;

import com.kelles.crawler.crawler.setting.Setting;
import com.sun.org.apache.xerces.internal.xs.StringList;
import org.junit.Assert;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class Simhash {
    protected List<String> tokens;
    protected final static Charset DEFAULT_CHARSET = Setting.DEFAULT_CHARSET;
    //通过MD5算法获取的hash值是128位
    protected final static int SIGN_BITS = 128;

    public Simhash(String content) {
        List<String> tokens = new ArrayList<>();
        for (int i = 0; i < content.length(); i++) {
            tokens.add(content.substring(i, i + 1));
        }
        this.tokens = tokens;
    }

    public Simhash(List<String> tokens) {
        if (tokens == null) throw new NullPointerException("tokens cannot be null");
        this.tokens = tokens;
    }

    public String getSign() {
        StringBuilder sb = new StringBuilder();
        int[] sign = getSign(tokens);
        if (sign == null) return null;
        Assert.assertTrue(sign.length == SIGN_BITS);
        for (int i = 0; i < sign.length; i++) {
            if (sign[i] > 0) {
                sb.append("1");
            } else {
                sb.append("0");
            }
        }
        return sb.toString();
    }

    public int getHammingDistance(Simhash another) {
        if (another == null) return -1;
        return getHammingDistance(getSign(tokens), getSign(another.tokens));
    }

    protected static int[] getSign(List<String> tokens) {
        if (tokens == null || tokens.size() == 0) return null;
        int[] sign = new int[SIGN_BITS];
        for (int i = 0; i < sign.length; i++) sign[i] = 0;
        for (String token : tokens) {
            byte[] hash = getHash(token);
            Assert.assertTrue(hash.length == SIGN_BITS / 8);
            mergeBytes(sign, hash);
        }
        return sign;
    }

    /**
     * 通过MD5算法返回128位签名
     *
     * @param token
     * @return
     */
    protected static byte[] getHash(String token) {
        if (token == null) return null;
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("md5");
            if (messageDigest == null) return null;
            byte[] hash = messageDigest.digest(token.getBytes(DEFAULT_CHARSET));
            Assert.assertTrue(hash.length == SIGN_BITS / 8);
            return hash;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    protected static void mergeBytes(int[] sign, byte[] hash) {
        if (sign == null || hash == null) return;
        Assert.assertTrue(sign.length / 8 == hash.length);
        for (int i = 0; i < hash.length * 8; i++) {
            int b = hash[i / 8] & (1 << (i % 8));
            sign[i] += b > 0 ? 1 : -1;
        }
    }

    protected static int getHammingDistance(int[] signSrc, int[] signDst) {
        if (signSrc == null || signDst == null || signSrc.length != signDst.length) return -1;
        int hammingDistance = 0;
        for (int i = 0; i < signSrc.length; i++) {
            int bSrc = signSrc[i] > 0 ? 1 : 0;
            int bDst = signDst[i] > 0 ? 1 : 0;
            if (bSrc != bDst) hammingDistance++;
        }
        return hammingDistance;
    }

    public List<String> getTokens() {
        return tokens;
    }
}
