package com.kelles.crawler.crawler.bean;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;

import com.kelles.crawler.crawler.util.Md5;
import com.kelles.crawler.crawler.util.Util;

public class CrawlUrl implements Serializable{
	
	public final static int DEFAULT_WEIGHT=100;
	
	private String url;
	private String md5;
	private BigInteger simHash; //html内容的simHash(抓取正文,提取关键词,得到SimHash码)
	private int depth=0; //深度
	private int weight=DEFAULT_WEIGHT; //根据这个值从todoUrls中取出
	private int statusCode;
	private Map<String,String> messages=new HashMap<>(); //信息
	private Set<String> urlsReferTo=new HashSet();
	private Set<String> urlsReferedTo=new HashSet();

    public Map<String, String> getMessages() {
        return messages;
    }

    public void setMessages(Map<String, String> messages) {
        this.messages = messages;
    }

    public int getDepth() {
		return depth;
	}

	public void setDepth(int depth) {
		this.depth = depth;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getMd5() {
		return md5;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public Set<String> getUrlsReferTo() {
		return urlsReferTo;
	}

	public void setUrlsReferTo(Set<String> urlReferTo) {
		this.urlsReferTo = urlReferTo;
	}

	public Set<String> getUrlsReferedTo() {
		return urlsReferedTo;
	}

	public void setUrlsReferedTo(Set<String> urlReferedTo) {
		this.urlsReferedTo = urlReferedTo;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public CrawlUrl(String url) {
		this.url = Util.removeSuffix(url);
		if (this.url!=null) this.md5= Md5.get(this.url);
	}
	
	public BigInteger getSimHash() {
		return simHash;
	}

	public void setSimHash(BigInteger simHash) {
		this.simHash = simHash;
	}

	@Override
	public String toString() {
		return "CrawlUrl [url=" + url + ", md5=" + md5 + ", simHash=" + simHash + ", depth=" + depth + ", statusCode=" + statusCode + ", urlsReferTo=" + urlsReferTo + ", urlsReferedTo="
				+ urlsReferedTo + "]"+", \nweight="
						+ weight ;
	}







	
	
	
}
