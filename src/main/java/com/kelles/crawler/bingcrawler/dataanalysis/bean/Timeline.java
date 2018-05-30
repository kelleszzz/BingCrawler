package com.kelles.crawler.bingcrawler.dataanalysis.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightInterface;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;

public class Timeline implements Serializable,WeightInterface{
	
	private Set<String> profiles=new HashSet();
	
	int year=0; //年份
	int totalCitedBy=0; //论文总引用数
	int totalLinkedCitedBy=0; //(有记录可查询的)论文总引用数
	Map<String,Integer> profileCitedBy=new HashMap(); //当年引用量最高的论文
	Map<String,Integer> keywordsTotal=new HashMap(); //各个关键词发表的论文数
	Map<String,Integer> authorsTotal=new HashMap(); //各个作者发表的论文数
	
	
	
	private static final int topCount=10;
	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("[年份]\r\n"+year+"\r\n");
		if (profiles.size()>0) sb.append("[论文数]\r\n"+profiles.size()+"\r\n");
		if (totalCitedBy>0) 
			sb.append("[总引用量,平均引用量]\r\n"+totalCitedBy+" , "+totalCitedBy/profileCitedBy.size()+"\r\n");
		if (totalLinkedCitedBy>0) 
			sb.append("[(有记录可查询的)引用量,平均引用量]\r\n"+totalLinkedCitedBy+" , "+totalLinkedCitedBy/profiles.size()+"\r\n");
		if (!profileCitedBy.isEmpty()) sb.append(Utils.formatTopMapStr(profileCitedBy, "年度引用量最高的论文", "次数",topCount));
		if (!keywordsTotal.isEmpty()) sb.append(Utils.formatTopMapStr(keywordsTotal, "领域相关论文数", "次数",topCount));
		if (!keywordsTotal.isEmpty()) sb.append(Utils.formatTopMapStr(keywordsTotal, "年度发表论文最多的作者", "次数",topCount));
		return sb.toString();
	}



	public Map<String, Integer> getProfileCitedBy() {
		return profileCitedBy;
	}



	public void setProfileCitedBy(Map<String, Integer> profileCitedBy) {
		this.profileCitedBy = profileCitedBy;
	}



	public Set<String> getProfiles() {
		return profiles;
	}



	public void setProfiles(Set<String> profiles) {
		this.profiles = profiles;
	}



	public int getYear() {
		return year;
	}



	public void setYear(int year) {
		this.year = year;
	}






	public int getTotalCitedBy() {
		return totalCitedBy;
	}



	public void setTotalCitedBy(int totalCitedBy) {
		this.totalCitedBy = totalCitedBy;
	}



	public int getTotalLinkedCitedBy() {
		return totalLinkedCitedBy;
	}



	public void setTotalLinkedCitedBy(int totalLinkedCitedBy) {
		this.totalLinkedCitedBy = totalLinkedCitedBy;
	}



	public Map<String, Integer> getKeywordsTotal() {
		return keywordsTotal;
	}



	public void setKeywordsTotal(Map<String, Integer> keywordsTotal) {
		this.keywordsTotal = keywordsTotal;
	}



	public Map<String, Integer> getAuthorsTotal() {
		return authorsTotal;
	}



	public void setAuthorsTotal(Map<String, Integer> authorsTotal) {
		this.authorsTotal = authorsTotal;
	}



	public Timeline(int year) {
		super();
		this.year = year;
	}



	@Override
	public int getWeight() {
		return year;
	}



	@Override
	public void setWeight(int weight) {
		this.year=weight;
	}
	
	
	
}
