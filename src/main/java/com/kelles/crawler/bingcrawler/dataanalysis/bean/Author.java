package com.kelles.crawler.bingcrawler.dataanalysis.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightInterface;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;

public class Author implements Serializable,WeightInterface {
	private String name=null;
	private Set<String> profiles=new HashSet();
	private int weight=DEFAULT_WEIGHT;
	public static final int DEFAULT_WEIGHT=0;

	int totalLinkedCitedBy=0,averageLinkedCitedBy=0; //记录在案的引用论文数和平均引用论文数
	int totalCitedBy=0,averageCitedBy=0; //总引用量和平均引用量
	Set<String> keywords=new HashSet(); //作品关键词
	Map<Integer,Integer> years=new TreeMap(); //作品年代分布<2002,3>2002年写了三部作品
	Set<String> co_authors=new HashSet(); //合作过的作者
	Set<String> journals=new HashSet(); //刊登过的期刊

	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("[作者]\r\n"+name+"\r\n");
		if (profiles.size()>0){
			sb.append("[作品]\r\n");
			for (String profileStr:profiles)
				sb.append(profileStr+"\r\n");
		}
		if (totalCitedBy>0||averageCitedBy>0){
			sb.append("[总引用量和平均引用量]\r\n"+totalCitedBy+","+averageCitedBy+"\r\n");
		}
		if (totalLinkedCitedBy>0||averageLinkedCitedBy>0){
			sb.append("[(有记录可查询的)总引用量和平均引用量]\r\n"+totalLinkedCitedBy+","+averageLinkedCitedBy+"\r\n");
		}
		if (keywords.size()>0){
			sb.append("[作品领域]\r\n");
			for (String keyword:keywords)
				sb.append(keyword+"\r\n");
		}
		if (!years.isEmpty()){
			sb.append("[发表年代分布]\r\n");
			Set<Integer> keys=years.keySet();
			for (int key:keys)
				sb.append(key+"年"+"发表了"+years.get(key)+"部作品"+"\r\n");
		}
		if (co_authors.size()>0){
			sb.append("[合作作者]\r\n");
			for (String co_author:co_authors)
				if (!name.equals(co_author))
					sb.append(co_author+",");
			sb.delete(sb.length()-1, sb.length());
			sb.append("\r\n");
		}
		if (journals.size()>0){
			sb.append("[发表期刊会议]\r\n");
			for (String journal:journals)
				sb.append(journal+"\r\n");
		}
		return sb.toString();
	}
	
	
	
	
	public int getTotalLinkedCitedBy() {
		return totalLinkedCitedBy;
	}




	public void setTotalLinkedCitedBy(int totalLinkedCitedBy) {
		this.totalLinkedCitedBy = totalLinkedCitedBy;
	}




	public int getAverageLinkedCitedBy() {
		return averageLinkedCitedBy;
	}




	public void setAverageLinkedCitedBy(int averageLinkedCitedBy) {
		this.averageLinkedCitedBy = averageLinkedCitedBy;
	}




	public int getTotalCitedBy() {
		return totalCitedBy;
	}




	public void setTotalCitedBy(int totalCitedBy) {
		this.totalCitedBy = totalCitedBy;
	}




	public int getAverageCitedBy() {
		return averageCitedBy;
	}




	public void setAverageCitedBy(int averageCitedBy) {
		this.averageCitedBy = averageCitedBy;
	}




	public Set<String> getKeywords() {
		return keywords;
	}




	public void setKeywords(Set<String> keywords) {
		this.keywords = keywords;
	}




	public Map<Integer, Integer> getYears() {
		return years;
	}




	public void setYears(Map<Integer, Integer> years) {
		this.years = years;
	}




	public Set<String> getCo_authors() {
		return co_authors;
	}




	public void setCo_authors(Set<String> co_authors) {
		this.co_authors = co_authors;
	}




	public Set<String> getJournals() {
		return journals;
	}




	public void setJournals(Set<String> journals) {
		this.journals = journals;
	}




	public Set<String> getProfiles() {
		return profiles;
	}
	public void setProfiles(Set<String> profiles) {
		this.profiles = profiles;
	}
	public Author(String name) {
		super();
		this.name = name;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	
}
