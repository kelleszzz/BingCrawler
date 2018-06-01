package com.kelles.crawler.crawler.dataanalysis.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.kelles.crawler.crawler.database.weightdb.WeightInterface;
import com.kelles.crawler.crawler.util.*;

public class Journal implements Serializable,WeightInterface {
	
	private String name=null;
	private Set<String> profiles=new HashSet();
	private int weight=DEFAULT_WEIGHT;
	public static final int DEFAULT_WEIGHT=0;

	private Map<String,Integer> profilesCitedBy=new HashMap(); //引用数最高的论文
	private Map<String,Integer> keywords=new HashMap(); //所有涉及的关键词(按包含该关键词的论文数排序,依赖关键词分析)
	private Map<String,Integer> authors=new HashMap(); //发表文章最多的作者(依赖作者分析)
	private Map<Integer,Integer> years=new HashMap(); //发表年代
	
	
	public static final int topCount=10; //列举前10个
	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		sb.append("[期刊或会议名]\r\n"+name+"\r\n");
		sb.append("[爬取论文数]\r\n"+profiles.size()+"\r\n");
		sb.append(Util.formatTopMapStr(profilesCitedBy,"引用最多的论文","引用数",topCount));
		sb.append(Util.formatTopMapStr(keywords,"涉及领域","总引用数",topCount));
		sb.append(Util.formatTopMapStr(authors,"影响力最大的作者","引用数",topCount));
		if (!years.isEmpty()){
			sb.append("[发表年代统计]\r\n");
			List<Integer> keys=new ArrayList(years.keySet());
			Collections.sort(keys);
			for (int key:keys)
				sb.append(key+"年"+"发表了"+years.get(key)+"部作品"+"\r\n");
		}
		return sb.toString();
	}
	
	
	public Map<String, Integer> getAuthors() {
		return authors;
	}
	public void setAuthors(Map<String, Integer> authors) {
		this.authors = authors;
	}
	public Map<String, Integer> getProfilesCitedBy() {
		return profilesCitedBy;
	}
	public void setProfilesCitedBy(Map<String, Integer> profilesCitedBy) {
		this.profilesCitedBy = profilesCitedBy;
	}
	public Map<String, Integer> getKeywords() {
		return keywords;
	}
	public void setKeywords(Map<String, Integer> keywords) {
		this.keywords = keywords;
	}
	public Map<Integer, Integer> getYears() {
		return years;
	}


	public void setYears(Map<Integer, Integer> years) {
		this.years = years;
	}


	public Set<String> getProfiles() {
		return profiles;
	}
	public void setProfiles(Set<String> profiles) {
		this.profiles = profiles;
	}
	public Journal(String name) {
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
