package com.kelles.crawler.bingcrawler.dataanalysis.bean;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightInterface;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;

public class Keyword implements Serializable,WeightInterface{
	private int weight=DEFAULT_WEIGHT;
	private String name=null; //关键词名
	public static final int DEFAULT_WEIGHT=0;
	
	private int totalLinkedCitedBy=0; //(有记录可查询)包含该关键字论文总引用数
	private Set<String> profiles=new HashSet(); //包含该关键字的论文,按照引用数排序
	private Map<String,Integer> profilesCitedBy=new HashMap(); //包含该关键字的论文引用数
	private Map<Integer,Integer> years=new HashMap(); //包含该关键词的论文的发表年代
	private Map<String,Integer> journals=new HashMap(); //包含该关键词的论文的会议(按包含的论文数排序)
	private Map<String,Integer> relatedKeywords=new HashMap(); //包含该关键词的论文中，包含的其它关键词,以相关度形式表现(按照包含的论文数排序)
	Map<String,Integer> authors=new HashMap(); //该关键词下最有影响力的作者(按引用总数排序,依赖作者分析)
	
	
	/*获取包含该关键词的论文总引用数*/
	public int getTotalCitedBy(){
		int totalCitedBy=0;
		for (String key:profilesCitedBy.keySet())
			totalCitedBy+=profilesCitedBy.get(key);
		return totalCitedBy;
	}
	
	public static final int topCount=10;
	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder(),sbProfiles=new StringBuilder();
		sb.append("[领域]\r\n"+name+"\r\n");
		int totalCitedBy=0;
		if (profiles.size()>0){
			sb.append("[爬取论文数]\r\n"+profiles.size()+"\r\n");
			if (!profilesCitedBy.isEmpty()) sb.append(Utils.formatTopMapStr(profilesCitedBy, "引用数最高的论文", "次数",topCount));
		}
		if (totalCitedBy>0){
			sb.append("[论文总引用数]\r\n"+totalCitedBy+"\r\n");
		}
		if (totalLinkedCitedBy>0){
			sb.append("[(有记录可查询的)总引用数]\r\n"+totalLinkedCitedBy+"\r\n");
		}
		if (!years.isEmpty()){
			sb.append("[发表年代统计]\r\n");
			List<Integer> keys=new ArrayList(years.keySet());
			Collections.sort(keys);
			for (int key:keys)
				sb.append(key+"年"+"发表了"+years.get(key)+"部作品"+"\r\n");
		}
		if (!journals.isEmpty()){
			sb.append(Utils.formatTopMapStr(journals, "期刊会议统计", "发表论文数",topCount));
		}
		if (!authors.isEmpty()){
			sb.append(Utils.formatTopMapStr(authors, "影响力最大的作者","引用数",topCount));
		}
		if (!relatedKeywords.isEmpty()){
			int curCount=0;
			sb.append("[关联领域(top"+topCount+")]\r\n");
			List<String> keys=new ArrayList(relatedKeywords.keySet());
			Collections.sort(keys,new Comparator<String>(){
				@Override
				public int compare(String o1, String o2) {
					int i1=relatedKeywords.get(o1),i2=relatedKeywords.get(o2);
					if (i1>i2) return -1;
					else if (i1<i2) return 1;
					else return 0;
				}
			});
			for (String key:keys){
				if ((++curCount)>topCount) break;
				DecimalFormat df=new DecimalFormat("#.##");  
				double percentage=relatedKeywords.get(key)/(double)profiles.size();
				if (percentage>1) percentage=1;
				sb.append(key+"(关联度"+df.format(percentage*100)+"%)\r\n");
			}
		}
		sb.append(sbProfiles);
		return sb.toString();
	}
	
	
	public Map<String, Integer> getJournals() {
		return journals;
	}
	public void setJournals(Map<String, Integer> journals) {
		this.journals = journals;
	}
	public Map<Integer, Integer> getYears() {
		return years;
	}
	public void setYears(Map<Integer, Integer> years) {
		this.years = years;
	}
	public Keyword(String name) {
		super();
		this.name = name;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	public String getName() {
		return name;
	}
	public Map<String, Integer> getAuthors() {
		return authors;
	}

	public void setAuthors(Map<String, Integer> authors) {
		this.authors = authors;
	}

	public void setName(String name) {
		this.name = name;
	}
	public int getTotalLinkedCitedBy() {
		return totalLinkedCitedBy;
	}
	public void setTotalLinkedCitedBy(int totalLinkedCitedBy) {
		this.totalLinkedCitedBy = totalLinkedCitedBy;
	}
	public Set<String> getProfiles() {
		return profiles;
	}
	public void setProfiles(Set<String> profiles) {
		this.profiles = profiles;
	}
	public Map<String, Integer> getProfilesCitedBy() {
		return profilesCitedBy;
	}
	public void setProfilesCitedBy(Map<String, Integer> profilesCitedBy) {
		this.profilesCitedBy = profilesCitedBy;
	}


	public Map<String, Integer> getRelatedKeywords() {
		return relatedKeywords;
	}


	public void setRelatedKeywords(Map<String, Integer> relatedKeywords) {
		this.relatedKeywords = relatedKeywords;
	}
	

	
	
	
	
	

}
