package com.kelles.crawler.bingcrawler.dataanalysis.bean;

import java.io.Serializable;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.util.TextUtils;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightInterface;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;

public class ProfileSimHash implements Serializable,WeightInterface{
	private int weight=DEFAULT_WEIGHT;
	public static final int DEFAULT_WEIGHT=0;
	
	private String title=null;
	private BigInteger simHash=null;
	private Map<String,Integer> distances=new HashMap();
	
	private static final int topCount=10;
	@Override
	public String toString() {
		StringBuilder sb=new StringBuilder();
		List<String> strList=null;
		String str=null;
		sb.append("[标题]\r\n"+title+"\r\n");
		if (!distances.isEmpty()){
			sb.append("[相似度最高的论文(top"+topCount+")]\r\n");
			int curCount=0;
			List<String> keys=new ArrayList(distances.keySet());
			Collections.sort(keys,new Comparator<String>(){
				@Override
				public int compare(String o1, String o2) {
					int i1=distances.get(o1),i2=distances.get(o2);
					if (i1>i2) return -1;
					else if (i1<i2) return 1;
					else return 0;
				}
			});
			for (String key:keys){
				if ((curCount++)>=topCount || distances.get(key)<0) break;
				DecimalFormat df=new DecimalFormat("#.##");  
				double percentage=distances.get(key)/(double)TextAnalysis.totalBits;
				if (percentage>1) percentage=1;
				sb.append(key+"(关联度"+df.format(percentage*100)+"%)\r\n");
			}
		}
		return sb.toString();
	}
	
	public Map<String, Integer> getDistances() {
		return distances;
	}
	public void setDistances(Map<String, Integer> distances) {
		this.distances = distances;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public BigInteger getSimHash() {
		return simHash;
	}
	public void setSimHash(BigInteger simHash) {
		this.simHash = simHash;
	}
	public ProfileSimHash(String title) {
		super();
		this.title = title;
	}
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}
	
	
}
