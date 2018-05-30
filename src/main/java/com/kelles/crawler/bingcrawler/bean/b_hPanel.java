package com.kelles.crawler.bingcrawler.bean;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.util.TextUtils;

public class b_hPanel implements Serializable{
	private String label;
	private List<Map<String,String>> snippets=new ArrayList(); //<"snippet","url">
	
	public void addSnippet(String snippet,String url){
		if (TextUtils.isEmpty(snippet)) return;
		Map<String,String> map=new HashMap();
		map.put("snippet", snippet);
		if (!TextUtils.isEmpty(url)) map.put("url", url);
		snippets.add(map);
	}
	
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public List<Map<String, String>> getSnippets() {
		return snippets;
	}
	public void setSnippets(List<Map<String, String>> snippets) {
		this.snippets = snippets;
	}

	@Override
	public String toString() {
		return "b_hPanel [label=" + label + ", snippets=" + snippets + "]";
	}
	
	
	
}
