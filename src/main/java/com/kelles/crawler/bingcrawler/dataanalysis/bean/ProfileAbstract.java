package com.kelles.crawler.bingcrawler.dataanalysis.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.kelles.crawler.bingcrawler.setting.Constant;
import org.apache.http.util.TextUtils;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightInterface;
import com.kelles.crawler.bingcrawler.bean.*;

public class ProfileAbstract implements Serializable,WeightInterface{
	private int weight=DEFAULT_WEIGHT;
	public static final int DEFAULT_WEIGHT=0;
	
	private java.lang.String title=null; //文章标题
	private List<b_hPanel> panels=null; //作者,摘要,会议
	
	
	
	@Override
	public java.lang.String toString() {
		StringBuilder sb=new StringBuilder();
		List<java.lang.String> strList=null;
		java.lang.String str=null;
		sb.append("[标题]\r\n"+title+"\r\n");
		strList=getAuthors();
		if (strList!=null && strList.size()>0){
			sb.append("[作者]\r\n");
			for (java.lang.String author:strList)
				sb.append(author+"\r\n");
		}
		strList=getKeywords();
		if (strList!=null && strList.size()>0){
			sb.append("[领域]\r\n");
			for (java.lang.String keyword:strList)
				sb.append(keyword+"\r\n");
		}
		str=getYear();
		if (str!=null && !TextUtils.isEmpty(str))
			sb.append("[年份]\r\n"+str+"\r\n");
		str=getJournal();
		if (str!=null && !TextUtils.isEmpty(str))
			sb.append("[期刊会议]\r\n"+str+"\r\n");
		str=getCitedBy();
		if (str!=null && !TextUtils.isEmpty(str))
			sb.append("[引用量]\r\n"+str+"\r\n");
		str=getDOI();
		if (str!=null && !TextUtils.isEmpty(str))
			sb.append("[DOI]\r\n"+str+"\r\n");
		return sb.toString();
	}

	/*仅可通过其它Profile构造*/
	public ProfileAbstract(Profile profile) {
		super();
		this.title=profile.getTitle();
		this.panels=profile.getPanels();
	}
	
	//获取作者名
	public List<java.lang.String> getAuthors(){
		return getPanelItems(Constant.Authors,SnippetType.snippet);
	}
	
	//获取作者链接
	public List<java.lang.String> getAuthorsUrl(){
		return getPanelItems(Constant.Authors,SnippetType.url);
	}
	
	//获取摘要(有多个snippet时合并在一起)
	public java.lang.String getIntroduction(){
		List<java.lang.String> items=getPanelItems(Constant.Introduction,SnippetType.snippet);
		if (items!=null && items.size()>0) {
			java.lang.String result="";
			for (java.lang.String item:items) result+=item+" ";
			return result;
		}
		return null;
	}
	
	//获取关键字
	public List<java.lang.String> getKeywords(){
		return getPanelItems(Constant.Keywords,SnippetType.snippet);
	}
	
	//获取关键字链接
	public List<java.lang.String> getKeywordsUrl(){
		return getPanelItems(Constant.Keywords,SnippetType.url);
	}
	
	//获取年份
	public java.lang.String getYear(){
		List<java.lang.String> items=getPanelItems(Constant.Year,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取会议
	public java.lang.String getJournal(){
		List<java.lang.String> items=getPanelItems(Constant.Journal,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取会议链接
	public java.lang.String getJournalUrl(){
		List<java.lang.String> items=getPanelItems(Constant.Journal,SnippetType.url);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取卷号
	public java.lang.String getVolumn(){
		List<java.lang.String> items=getPanelItems(Constant.Volume,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取期号
	public java.lang.String getIssue(){
		List<java.lang.String> items=getPanelItems(Constant.Issue,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取页码范围
	public java.lang.String getPages(){
		List<java.lang.String> items=getPanelItems(Constant.Pages,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取被引量
	public java.lang.String getCitedBy(){
		List<java.lang.String> items=getPanelItems(Constant.Cited_By,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取DOI
	public java.lang.String getDOI(){
		List<java.lang.String> items=getPanelItems(Constant.DOI,SnippetType.snippet);
		if (items!=null && items.size()>0) return items.get(0);
		return null;
	}
	
	//获取某个标签下的元素,如Authors,Keywords,Journal,不存在返回null
	private enum SnippetType{snippet,url}
	private List<java.lang.String> getPanelItems(java.lang.String label, SnippetType type){
		java.lang.String typeStr=null;
		if (type==SnippetType.snippet) typeStr="snippet";
		else if (type==SnippetType.url) typeStr="url";
		for (b_hPanel panel:panels)
			if (label.equals(panel.getLabel())){
				List<java.lang.String> urls=new ArrayList();
				for (Map<java.lang.String, java.lang.String> map:panel.getSnippets())
					if (map.containsKey(typeStr)) urls.add(map.get(typeStr));
				return urls;
			}
		return null;
	}
	
	public int getWeight() {
		return weight;
	}
	public void setWeight(int weight) {
		this.weight = weight;
	}

	public java.lang.String getTitle() {
		return title;
	}

	public void setTitle(java.lang.String title) {
		this.title = title;
	}
	
}
