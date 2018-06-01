package com.kelles.crawler.crawler.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.kelles.crawler.crawler.setting.Constant;
import org.apache.http.util.TextUtils;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.nodes.TagNode;
import org.htmlparser.util.NodeList;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import com.kelles.crawler.crawler.util.*;
import com.kelles.crawler.crawler.bean.*;

public class BingAnalysisUtils {
	
	/*是否是必应学术页面*/
	public static boolean isBingAcademicSearchUrl(java.lang.String url){
		java.lang.String hostUrl= Util.getHostUrl(url);
		if (hostUrl.contains("bing") && url.contains("academic/search")) return true;
		return false;

	}
	public static boolean isBingAcademicProfileUrl(java.lang.String url){
		java.lang.String hostUrl= Util.getHostUrl(url);
		if (hostUrl.contains("bing") && url.contains("academic/profile")) return true;
		return false;
	}
	
	
	/*<div id="tab_1_3C78B5" data-appns="SERP" data-k="5135.1" class="" style="transition: opacity 0.3s linear; opacity: 1;">
	从中获取<a target="_blank" href="http://www.dmi.unict.it/mpavone/nc-cs/materiale/NSGA-II.pdf" h="ID=SERP,5131.1">*/
	public static List<java.lang.String> analyze_downloads_div(Node divNode){
		List<java.lang.String> urls=new ArrayList();
		Node spanNode= Util.extractOneNodeThatMatch(divNode, new NodeFilter(){
			@Override
			public boolean accept(Node arg0) {
				if (arg0.getText().startsWith("span")
						&& !arg0.getText().contains("aca_doc cipl")) return true;
				return false;
			}
		});
		NodeList itemsList=spanNode.getChildren();
		for (int i=0;i<itemsList.size();i++){
			Node itemNode=itemsList.elementAt(i);
			//<a target="_blank" href="http://www.dmi.unict.it/mpavone/nc-cs/materiale/NSGA-II.pdf" h="ID=SERP,5131.1">
			if (itemNode.getText().startsWith("a")
					&& itemNode.getText().contains("target=\"_blank\"")){
				Pattern pat=Pattern.compile("a.+href=\"(?<link>.+?)\"");
				Matcher m=pat.matcher(itemNode.getText());
				if (m.find()) urls.add(m.group("link"));
			}
		}
		return urls;
	}
	
	
	/* 从<div class="mpage" id="aca_r|cpaper">分析出
	References 或 Cited Papers*/
	public static List<Profile> analyze_aca_paper(Node aca_paper){
		NodeList baseList=aca_paper.getChildren();
		List<Profile> profiles=new ArrayList();
		if (baseList!=null) for (int p=0;p<baseList.size();p++){
			Node baseNode=baseList.elementAt(p);
			//<tr data-exp="H;;;;;;">
			if (baseNode.getText().startsWith("tr")
					&& baseNode.getText().contains("data-exp=\"H;;;;;;\"")){
				NodeList childrenList=baseNode.getChildren();
				Profile profile=new Profile();
				profiles.add(profile);
				for (int i=0;i<childrenList.size();i++){
					Node childNode=childrenList.elementAt(i);
					//<li>
					if (childNode.getText().startsWith("li")){
						//<a href="/academic/search?q=Comparison+of+Multiobjective+Evolutionary+Algorithms%3a+Empirical+Results&amp;mkt=zh-cn" h="ID=morepage.1_1,5025.1">Comparison of Multiobjective Evolutionary Algorithms: Empirical Results</a>
						Node aNode= Util.extractOneNodeThatMatch(childNode, new NodeFilter(){
							@Override
							public boolean accept(Node arg0) {
								if (arg0.getText().startsWith("a")
										&& arg0.getText().contains("href")) return true;
								return false;
							}
						});
						if (aNode!=null){
							/*引用文章的标题及链接*/
							profile.setTitle(aNode.toPlainTextString().trim());
							java.lang.String url=null;
							Pattern pat=Pattern.compile("a.+href=\"(?<link>.+?)\"");
							Matcher m=pat.matcher(aNode.getText());
							if (m.find()) profile.setUrl(m.group("link"));
						}
						else{
							 /*<li>Eckart Zitzler · Kalyanmoy Deb · Lothar Thiele</li>
							引用文章的作者*/
							java.lang.String[] authors=childNode.toPlainTextString().split("·");
							b_hPanel authorsPanel=new b_hPanel();
							authorsPanel.setLabel("Authors");
							for (java.lang.String author:authors)
								authorsPanel.addSnippet(author.trim(), null);
							profile.getPanels().add(authorsPanel);
						}
					}
					else Util.addChildrenNodesToNodeList(childNode, childrenList);
				}
			}
			else Util.addChildrenNodesToNodeList(baseNode, baseList);
		}
		return profiles;
	}

	/*分析b_hPanel 如:Authors
	Kalyanmoy Deb, Amrit Pratap, Sameer Agarwal, T Meyarivan*/
	public static b_hPanel analyze_b_hPanel(Node baseNode){		
		b_hPanel panel=new b_hPanel();
		NodeList nodeList=baseNode.getChildren();
		for (int p=0;p<nodeList.size();p++){
			Node node=nodeList.elementAt(p);
			//<span class="aca_labels">
			if (node.getText().startsWith("span")
					&& node.getText().contains("class=\"aca_labels\"")){
				panel.setLabel(node.toPlainTextString().trim()); //标签,即Authors
			}
			//<span class="aca_content">
			else if (node.getText().startsWith("span")
					&& node.getText().contains("class=\"aca_content\"")){
				//通用分析
				NodeList snippetList=new NodeList();
				node.collectInto(snippetList, new NodeFilter(){
					@Override
					public boolean accept(Node snippetNode) {
						//找到最小的TagNode
						if (snippetNode instanceof TagNode){
							NodeList snippetChildrenList=snippetNode.getChildren();
							if (snippetChildrenList!=null)
								for (int i=0;i<snippetChildrenList.size();i++){
									Node snippetChildNode=snippetChildrenList.elementAt(i);
									if (snippetChildNode instanceof TagNode) return false;
								}
							else return false;
							return true;
						}
						return false;
					}
				});
				for (int i=0;i<snippetList.size();i++){
					Node snippetNode=snippetList.elementAt(i);
					java.lang.String url=null;
					if (snippetNode.getText().startsWith("a")){
						Pattern pat=Pattern.compile("a.+href=\"(?<link>.+?)\"");
						Matcher m=pat.matcher(snippetNode.getText());
						if (m.find()) url=m.group("link");
					}
					panel.addSnippet(snippetNode.toPlainTextString().trim(), url);
				}
			}
			else Util.addChildrenNodesToNodeList(node,nodeList);
		}
		return panel;
	}
	
	public static void downloadsLoadMore(WebDriver driver){
		WebElement aca_source=null;
		WaitForWebElementToChange wait=null;
		try{
			aca_source=driver.findElement(By.className("aca_source"));
			//是否存在Download & Source 
			boolean hasDownload=false,hasSource=false;
			try{
				/*<div class="tab-head">*/
				WebElement tab_head=aca_source.findElement(By.className("tab-head"));
				List<WebElement> tabs=tab_head.findElements(By.tagName("li"));
				for (WebElement tab:tabs){
					if (Constant.Download.equals(tab.getText())) hasDownload=true;
					else if (Constant.Source.equals(tab.getText())) hasSource=true;
				}
			}
			catch(NoSuchElementException e){}
			//加载更多
//			Logger.log(11.26,"[Download默认版面加载更多]"); //
			downloadsLoadMoreSml(aca_source);
			if (hasDownload && hasSource){
				//切换Tab至Source
//				Logger.log(11.26,"[切换Tab至Source]"); //
				wait=WaitForWebElementToChange.newInstance(driver, By.className("aca_source"));
				downloadsLoadMoreTab(aca_source, Constant.Source);
					wait.waitForChange(1000);
				aca_source=driver.findElement(By.className("aca_source")); //重新加载一次aca_source
				//加载更多
//				Logger.log(11.26,"[Source版面加载更多]"); //
				downloadsLoadMoreSml(aca_source);
				//切换回Download
//				Logger.log(11.26,"[切换Tab回Download]"); //
				wait=WaitForWebElementToChange.newInstance(driver, By.className("aca_source"));
				downloadsLoadMoreTab(aca_source, Constant.Download);
				wait.waitForChange(1000);
			}
		}catch(NoSuchElementException e){return;} 
	}
	
	//在Download和Source之间跳转,不存在对应Tab时返回false
	private static void downloadsLoadMoreTab(WebElement aca_source, java.lang.String tabText){
		//<div class="tab-head">
		try{
			WebElement tab_head=aca_source.findElement(By.className("tab-head"));
			//<li data-dataurl="" data-nc="" class="" style="width: 50%;" data-w="50" data-ow="80" role="tab" data-content="tab_2_29AEB8" data-tabindex="1" data-bm="5">Source</li>
			List<WebElement> clickItems=tab_head.findElements(By.tagName("li"));
			for (WebElement clickItem:clickItems){
				if (tabText.equals(clickItem.getText().trim())){
					if (clickItem.isDisplayed()) {
						clickItem.click();
						break;
					}
				}
				
			}
			
		}catch(NoSuchElementException e){}
	}
	
	//点击Expand,没有Expand按钮时返回false
	private static void downloadsLoadMoreSml(WebElement aca_source){
		boolean ifLoadMore;
		do{
			//<div class="tab-content">
			WebElement tab_content=aca_source.findElement(By.className("tab-content"));
			ifLoadMore=false;
			//<div class="sml" id="expitem_-938946284_7" data-appns="SERP" data-k="5260.2" data-expl="">
			List<WebElement> smls=tab_content.findElements(By.className("sml"));
			for (WebElement sml:smls){
				if (!sml.isDisplayed()) continue;
				try{
					//<a id="expitem_-938946284_7_hit" class="leftAlign" href="javascript:void(0);" aria-expanded="false" role="button">
					WebElement aExpand=sml.findElement(By.linkText(Constant.Expand));
					if (aExpand.isDisplayed() && sml.isDisplayed()) {
						ifLoadMore=true;
						sml.click();
						break;
					}
				}catch(NoSuchElementException e){}
			}
		}while(ifLoadMore);
	}
	
	public static void introductionLoadMore(WebDriver driver){
		WebElement aca_main=null;
		try{
			aca_main=driver.findElement(By.className("aca_main"));
		}catch(NoSuchElementException e){return;}
		
		//<div class="sml inline" id="expitem_-499075098_6" data-appns="SERP" data-k="5162.1" data-expl="">
		List<WebElement> inlineElements=aca_main.findElements(By.tagName("div"));
		for (WebElement inlineElement:inlineElements){
			if ("sml inline".equals(inlineElement.getAttribute("class"))){
				try{
					//<a id="expitem_-499075098_6_hit" class="leftAlign" href="javascript:void(0);" aria-label="Show more" aria-expanded="false" role="button">
					WebElement showMoreElement=inlineElement.findElement(By.tagName("a"));
					if (showMoreElement.isDisplayed() && "Show more".equals(showMoreElement.getAttribute("aria-label")))
						showMoreElement.click();
				}catch(NoSuchElementException e){}
			}
			else continue;
		}
	}
	
	/*<div class="acap_tab_header" data-tabid="aca_citation">
	点击CitedPapers,加载更多文章*/
	public static void papersLoadMore(WebDriver driver) {
		WebElement acapp_papers=null;
		try{
			acapp_papers=driver.findElement(By.id("acapp_papers"));
			/*是否存在References & Cited Papers*/
			boolean hasReferences=false,hasCitedPapers=false;
			try{
				List<WebElement> acap_module_seps=acapp_papers.findElements(By.className("acap_module_sep"));
				for (WebElement acap_module_sep:acap_module_seps){
					List<WebElement> acap_tab_headers=acap_module_sep.findElements(By.tagName("div"));
					for (WebElement acap_tab_header:acap_tab_headers){
						java.lang.String header_class=acap_tab_header.getAttribute("class");
						if (header_class!=null
								&& header_class.startsWith("acap_tab_header")){
							if (acap_tab_header.getText().trim().contains(Constant.References)) hasReferences=true;
							else if (acap_tab_header.getText().trim().contains(Constant.Cited_Papers)) hasCitedPapers=true;
						}
					}
					if (hasReferences||hasCitedPapers) break;
				}
			}catch(NoSuchElementException e){}
			//点击默认版面Load More
//			Logger.log(11.26,"[点击默认版面Load More]");//
			papersLoadMoreSml(acapp_papers,driver);
			if (hasReferences && hasCitedPapers){
				//切换至Cited Papers
//				Logger.log(11.26,"[切换至Cited Papers]");//
				papersLoadMoreTab(acapp_papers, Constant.Cited_Papers);
				acapp_papers=driver.findElement(By.id("acapp_papers"));//重新加载一次acapp_papers
				//点击Load More
//				Logger.log(11.26,"[点击Cited Papers的LoadMore]"); //
				papersLoadMoreSml(acapp_papers,driver);
				//切换回References
//				Logger.log(11.26,"[切换回References]");//
				papersLoadMoreTab(acapp_papers, Constant.References);
			}
		}catch(NoSuchElementException e){return;}
	}
	
	//在References和Cited Papers之间跳转
	private static void papersLoadMoreTab(WebElement acapp_papers, java.lang.String tabText) {
		List<WebElement> acap_tab_headers=acapp_papers.findElements(By.tagName("div"));
		for (WebElement acap_tab_header:acap_tab_headers){
			 /*<div class="acap_tab_header" data-tabid="aca_reference">
			 或<div class="acap_tab_header acap_tab_active" data-tabid="aca_citation">*/
			if (!TextUtils.isEmpty(acap_tab_header.getAttribute("class")) 
					&& acap_tab_header.getAttribute("class").startsWith("acap_tab_header")
					&& tabText.equals(acap_tab_header.getText())){
				acap_tab_header.click();
			}
		}
	}
	
	//点击LoadMore
	private static void papersLoadMoreSml(WebElement acapp_papers,WebDriver driver) {
		boolean ifLoadMore;
		WaitForWebElementToChange wait=null;
		do{
			try{
				//<div class="sml" id="expitem_-939102536_4" data-appns="SERP" data-k="5245.2" data-expl="">
				List<WebElement> smls=acapp_papers.findElements(By.className("sml"));
				ifLoadMore=false;
				for (WebElement sml:smls){
					if (!sml.isDisplayed()) continue;
					try{
						//<a id="expitem_-939102536_4_hit" class="leftAlign" href="javascript:void(0);" aria-expanded="false" role="button">Load More</a>
						WebElement aLoadMore=sml.findElement(By.linkText(Constant.Load_More));
						if (aLoadMore.isDisplayed() && sml.isDisplayed()){
							//点击后加载需时间
							wait=WaitForWebElementToChange.newInstance(driver, By.id("acapp_papers"));
							sml.click();
							wait.waitForChange(1000);
							ifLoadMore=true;
							break;
						}
					}catch(NoSuchElementException e){}
				}
		   }
		   //找不到Load More按钮
		   catch(NoSuchElementException e){ifLoadMore=false;}
		} while(ifLoadMore);
	}
	
}
