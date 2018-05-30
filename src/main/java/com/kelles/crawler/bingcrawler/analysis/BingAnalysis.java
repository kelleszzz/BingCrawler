package com.kelles.crawler.bingcrawler.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.kelles.crawler.bingcrawler.setting.Constant;
import org.apache.http.util.TextUtils;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.lexer.Lexer;
import org.htmlparser.nodes.TagNode;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;

public class BingAnalysis {

	public static void main(java.lang.String[] args) throws Exception{
		//静态初始化服务
		RemoteDriver.startService();
		//初始化一个浏览器,且不加载图片
		RemoteDriver remoteDriver=new RemoteDriver(false);
		//获取创建的WebDriver
		WebDriver driver=remoteDriver.getDriver();
		//分析Search
		java.lang.String html=CommonAnalysis.seleniumGetHtml("http://cn.bing.com/academic/search?q=artificial+intelligence+papers&qs=ACA&pq=artifi&sk=ACA1&sc=8-6&sp=2&cvid=0125092B09FA499983E88895FB73C17F&FORM=QBAR", driver);
		List<Profile> profileUrls=analyzeBingAcademicSearch(html);
		System.out.println("[搜索到链接]\n"+profileUrls.get(1));
		//分析Profile
//		html=seleniumGetBingAcademicProfileHtml("http://cn.bing.com/academic/profile?id=2126105956&encoded=0&v=paper_preview&mkt=zh-cn#",driver);
		html=seleniumGetBingAcademicProfileHtml("http://cn.bing.com/academic/profile?id=2340633806&encoded=0&v=paper_preview&mkt=zh-cn", driver);
//		html=seleniumGetBingAcademicProfileHtml("https://www.bing.com/academic/profile?id=2064920134&encoded=0&v=paper_preview&mkt=zh-cn", driver);
		if (html!=null) CommonAnalysis.strToFile(Util.htmlFormatting(html),"BingAnalysisTest","BingAnalysis.html");
		Profile profile=analyzeBingAcademicProfile(html);
		System.out.println("[分析出Profile]\n"+profile);
		//关闭这个浏览器
		remoteDriver.quitDriver();
		//静态关闭服务
		RemoteDriver.stopService();
	}
		
	
	/*分析必应学术的一条具体条目(Profile)*/
	public static Profile analyzeBingAcademicProfile(java.lang.String html){
		try{
			Profile profile=new Profile();
			Parser parser=new Parser(new Lexer(html));
			NodeList baseList=parser.parse(null);
			for (int p=0;p<baseList.size();p++){
				Node baseNode=baseList.elementAt(p);
				//标题,<li class="aca_title">
				if (baseNode.getText().startsWith("li")
						&& baseNode.getText().contains("class=\"aca_title\"")){
					profile.setTitle(baseNode.toPlainTextString().trim()); //标题
				}
				//作者,年代,会议等<li class="aca_main">
				else if (baseNode.getText().startsWith("li")
						&& baseNode.getText().contains("class=\"aca_main\"")){
					NodeList panelList=baseNode.getChildren();
					for (int p1=0;p1<panelList.size();p1++){
						Node panelNode=panelList.elementAt(p1);
						//<div class="b_hPanel">
						if (panelNode.getText().startsWith("div")
								&& panelNode.getText().contains("class=\"b_hPanel\"")){
							b_hPanel panel=BingAnalysisUtils.analyze_b_hPanel(panelNode);
							profile.getPanels().add(panel);
						}
						else Util.addChildrenNodesToNodeList(panelNode,panelList);
					}
				}
				/*<li id="acapp_papers">
				References & Cited Papers*/
				else if (baseNode.getText().startsWith("li")
						&& baseNode.getText().contains("id=\"acapp_papers\"")){
					NodeList papersList=baseNode.getChildren();
					for (int p1=0;p1<papersList.size();p1++){
						Node paperNode=papersList.elementAt(p1);
						 /*<div class="mpage" id="aca_rpaper">
						 References*/
						if (paperNode.getText().startsWith("div")
								&& paperNode.getText().contains("id=\"aca_rpaper\"")){
							List<Profile> referencesProfiles=
									BingAnalysisUtils.analyze_aca_paper(paperNode);
							profile.getReferences().addAll(referencesProfiles);
						}
						/*<div class="mpage" id="aca_cpaper">
						Cited Papers*/
						else if (paperNode.getText().startsWith("div")
								&& paperNode.getText().contains("id=\"aca_cpaper\"")){
							List<Profile> citedPapersProfiles=
									BingAnalysisUtils.analyze_aca_paper(paperNode);
							profile.getCitedPapers().addAll(citedPapersProfiles);
						}
						else Util.addChildrenNodesToNodeList(paperNode, papersList);
					}
				}
				/*<div class="aca_source">
				Download & Source*/
				else if (baseNode.getText().startsWith("div")
						&& baseNode.getText().contains("class=\"aca_source\"")){
					boolean hasDownload=false,hasSource=false;
					NodeList itemsList=baseNode.getChildren();
					for (int p1=0;p1<itemsList.size();p1++){
						Node itemsNode=itemsList.elementAt(p1);
						/*<div class="tab-head">*/
						if (itemsNode.getText().startsWith("div")
								&& itemsNode.getText().contains("class=\"tab-head\"")){
							NodeList liList=itemsNode.getChildren();
							for (int p2=0;p2<liList.size();p2++){
								Node liNode=liList.elementAt(p2);
								/*<li data-dataurl="" data-nc="" class="tab-active tab-first" style="width: 50px;" data-w="50" data-ow="80" tabindex="0" role="tab" data-content="tab_1_A489AD" data-tabindex="0" data-bm="4">Source</li>*/
								if (liNode.getText().startsWith("li")){
									if (Constant.Download.equals(liNode.toPlainTextString().trim())) hasDownload=true;
									else if (Constant.Source.equals(liNode.toPlainTextString().trim())) hasSource=true;
								}
								else Util.addChildrenNodesToNodeList(liNode, liList);
							}
						}
						/*<div id="tab_1_3C78B5" data-appns="SERP" data-k="5135.1" class="" style="transition: opacity 0.3s linear; opacity: 1;">
						Download*/
						else if (itemsNode.getText().startsWith("div")
								&& itemsNode.getText().contains("id=\"tab_1")){
							if (hasDownload && hasSource){
								List<java.lang.String> downloadUrls=BingAnalysisUtils.analyze_downloads_div(itemsNode);
								profile.getDownloadUrls().addAll(downloadUrls);
							}
							else if (!hasDownload && hasSource){
								List<java.lang.String> sourceUrls=BingAnalysisUtils.analyze_downloads_div(itemsNode);
								profile.getSourceUrls().addAll(sourceUrls);
							}
						}
						/*<div id="tab_2_3C78B5" class="tab-hide" data-appns="SERP" data-k="5147.1" style="opacity: 0; transition: opacity 0.3s linear;">
						Source*/
						else if (itemsNode.getText().startsWith("div")
								&& itemsNode.getText().contains("id=\"tab_2")){
							List<java.lang.String> sourceUrls=BingAnalysisUtils.analyze_downloads_div(itemsNode);
							profile.getSourceUrls().addAll(sourceUrls);
						}
						else Util.addChildrenNodesToNodeList(itemsNode, itemsList);
					}
				}
				else Util.addChildrenNodesToNodeList(baseNode,baseList);
			}
			return profile;
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
		
	
	//使用Selenium库获取必应学术具体条目的html文本
	private static final int TIMEOUT=5000;
	private static final int POLLING_EVERY=500;
	@SuppressWarnings("unchecked")
	public static java.lang.String seleniumGetBingAcademicProfileHtml(java.lang.String url, WebDriver driver){
		java.lang.String html="";
		driver.navigate().to(url); //跳转
		
		//初始加载等待
		Wait wait=new FluentWait(driver)
				.ignoring(NoSuchElementException.class)
				.withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
				.pollingEvery(POLLING_EVERY, TimeUnit.MILLISECONDS);
		
		
		/*<div id="b_content">
		初步加载,此时可能aca_cpaper & aca_rpaper未加载完全*/
		try{
			WebElement contentElement=(WebElement) wait.until(new WebDriverFunction(){
				@Override
				public WebElement apply(WebDriver driver) {
					WebElement contentElement=driver.findElement(By.tagName("html"));
					if (!TextUtils.isEmpty(contentElement.getText())) {
						int contentTextLength=contentElement.getText().length();
						if (contentTextLength!=arg0){
							arg0=contentTextLength;
							arg1=0;
							throw new NoSuchElementException("页面内容仍在更新中");
						}
						else if (contentTextLength==arg0 && arg1==0){
							arg1=1;
							throw new NoSuchElementException("页面内容未变化,再等待更新一次");
						}
						else return contentElement;
					}
					else throw new NoSuchElementException("页面内容为空");
				}
			});
		}catch(TimeoutException e){e.printStackTrace();}
		
		/*<li class="aca_main">
		加载Introduction*/
		try{
			BingAnalysisUtils.introductionLoadMore(driver);
		}catch(WebDriverException e){
			e.printStackTrace();
		}
		/*<li id="acapp_papers">
		References & Cited Papers*/
		try{
			BingAnalysisUtils.papersLoadMore(driver);
		}catch(WebDriverException e){
			e.printStackTrace();
		}
		 /*<div class="aca_source">
		 Downloads & Source*/
		try{
			BingAnalysisUtils.downloadsLoadMore(driver);
		}catch(WebDriverException e){
			e.printStackTrace();
		}
		WebElement htmlElement=driver.findElement(By.tagName("html"));
		html=htmlElement.getAttribute("outerHTML");
		return html;
	}
	
	
	
	
	//分析必应学术的搜索链接,获取具体条目的url
	public static java.lang.String BING_PREFIX="http://www.bing.com";
	public static List<Profile> analyzeBingAcademicSearch(java.lang.String html){
		try {
			Parser parser=new Parser(new Lexer(html));
			NodeList nodeList=parser.parse(null);
			//找寻<ol id="b_results" role="main" aria-label="Search Results">
			for (int p=0;p<nodeList.size();p++){
				Node node=nodeList.elementAt(p);
				if (node.getText().startsWith("ol")
						&& node.getText().contains("id=\"b_results\"") 
						&& node.getText().contains("aria-label=\"Search Results\"")){
					//找到了条目列表
					List<Profile> paperProfiles=new ArrayList();
					NodeList paperList=node.getChildren();
					for (int i=0;i<paperList.size();i++){
						Node paperNode=paperList.elementAt(i);
						//找寻<li class="aca_algo">
						if (paperNode.getText().startsWith("li")
								&& paperNode.getText().contains("class=\"aca_algo\"")){
							Profile paperProfile=new Profile();
							//找到了一则条目,分析url
							NodeList detailList=paperNode.getChildren();
							for (int p1=0;p1<detailList.size();p1++){
								Node detailNode=detailList.elementAt(p1);
								/*找寻 <h2 class="">,获取文章标题及url*/
								if (detailNode.getText().startsWith("h2")){
									paperProfile.setTitle(detailNode.toPlainTextString().trim()); //标题
									Pattern pa=Pattern.compile("a.+href=\"(?<link>.+?)\"");
									Matcher m=pa.matcher(detailNode.toHtml());
									if (m.find()) {
										paperProfile.setUrl(convertHtmlCharEntity(
												BING_PREFIX+m.group("link"))); //具体内容链接
									}
								}
								/*<div class="aca_caption">*/
								else if (detailNode.getText().startsWith("div")
										&& detailNode.getText().contains("class=\"aca_caption\"")){
									NodeList snippetsList=detailNode.getChildren();
									for (int p2=0;p2<snippetsList.size();p2++){
										Node snippetNode=snippetsList.elementAt(p2);
										/*<div class="caption_author">*/
										if (snippetNode.getText().startsWith("div")
												&& snippetNode.getText().contains("class=\"caption_author\"")){
											b_hPanel panel=new b_hPanel();
											panel.setLabel(Constant.Authors);
											
											NodeList aList=snippetNode.getChildren();
											for (int p3=0;p3<aList.size();p3++){
												Node aNode=aList.elementAt(p3);
												if (!TextUtils.isEmpty(aNode.getText().trim()) && !"·".equals(aNode.getText().trim())){
													if (aNode instanceof TagNode && aNode.getText().startsWith("a")){
														java.lang.String author=aNode.toPlainTextString().trim(),authorUrl=null;
														Pattern pat1=Pattern.compile("^a.+href=\"(?<link>.+?)\"");
														Matcher mat1=pat1.matcher(aNode.getText());
														if (mat1.find()) authorUrl=mat1.group("link");
														panel.addSnippet(author, authorUrl);
													}
													else{
														java.lang.String[] authorsRawStr=aNode.toPlainTextString().split("·");
														for (java.lang.String authorRawStr:authorsRawStr)
															if (!TextUtils.isEmpty(authorRawStr.trim()))
																panel.addSnippet(authorRawStr.trim(),null);
													}
												}
											}
											paperProfile.getPanels().add(panel);
										}
										/*<div class="caption_venue">*/
										else if (snippetNode.getText().startsWith("div")
												&& snippetNode.getText().contains("class=\"caption_venue\"")){
											/*2002 · IEEE Transactions on Evolutionary Computation|Cited by:19475
											1995|Cited by:25325
											2011
											2007 · 自然辩证法研究
											*/
											java.lang.String rawLine=snippetNode.toPlainTextString().trim();
											java.lang.String year=null,journal=null,journalUrl=null,citedBy=null;
											for (java.lang.String rawPart:rawLine.split("\\|")){
												rawPart=rawPart.trim();
												if (!rawPart.startsWith(Constant.Cited_By)){
													for (java.lang.String rawPart2:rawPart.split("·")){
														rawPart2=rawPart2.trim();
														Pattern pat=Pattern.compile("[0-9]{1,4}");
														Matcher mat=pat.matcher(rawPart2);
														if (mat.matches()){
															/*年份*/
															year=rawPart2;
															b_hPanel panel=new b_hPanel();
															panel.setLabel(Constant.Year);
															panel.addSnippet(year, null);
															paperProfile.getPanels().add(panel);
														}
														else{
															/*会议*/
															journal=rawPart2;
															final java.lang.String _journal=journal;
															Node journalUrlNode= Util.extractOneNodeThatMatch(snippetNode, new NodeFilter(){
																@Override
																public boolean accept(Node arg0) {
																	if (arg0.getText().startsWith("a") && arg0.toPlainTextString().trim().equals("_journal"))
																		return true;
																	return false;
																}
															});
															if (journalUrlNode!=null){
																Pattern pat1=Pattern.compile("^a.+href=\"(?<link>.+?)\"");
																Matcher mat1=pat1.matcher(journalUrlNode.getText());
																if (mat1.find()) journalUrl=mat1.group("link");
															}
															b_hPanel panel=new b_hPanel();
															panel.setLabel(Constant.Journal);
															panel.addSnippet(journal, journalUrl);
															paperProfile.getPanels().add(panel);
														}
													}
												}
												else{
													Pattern pat=Pattern.compile(Constant.Cited_By+":"+"(?<citedby>.*)");
													Matcher mat=pat.matcher(rawPart);
													if (mat.find()) {
														/*引用数*/
														citedBy=mat.group("citedby");
														b_hPanel panel=new b_hPanel();
														panel.setLabel(Constant.Cited_By);
														panel.addSnippet(citedBy, null);
														paperProfile.getPanels().add(panel);
													}
												}
											}
										}
										/*<div class="caption_abstract">*/
										else if (snippetNode.getText().startsWith("div")
												&& snippetNode.getText().contains("class=\"caption_abstract\"")){
											b_hPanel panel=new b_hPanel();
											panel.setLabel(Constant.Introduction);
											panel.addSnippet(snippetNode.toPlainTextString().trim(), null);
											paperProfile.getPanels().add(panel);
										}
										/*<div class="caption_field">*/
										else if (snippetNode.getText().startsWith("div")
												&& snippetNode.getText().contains("class=\"caption_field\"")){
											b_hPanel panel=new b_hPanel();
											panel.setLabel(Constant.Keywords);
											NodeList aList=snippetNode.getChildren();
											for (int i1=0;i1<aList.size();i1++){
												Node aNode=aList.elementAt(i1);
												if (!TextUtils.isEmpty(aNode.getText().trim())){
													java.lang.String keyword=aNode.toPlainTextString().trim(),keywordUrl=null;
													if (aNode instanceof TagNode && aNode.getText().startsWith("a")){
														Pattern pat2=Pattern.compile("^a.+href=\"(?<link>.+?)\"");
														Matcher mat2=pat2.matcher(aNode.getText());
														if (mat2.find()) keywordUrl=mat2.group("link");
													}
													panel.addSnippet(keyword, keywordUrl);
												}
												else Util.addChildrenNodesToNodeList(aNode, aList);
											}
											paperProfile.getPanels().add(panel);
										}
										else Util.addChildrenNodesToNodeList(snippetNode, snippetsList);
									}
								}
								else Util.addChildrenNodesToNodeList(detailNode, nodeList);
							}
							paperProfiles.add(paperProfile);
						}
					} 
					return paperProfiles;
				}
				//添加子节点到队列中,继续寻找
				NodeList childrenList=node.getChildren();
				if (childrenList!=null)
					for (int i=0;i<childrenList.size();i++) 
						nodeList.add(childrenList.elementAt(i));
			}
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;		
	}
		
		
	//转换如&amp; &nbsp; 等html字符实体
	private static java.lang.String convertHtmlCharEntity(java.lang.String url){
		url=url.replaceAll("&amp;", "&");
		url=url.replaceAll("&lt;", "<");
		url=url.replaceAll("&gt;", ">");
		url=url.replaceAll("&yen;", "¥");
		url=url.replaceAll("&cent;", "¢");
		url=url.replaceAll("&copy;", "©");
		url=url.replaceAll("&reg;", "®");
		url=url.replaceAll("&trade;", "™");
		return url;
	}

}
