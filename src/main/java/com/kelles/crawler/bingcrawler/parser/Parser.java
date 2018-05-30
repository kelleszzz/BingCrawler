package com.kelles.crawler.bingcrawler.parser;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import com.kelles.crawler.bingcrawler.setting.Setting;
import org.apache.http.HttpStatus;
import org.apache.http.util.TextUtils;
import org.openqa.selenium.NoSuchSessionException;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.kelles.crawler.bingcrawler.analysis.*;
import com.kelles.crawler.bingcrawler.database.*;
import com.kelles.crawler.bingcrawler.download.*;
import com.sleepycat.je.OperationStatus;


//爬取必应学术的Parser
public class Parser {
	
	private UrlsDbManager urlsManager=null;
	private DbManager<Profile> profilesManager=null;
	private RemoteDriver remoteDriver=null;
	private DownloadPool pool=null;
	
	public final static java.lang.String BING_PARSER_URLSDB_PATH= Setting.ROOT+"bing_urlsdb_home"; //Urls数据库文件夹路径
	public final static java.lang.String BING_PARSER_PROFILESDB_PATH= Setting.ROOT+"bing_profilesdb_home"; //Urls数据库文件夹路径
	public final static java.lang.String BING_PARSER_DOWNLOADPOOL_PATH= Setting.ROOT+"bing_downloadpool_home"; //下载线程池数据库地址
	public final static java.lang.String PROFILES_PATH= Setting.ROOT+"Profiles"; //下载论文文件夹
	public final static java.lang.String HTMLURLS_PATH= Setting.ROOT+"html_urls"; //下载html源代码文件夹
	
	public static void main(java.lang.String[] args){
		Parser parser=null;
		try{
			parser=new Parser();
			parser.addBingAcademicTheme("artificial intelligence papers", 1); //必应学术搜索第一页
			parser.menu();
		}
		catch (Exception e){e.printStackTrace();}
		finally{parser.close();}
    }
	
	
	/*菜单*/
	private Scanner scanner=new Scanner(System.in);
	private void menu() throws UnsupportedEncodingException{
		java.lang.String nextUrl=urlsManager.getNext();
		VersionUtils.log("[即将分析]"+URLDecoder.decode(nextUrl,"utf-8"));
		for (boolean jump=false;!jump;){
			VersionUtils.log("[菜单]任意字符->继续 | p->跳过 | db->查看数据库 | auto->自动爬取 | download->仅开始下载 | exit->退出"); //
			java.lang.String str=scanner.nextLine();
			if ("exit".equals(str)) {
				return;
			}
			else if ("p".equals(str)) {
				urlsManager.settleUrl(nextUrl, 0);
				jump=true;
			}
			else if ("db".equals(str)){
				for (;;){
					VersionUtils.log("[菜单]1->urls | 2->profiles | 3->downloads | 4->清除downloads | 其它字符->什么也不看"); //
					try{
						str=scanner.nextLine();
						if ("1".equals(str)) urlsManager.describe();
						else if ("2".equals(str)) profilesManager.describe();
						else if ("3".equals(str)) pool.describe();
						else if ("4".equals(str)) pool.getManager().clearDb();
						else break;
					}catch(Exception e){
						e.printStackTrace();
						break;
					}
				}
				jump=true;
			}
			else if ("auto".equals(str)){
				VersionUtils.log("输入自动爬取的链接数");
				str=scanner.nextLine();
				try{
					int auto=Integer.parseInt(str);
					for (int i=0;i<auto;i++){
						if ((nextUrl=urlsManager.getNext())!=null) {
							try {
								httpGet(nextUrl);
							} 
							/*这是紧急异常,表示浏览器会话失效*/
							catch(NoSuchSessionException e0){
								VersionUtils.log("[异常]"+Utils.getSystemTime());
								e0.printStackTrace();
								break;
							}
							catch (Exception e) {
								e.printStackTrace();
							}
						}
						else break;
					}
				}
				catch(NumberFormatException e1){VersionUtils.log("输入错误");}
				jump=true;
			}
			else if ("download".equals(str)){
				pool.tryStart();
			}
			else {
				try {
					httpGet(nextUrl);
				}
				/*这是紧急异常,表示浏览器会话失效*/
				catch(NoSuchSessionException e0){
					VersionUtils.log("[异常]"+Utils.getSystemTime());
					e0.printStackTrace();
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				jump=true;
			}
		}
		menu();
	}
	
	private void setup(){
		try{
			pool=new DownloadPool(BING_PARSER_DOWNLOADPOOL_PATH);
			pool.setMaxThreads(5); //同时进行5个下载任务
			profilesManager=new DbManager<Profile>(BING_PARSER_PROFILESDB_PATH,Profile.class);
			urlsManager=new UrlsDbManager(BING_PARSER_URLSDB_PATH);
			urlsManager.setMaxDepth(3); //深度
			RemoteDriver.startService();
			remoteDriver=new RemoteDriver(false);
		}catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	public void close(){
		pool.close();
		profilesManager.close();
		urlsManager.close();
		remoteDriver.stopService();
		RemoteDriver.stopService();
	}
	
	/*添加bing学术上指定的主题至todoUrls*/
	public void addBingAcademicTheme(java.lang.String theme, int page){
		java.lang.String prefix="http://cn.bing.com/academic/search?q=";
		java.lang.String suffix="&sort=1";
		try {
			java.lang.String url=prefix+URLEncoder.encode(theme, "utf-8")+"&first="+((page-1)*10+1)+suffix;
			java.lang.String html=CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
			if (html==null) throw new Exception("未能获取指定Bing学术搜索页面:"+url);
			//按照自定义规则保存html文本至文件
	        java.lang.String fileName=CommonAnalysis.urlGetFileName(URLDecoder.decode(url,"utf-8"));
	        File f=CommonAnalysis.strToFile(Utils.htmlFormatting(html),HTMLURLS_PATH,fileName);
	        //
			List<Profile> linkProfiles=BingAnalysis.analyzeBingAcademicSearch(html);
			if (linkProfiles!=null)
				for (Profile linkProfile:linkProfiles) {
					if (linkProfile.getUrl()!=null) {
						if (urlsManager.putUrl(linkProfile.getUrl())==OperationStatus.SUCCESS){
							VersionUtils.log("[添加论文链接]"+linkProfile); //
							urlsManager.updateWeight(linkProfile.getUrl(), CrawlUrl.DEFAULT_WEIGHT+10);
						}
					}
				}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch(Exception e1){
			e1.printStackTrace();
		}
	}
		
	
	/*不是必应学术时时分析html正文*/
	protected void commonAnalyzeHtml(java.lang.String html, java.lang.String url){
        //分析html
//		VersionUtils.log("正文内容:\n"+CommonAnalysis.getContent(html));
//		ParserUtils.showHammingDistances(url); //显示和所有uniUrl的海明距离
//		ParserUtils.showNodesInfo(html); //每个html结点查看
//      ParserUtils.readTorrent(html); //分析种子
	}


	/*返回true时,表示继续;返回false时,表示用户手动退出*/
	private void httpGet(java.lang.String url) throws Exception{
		if (TextUtils.isEmpty(url)) return;
		VersionUtils.log("[开始分析]"+URLDecoder.decode(url,"utf-8"));
		java.lang.String hostUrl=Utils.getHostUrl(url);
		/*判断是必应搜索还是必应Profile*/
		boolean isProfileUrl=false,isSearchUrl=false;
		isProfileUrl=BingAnalysisUtils.isBingAcademicProfileUrl(url);
		isSearchUrl=BingAnalysisUtils.isBingAcademicSearchUrl(url);
		/*获取Profile的html文本*/
		java.lang.String html=null;
		if (isProfileUrl) html=BingAnalysis.seleniumGetBingAcademicProfileHtml(url, remoteDriver.getDriver());
		else html=CommonAnalysis.seleniumGetHtml(url, remoteDriver.getDriver());
		/*添加url至uniUrls*/
		if (html==null) {
			VersionUtils.log("[无法访问]"+url);
			urlsManager.settleUrl(url,HttpStatus.SC_NOT_FOUND);
			return;
		}
		else urlsManager.settleUrl(url, HttpStatus.SC_OK); 
		/*按照自定义规则保存html文本至文件*/
        java.lang.String fileName=CommonAnalysis.urlGetFileName(URLDecoder.decode(url,"utf-8"));
        File f=CommonAnalysis.strToFile(Utils.htmlFormatting(html),HTMLURLS_PATH,fileName);
        /*分析html*/
        if (isProfileUrl){
        	/*提取Profile,保存至profilesDb*/
        	Profile profile=BingAnalysis.analyzeBingAcademicProfile(html);
        	profilesManager.put(profile.getTitle(), profile);
            VersionUtils.log("[读取文章]"+profile.getTitle()); //
            /*下载论文*/
            if (profile.getDownloadUrls().size()>0){
                DownloadTask.Builder taskBuilder=new DownloadTask.Builder();
                taskBuilder.setReferer(url);
                taskBuilder.toFile(PROFILES_PATH, Utils.replaceFileBadLetter(profile.getTitle()),".pdf");
                taskBuilder.addUrls(profile.getDownloadUrls());
                pool.addTask(taskBuilder.build());
            }
        	/*使用摘要生成SimHash*/
            urlsManager.updateSimHash(url, TextAnalysis.getSimHash(profile.getIntroduction()));
            /* 添加链接url至todoUrls*/
            List<Profile> linkProfiles=new ArrayList();
            if (profile.getReferences()!=null) linkProfiles.addAll(profile.getReferences());
            if (profile.getCitedPapers()!=null) linkProfiles.addAll(profile.getCitedPapers());
            for (Iterator it=linkProfiles.iterator();it.hasNext();){
            	Profile linkProfile=(Profile) it.next();
            	/*获取url*/
            	java.lang.String linkUrl=linkProfile.getUrl();
            	if (linkUrl==null) continue;
            	linkUrl=Utils.removeSuffix(linkUrl);
            	if (linkUrl.startsWith("/")) linkUrl=hostUrl+linkUrl;
            	/*仅添加必应学术url*/
            	if (BingAnalysisUtils.isBingAcademicSearchUrl(linkUrl)){
            		urlsManager.putUrl(linkUrl,url);
            		List<java.lang.String> messages=new ArrayList();
            		messages.add(linkProfile.getTitle());
            		if (linkProfile.getAuthors()!=null)
            			for (java.lang.String author:linkProfile.getAuthors())
        					messages.add(author);
            		urlsManager.updateMessages(linkUrl, messages);
            	}
            	else if (BingAnalysisUtils.isBingAcademicProfileUrl(linkUrl)){
            		urlsManager.putUrl(linkUrl,url);
            		urlsManager.updateWeight(linkUrl, CrawlUrl.DEFAULT_WEIGHT+1); /*提高优先级*/
            	}
            }
        }
        else if (isSearchUrl){
        	/*提取链接*/
        	List<Profile> linkProfiles=BingAnalysis.analyzeBingAcademicSearch(html);
        	/*使用链接摘要生成SimHash*/
        	StringBuilder content=new StringBuilder();
        	for (Profile linkProfile:linkProfiles){
        		java.lang.String introduction=linkProfile.getIntroduction();
        		if (introduction!=null) content.append(introduction+" ");
        	}
            urlsManager.updateSimHash(url, TextAnalysis.getSimHash(content.toString()));
            VersionUtils.log(content.toString());
            /* 添加链接url至todoUrls*/
            if (linkProfiles!=null){
            	List<java.lang.String> messages=urlsManager.getMessages(url); //获取messages
            	for (Profile linkProfile:linkProfiles){
            		if (linkProfile.getUrl()==null) continue;
                	linkProfile.setUrl(Utils.removeSuffix(linkProfile.getUrl()));
                	if (linkProfile.getUrl().startsWith("/"))
                		linkProfile.setUrl(hostUrl+linkProfile.getUrl());
                	/*仅添加必应学术url*/
                	if (BingAnalysisUtils.isBingAcademicProfileUrl(linkProfile.getUrl())){
                		if (messages!=null && messages.size()>0){
                			/*仅添加指定搜索的文章*/
                			boolean containsProperAuthor=false,isTitleRight=false;
            				/*标题是否正确(比较前40个字符)*/
                			int compareStarts=40;
                			java.lang.String properTitle=messages.get(0);
                			java.lang.String linkTitle=linkProfile.getTitle();
            				if (!properTitle.substring(0, properTitle.length()>compareStarts?compareStarts:properTitle.length())
            						.equalsIgnoreCase(linkTitle.substring(0, linkTitle.length()>compareStarts?compareStarts:linkTitle.length()))) {
//            					VersionUtils.log("[指定搜索文章]错误标题"+linkProfile.getTitle()); //
            				}
            				else isTitleRight=true;
            				/*作者是否正确*/
            				if (messages.size()>1){
            					List<java.lang.String> linkAuthors=linkProfile.getAuthors();
            					if (linkAuthors==null || linkAuthors.size()==0) {
//            						VersionUtils.log("[指定搜索文章]作者不存在"); //
            						containsProperAuthor=false;
            					}
            					/*存在一个正确的作者即可*/
            					else for (int i=1;i<messages.size();i++){
            						if (linkAuthors.contains(messages.get(i))) {
            							containsProperAuthor=true;
            							break;
            						}
            					}
            				}
            				if (isTitleRight || containsProperAuthor){
            					/*添加指定文章,且移除深度限制*/
                        		if (urlsManager.putUrl(linkProfile.getUrl(),url,false)==OperationStatus.SUCCESS){
                					VersionUtils.log("[添加指定文章]"+linkProfile.getTitle()+"("+linkProfile.getUrl()+")");
                        			urlsManager.updateWeight(linkProfile.getUrl(), CrawlUrl.DEFAULT_WEIGHT+1); /*提高优先级*/
                        		}
            				}
                		}
                		else{
                			VersionUtils.log("[添加文章]"+linkProfile);
                    		urlsManager.putUrl(linkProfile.getUrl(),url);
                    		urlsManager.updateWeight(linkProfile.getUrl(), CrawlUrl.DEFAULT_WEIGHT+1); /*提高优先级*/
                		}
                	}
                	else if (BingAnalysisUtils.isBingAcademicSearchUrl(linkProfile.getUrl())){
                		VersionUtils.log("[添加搜索]"+linkProfile); //不太可能出现的情况
                		urlsManager.putUrl(linkProfile.getUrl(),url);
                	}
                }
            }
        }
        else{
        	/*不是必应学术时,一般化分析html*/
        	commonAnalyzeHtml(html,url);
        	/*使用一般正文内容生成SimHash*/
            urlsManager.updateSimHash(url, TextAnalysis.getSimHash(CommonAnalysis.getContent(html)));
            /*添加链接url至todoUrls(暂停使用)*/
            /*Set<String> linkSet=CommonAnalysis.extractLinks(html,Utils.getHostUrl(url)); //
            for (Iterator it=linkSet.iterator();it.hasNext();){
            	String linkUrl=(String) it.next();
            	linkUrl=Utils.removeSuffix(linkUrl);
            	manager.putUrl(linkUrl,url);
            }*/
        }
	}

	public Parser() {
		super();
		setup();
	}

}
