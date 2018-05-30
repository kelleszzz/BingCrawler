package com.kelles.crawler.bingcrawler.parser;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.kelles.crawler.bingcrawler.database.UrlsDbManager;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.kelles.crawler.bingcrawler.analysis.*;
import com.kelles.crawler.bingcrawler.database.*;
@Deprecated
public class Parser {
	
	private static UrlsDbManager manager=null;
	
	public static void main(String[] args){
		try{
			manager.clearDb();
			
//			addBingAcademicTheme("artificial intelligence papers",1);
			addUrl("https://www.bing.com/academic/profile?id=2126105956&encoded=0&v=paper_preview&mkt=zh-cn#");
//			addBingTheme("人工智能");
//			addUrl("https://www.hacg.li");
//	        addUrl("http://www.hacg.fi/wp/23147.html#comment-62635");
//	        addUrl("https://www.hacg.li");
//	        addUrl("http://www.w3school.com.cn/");
//			addUrl("http://blog.csdn.net/shangboerds/article/details/7532676");
//			addUrl("http://www.bing.com");
			
			parse();
		}
		catch (Exception e){e.printStackTrace();}
		finally{
			manager.close();
			VersionUtils.log("关闭Db");
		}
    }
	
	//添加bing学术上指定的主题至todoUrls
		public void addBingAcademicTheme(String theme,int page){
			String prefix="http://cn.bing.com/academic/search?q=";
			try {
				String url=prefix+URLEncoder.encode(theme, "utf-8")+"&first="+((page-1)*10+1);
				manager.putUrl(url);
				System.out.println(url); //
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
	//添加bing上指定的主题至todoUrls
	public static void addBingTheme(String theme){
		String prefix="https://www.bing.com/search?q=";
		try {
			manager.putUrl(prefix+URLEncoder.encode(theme, "utf-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//添加给定的url至todoUrls
	public static void addUrl(String url){
		ArrayList<String> list=new ArrayList<String>();
		list.add(url);
		addUrl(list);
	}
	public static void addUrl(List<String> initUrls){
		for (String url:initUrls){
			manager.putUrl(url);
			manager.updateWeight(url, CrawlUrl.DEFAULT_WEIGHT+10);
		}
	}
	
	//开始爬取
	public static void parse(){
		try {
            String nextUrl;
            while ((nextUrl=manager.getNext())!=null) {
            	if (!httpGet(nextUrl)) break;
            }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	//分析html正文
	protected static void analyzeHtml(String html,String url){
        //分析html
//		List<String> urls=BingAnalysis.analyzeBingAcademicLinks(html); for (String str:urls) VersionUtils.log(str); //分析必应学术
//		VersionUtils.log("正文内容:\n"+ParserUtils.getContent(html));
//		ParserUtils.showHammingDistances(url); //显示和所有uniUrl的海明距离
//		ParserUtils.showNodesInfo(html); //每个html结点查看
//        ParserUtils.readTorrent(html); //分析种子
	}


	//返回true时,表示继续;返回false时,表示用户手动退出
	public static String HTMLURLS_PATH="html_urls";
	private static boolean exit=false;
	private static boolean httpGet(String url) throws Exception{
		if (exit) return false;
		for (boolean jump=false;!jump;){
			VersionUtils.log("开始分析"+URLDecoder.decode(url,"utf-8")+"\n任意字符->继续| p->跳过 | db->查看数据库 | exit->退出"); //
			Scanner s=new Scanner(System.in);
			String str=s.nextLine();
			if ("exit".equals(str)) {
				exit=true;
				return false;
			}
			else if ("p".equals(str)) {
				manager.settleUrl(url, 0);
				return true;
			}
			else if ("db".equals(str)){
				manager.describe();
			}
			else jump=true;
		}
		   
		//获取html文本
		String html=CommonAnalysis.htmlUnitGetHtml(url);
        //添加url至uniUrls
		if (html==null) {
			VersionUtils.log("无法访问"+url);
			manager.settleUrl(url,HttpStatus.SC_NOT_FOUND);
			return true;
		}
		else manager.settleUrl(url, HttpStatus.SC_OK); 
        //按照自定义规则保存html文本至文件
        String fileName=CommonAnalysis.urlGetFileName(URLDecoder.decode(url,"utf-8"));
        File f=CommonAnalysis.strToFile(html,HTMLURLS_PATH,fileName);
        //添加SimHash
        manager.updateSimHash(url, TextAnalysis.getSimHash(CommonAnalysis.getContent(html)));
        //添加链接url至todoUrls
        Set<String> linkSet=CommonAnalysis.extractLinks(html,Utils.getHostUrl(url)); //
        for (Iterator it=linkSet.iterator();it.hasNext();){
        	String linkUrl=(String) it.next();
        	linkUrl=Utils.removeSuffix(linkUrl);
        	manager.putUrl(linkUrl,url);
        }
        //分析html
        analyzeHtml(html,url);
        return true;
	}
	
	protected static void httpPost(String url) throws Exception{

			HttpClientBuilder httpbuilder=HttpClientBuilder.create();
			CloseableHttpClient httpclient=httpbuilder.build();
		    // �����Ǵ����ַ������˿ںţ�Э������
		    HttpHost proxy = new HttpHost("14.29.124.52", 80, "http");
		    RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
		    
			try{
		 HttpPost httpPost = new HttpPost("http://httpbin.org/post");
         List <NameValuePair> nvps = new ArrayList <NameValuePair>();
         nvps.add(new BasicNameValuePair("username", "vip"));
         nvps.add(new BasicNameValuePair("password", "secret"));
         httpPost.setEntity(new UrlEncodedFormEntity(nvps));
         CloseableHttpResponse response2 = httpclient.execute(httpPost);

         try {
             VersionUtils.log(response2.getStatusLine());
             HttpEntity entity2 = response2.getEntity();
             BufferedReader bis=new BufferedReader(new InputStreamReader(entity2.getContent()));
             String str;
             while ((str=bis.readLine())!=null)
             VersionUtils.log(str);
             // do something useful with the response body
             // and ensure it is fully consumed
             EntityUtils.consume(entity2);
         } finally {
             response2.close();
         }
     } finally {
         httpclient.close();
     }
	}
	
	public static void showHammingDistances(String url){
		List<Map<String,Object>> simHashs=manager.getAllSimHashs();
		BigInteger simHash=manager.getSimHash(url);
		if (simHash==null) return;
		String str="和库中所有网页对比相似度:\n";
		for (Map<String,Object> map:simHashs){
			int hammingDistance=TextAnalysis.hammingDistance(simHash,(BigInteger) map.get("simHash"));
			str+="["+hammingDistance+"]"+map.get("url")+"\n";
		}
		VersionUtils.log(str);
	}
	
}
