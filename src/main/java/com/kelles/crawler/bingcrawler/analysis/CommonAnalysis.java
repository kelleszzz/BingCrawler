package com.kelles.crawler.bingcrawler.analysis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.edu.hfut.dmic.contentextractor.ContentExtractor;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.util.TextUtils;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.lexer.Lexer;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;

public class CommonAnalysis {
	
	//使用Selenium在给定时间的读取之后获取html
	private static final int TIMEOUT=5000;
	private static final int POLLING_EVERY=500;
	@SuppressWarnings("unchecked")
	public static String seleniumGetHtml(String url,WebDriver driver){
		try{
			String html="";
			driver.navigate().to(url); //跳转
			
			//初始加载等待
			Wait wait=new FluentWait(driver)
					.ignoring(NoSuchElementException.class)
					.withTimeout(Duration.of(TIMEOUT, ChronoUnit.MILLIS))
					.pollingEvery(Duration.of(POLLING_EVERY,ChronoUnit.MILLIS));
			
			//对比上一次是否有加载更多,不再加载更多时返回
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
								throw new NoSuchElementException("页面仍在更新");
							}
							else if (contentTextLength==arg0 && arg1==0){
								arg1=1;
								throw new NoSuchElementException("页面内容停止更新,再次进行尝试");
							}
							else return contentElement;
						}
						else throw new NoSuchElementException("无法获取页面");
					}
				});
			}catch(TimeoutException e){e.printStackTrace();}
			
			WebElement htmlElement=driver.findElement(By.tagName("html"));
			html=htmlElement.getAttribute("outerHTML");
			return html;
		}catch(WebDriverException e){
			e.printStackTrace();
			return null;
		}
	}

	//使用HtmlUnit库获取网页的html文本
	public static String htmlUnitGetHtml(String url){
			WebClient webClient=null;
			try{
				//创建一个webclient
				webClient = new WebClient();
		        //htmlunit 对css和javascript的支持不好，所以请关闭之
		        webClient.setJavaScriptEnabled(false);
		        webClient.setCssEnabled(false);
		        webClient.setTimeout(20000);
		        //获取页面
		        HtmlPage page = webClient.getPage(url);
		        //获取页面的XML代码
		        String html = page.asXml();
		        return html;
			}
			catch (FailingHttpStatusCodeException e) {
				//404
				return null;
			}
			catch(Exception e){
				e.printStackTrace();
				return null;
			}
			finally{
				 //关闭webclient
				if (webClient!=null) webClient.closeAllWindows();
			}
		}
		
	
	//使用HttpClient库获取网页的html文本
	public static String httpClientGetHtml(String url){
		HttpClientBuilder httpbuilder=HttpClientBuilder.create();
		CloseableHttpClient httpclient=httpbuilder.build();
		
		String html=null;
		try{
		    try {
	            HttpGet httpGet = new HttpGet(url);
	            //setProxy(httpGet); //设置代理
	            addHeaders(httpGet); //模拟为浏览器登录
	            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(15000).setConnectTimeout(15000).build();//设置超时
	            httpGet.setConfig(requestConfig);
	            CloseableHttpResponse response1 = httpclient.execute(httpGet);
	            try {
	            	VersionUtils.log(response1.getStatusLine());
	                if (ifRedirect(response1.getStatusLine().getStatusCode())) {
	                	//重定向
	                	Header[] headers=response1.getHeaders("location");
	                	if (headers!=null && headers.length>0)
	                		return httpClientGetHtml(headers[0]. getValue());
	                }
	                else if (response1.getStatusLine().getStatusCode()==HttpStatus.SC_OK){
		                HttpEntity entity1 = response1.getEntity();
		                //获取html
		                html=CommonAnalysis.inputStreamToString(entity1.getContent());
		                //关闭Entity
		                EntityUtils.consume(entity1);
	                }
	            }
	            finally {
					response1.close();
	            }
		    }
		    finally{
		    	httpclient.close();
		    }
		}
		catch(Exception e){return null;}
		return html;
		    
	}
	private static void addHeaders(HttpRequestBase base){
		base.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0");
		base.setHeader("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		base.setHeader("Accept-Language","zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3");
		base.setHeader("Accept-Encoding","gzip, deflate ,br");
	}
	
	private static void setProxy(HttpRequestBase base){
		// �����Ǵ����ַ������˿ںţ�Э������
	    HttpHost proxy = new HttpHost("14.29.124.52", 80, "http");
	    RequestConfig config = RequestConfig.custom().setProxy(proxy).build();
        base.setConfig(config);
	}
	
	//�ж�״̬���Ƿ���Ҫ�ض���
	protected static boolean ifRedirect(int statusCode){
		return (statusCode==HttpStatus.SC_MOVED_TEMPORARILY
				|| statusCode==HttpStatus.SC_MOVED_PERMANENTLY
				|| statusCode==HttpStatus.SC_SEE_OTHER
				|| statusCode==HttpStatus.SC_TEMPORARY_REDIRECT);
	}	
	//��ʾ��ҳ������Header
	private static void showHeaders(Header[] headers){
		for (Header header:headers){
			VersionUtils.log(header.getName()+" : "+header.getValue());
		}
	}
	
	
	//根据filePath获取html
	public static String getHtmlByPath(String filePath){return getHtmlByPath(filePath,null);}
	public static String getHtmlByPath(String filePath,String charset){
		try {
			File file=new File(filePath);
			if (!file.isFile()) return null;
			FileInputStream fis=new FileInputStream(file);
			byte[] bytes=new byte[fis.available()];
			fis.read(bytes);
			if (TextUtils.isEmpty(charset)) charset=CommonAnalysis.getCharset(bytes);
			String html=new String(bytes,charset);
			return html;
		} 
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	//通过html文本获取正文内容
	public static String getContent(String html){
//		VersionUtils.log(11.15,"准备提取正文,charset为"+charset+",html为:\n"+html);
		try {
			if (TextUtils.isEmpty(html)) return null;
			String content = ContentExtractor.getContentByHtml(html);
			return content;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	//通过html文本读取其中的种子字符串
	public static void readTorrent(String html){
		try {
			Parser parser=new Parser(new Lexer(html));
			parser.setEncoding(getCharset(html));
			NodeList baselist=parser.parse(null);
			List<Node> linkedList=new LinkedList(); 
			for (int i=0;i<baselist.size();i++){
				Node node=baselist.elementAt(i);
				linkedList.add(node);
			}
			for (int p=0;p<linkedList.size();p++){
				Node node=linkedList.get(p);
				//添加子节点去搜索
				NodeList childList=node.getChildren();
				if (childList!=null)
				for (int i=0;i<childList.size();i++){
					Node childNode=childList.elementAt(i);
					linkedList.add(childNode);
				}
				//搜索作为正文内容的结点
				if (childList==null){
					List<String> torrentList=readTorrentFromText(node.toPlainTextString());
					if (torrentList!=null){
						showNodeInfo(node);
						String str=new String("");
						for (String torrent:torrentList) str+=torrent+"\n";
						VersionUtils.log("分析到种子:\n"+str+"["+(str.length()-1)+"]");
						VersionUtils.log("===============================================\n");
					}
				}
			}
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//通过一段纯文本读取种子字符串
	protected static List<String> readTorrentFromText(String plainText){
		List<String> torrentList=null;
		Pattern pattern;
		Matcher matcher;
		pattern=Pattern.compile("[0-9a-zA-Z]{10,45}");
		matcher=pattern.matcher(plainText);
		String str="";
		while (matcher.find()){
			if (!Pattern.compile("[0-9]").matcher(matcher.group(0)).find()) continue;
			if (!Pattern.compile("[a-zA-Z]").matcher(matcher.group(0)).find()) continue;
			if (torrentList==null) torrentList=new ArrayList<String>();
			torrentList.add(matcher.group(0));
		}
		if (torrentList!=null){
			for (int i=0;i<torrentList.size();i++){
				if (torrentList.get(i).length()<40 && i+1<torrentList.size() && torrentList.get(i).length()+torrentList.get(+1).length()==40){
					torrentList.set(i, torrentList.get(i)+torrentList.get(i+1));
					torrentList.remove(i+1);
			    }
			}
			for (int i=0;i<torrentList.size();i++){
				if (torrentList.get(i).length()!=40) {
//					VersionUtils.log(11.15,"移除非种子格式的"+torrentList.get(i));
					torrentList.remove(i--);
				}
			}
			if (torrentList.size()==0) return null;
 		}
		return torrentList;
	}
		
	//从html文本中读取指向的链接
		public static Set<String> extractLinks(String html,String hostUrl){
			try {
				Set<String> set=new HashSet(); 
				Parser parser=new Parser(new Lexer(html));
				parser.setEncoding(getCharset(html));
				parser.extractAllNodesThatMatch(new NodeFilter(){
					@Override
					public boolean accept(Node node) {
						String linkUrl=null,_hostUrl=hostUrl;
						//a href
						Pattern p=Pattern.compile("^a.+href=\"(?<link>.+?)\"");
						Matcher m=p.matcher(node.getText());
						if (m.find()) linkUrl=m.group("link");
						//iframe src
						else{
							p=Pattern.compile("i?frame.*src=\"(?<link>.+?)\"");
							m=p.matcher(node.getText());
							if (m.find()) linkUrl=m.group("link");
						}
						//
						if (linkUrl!=null){
							if (linkUrl.startsWith("#") || linkUrl.startsWith("javascript"))
								return false; //jQuery,js舍弃
							if (linkUrl.startsWith("/")) {
								if (TextUtils.isEmpty(_hostUrl)) return false;
								linkUrl=_hostUrl+linkUrl;
								linkUrl=Utils.removeSuffix(linkUrl);
							}
							set.add(Utils.convertHtmlCharEntity(linkUrl));
							return true;
						}
						return false;
					}
				});
				return set;
			} catch (ParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		
		private static void gothroughNode(Node node){
			System.out.println("===============================================\n");
			showNodeInfo(node);
			NodeList list=node.getChildren();
			if (list!=null){
				for (int i=0;i<list.size();i++)
					gothroughNode(list.elementAt(i));
			}
		}
		
		protected static void showNodeInfo(Node node){
			String str=new String("");
			str+="[node.toString()]\n"+node.toString()+"\n";
			str+="[node.getText()]\n"+node.getText()+"\n";
			str+="[node.toPlainTextString()]\n"+node.toPlainTextString()+"\n";
			str+="[node.toHtml()]\n"+node.toHtml()+"\n";
			VersionUtils.log(str);
		}

		public static void showNodesInfo(String html){
			try {
				Parser parser=new Parser(new Lexer(html));
				parser.setEncoding("utf-8");
				NodeList baselist=parser.parse(null);
				for (int i=0;i<baselist.size();i++){
					Node node=baselist.elementAt(i);
					gothroughNode(node);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//根据自定义规则将url转换为一个文件名
		/*如"https://www.bing.com/search?q=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD"
		将被替换为"https://www.bing.com/search?q=人工智能"后转换*/
		private static final String URL_CONVERT_MARK_PREFIX="_", URL_CONVERT_MARK_SUFFIX="_";
		public static String urlGetFileName(String url){return urlGetFileName(url,"html");}
		public static String urlGetFileName(String url,String suffix){
			String result=url;
			if (result.charAt(result.length()-1)=='/') result=result.substring(0, result.length()-1);
			Pattern p2=Pattern.compile("[\\.\\\\/:*?\"<>|]");
			Matcher m2=p2.matcher(result);
			while (m2.find()){
				int charCode=m2.group().charAt(0);
				result=m2.replaceFirst(URL_CONVERT_MARK_PREFIX+charCode+URL_CONVERT_MARK_SUFFIX);
				m2.reset(result);
			}
			//��"http://www.hacg.fi/wp/23147.html#comment-62755"��ȥ��"#comment-62755"
			result=Utils.removeSuffix(result); 
			if (suffix!=null) result+="."+suffix;
			return result;
		}
		//从文件名获取Ϊurl
		public static String fileNameGetUrl(String fileName){return fileNameGetUrl(fileName,"html");}
		public static String fileNameGetUrl(String fileName,String suffix){
			String url=fileName;
			if (url.lastIndexOf("\\")!=-1) url=url.substring(url.lastIndexOf("\\")+1); //���ļ�·���н�ȡ�ļ���
			url=url.replaceAll("\\."+suffix, ""); //ȥ���ļ���ʽ
			Pattern p2=Pattern.compile(URL_CONVERT_MARK_PREFIX+"(?<charcode>\\d+)"+URL_CONVERT_MARK_SUFFIX);
			Matcher m2=p2.matcher(url);
			while (m2.find()){
				char ch=(char) Integer.parseInt(m2.group("charcode"));
				url=m2.replaceFirst(ch+"");
				m2.reset(url);
			}
			if (!(url.startsWith("http")))
				url="http://"+url; //���httpͷ
			return url;
		}
		

		//保存String到File中
		public static File strToFile(String str,String dir,String fileName){
			File fileDir=new File(dir);
			if (!fileDir.isDirectory()) fileDir.mkdir();
			File file=new File(fileDir,fileName);
			return strToFile(str,file);
		}
		private static File strToFile(String str,File file){
			FileOutputStream fos=null;
			BufferedOutputStream bos=null;
			try {
				fos=new FileOutputStream(file);
				bos=new BufferedOutputStream(fos);
				bos.write(str.getBytes("utf-8"));
			} 
			catch (Exception e){
				e.printStackTrace();
			}
			finally{
				try{
					bos.close();
					fos.close();
				}catch(Exception e){}
			}
			return file;
		}
		
		public static File inputStreamToFile(InputStream is,String dir,String fileName){
			File fileDir=new File(dir);
			if (!fileDir.isDirectory()) fileDir.mkdir();
			File file=new File(fileDir,fileName);
			return inputStreamToFile(is,file);
		}
		private static File inputStreamToFile(InputStream is,File file){
			BufferedInputStream bis=null;
			FileOutputStream fos=null;
			try {
				bis=new BufferedInputStream(is);
				fos=new FileOutputStream(file);
				byte[] bytes=new byte[1024];
				for (int readCount=0;;){
					readCount=bis.read(bytes,0,bytes.length);
					if (readCount==-1) break;
					fos.write(bytes,0,readCount);
				}
			} 
			catch (Exception e){
				e.printStackTrace();
			}
			finally{
				try{
					bis.close();
					fos.close();
				}catch(Exception e){}
			}
			return file;
		}
		
		public static String inputStreamToString(InputStream is){
			File file=null;
			FileInputStream fis=null;
			try{
				//获取charset
				file=new File("temp.txt");
				inputStreamToFile(is,file);
				fis=new FileInputStream(file);
				byte[] bytes=new byte[fis.available()];
				fis.read(bytes);
				
				String charset=getCharset(bytes);
				String body=new String(bytes,charset);
				return body;
			}
			catch (Exception e){
				e.printStackTrace();
				return null;
			}
			finally{
				file.delete();
			}
		}
		
		//获取html的charset
		public static String getCharset(File file){
			FileInputStream fis=null;
			try{
				fis=new FileInputStream(file);
				byte[] bytes=new byte[fis.available()];
				fis.read(bytes);
				return getCharset(bytes);
			}catch(Exception e){
				e.printStackTrace();
				return null;
			}
			finally{
				try {
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		public static String getCharset(byte[] bytes){
			try{
				String html;
				html=new String(bytes,"utf-8");
				return getCharset(html);
			}catch(Exception e){return null;}
		}
		public static String getCharset(String html){
			String charset="utf-8";
			Pattern pattern=Pattern.compile("charset=\"(.+)\"");
			Matcher matcher=pattern.matcher(html);
			if (matcher.find()){charset=matcher.group(1);}
			return charset;
		}
		
}
