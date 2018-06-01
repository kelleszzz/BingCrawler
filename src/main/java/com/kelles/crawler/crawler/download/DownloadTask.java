package com.kelles.crawler.crawler.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import com.kelles.crawler.crawler.setting.Setting;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.util.TextUtils;

import com.kelles.crawler.crawler.util.*;

public class DownloadTask implements Serializable{
	
	private Set<java.lang.String> backupUrls=new TreeSet(); //从这些url中下载,成功一个即可
	private File toFile=null;
	private java.lang.String refererUrl=null;
	private java.lang.String suffix=null;
	private int weight=DEFAULT_WEIGHT;
	public static final int DEFAULT_WEIGHT=100;
	private static final int retryTimes=3;
	private static final int SOCKET_TIMEOUT=5000;
	private static final int CONNECT_TIMEOUT=3000;
	
	
	public static void main(java.lang.String[] args){
		DownloadTask task=new Builder()
				.toFile(Setting.ROOT+"/DownloadTaskTest", "A view of cloud computing",".pdf")
				.setReferer("http://cn.bing.com/academic/profile?id=2114296561&encoded=0&v=paper_preview&mkt=zh-cn")
				.addUrl("http://genet.univ-tours.fr/gen002200/bibliographie/biblio_complete_juin2008/Clustering/Weighted%20rank%20aggregation%20of%20cluster%20validation%20measures%20a%20_1.pdf")
				.addUrl("http://cs.nyu.edu/courses/spring15/CSCI-GA.3033-011/ViewofCloud.pdf")
				.addUrl("http://people.csail.mit.edu/matei/courses/2015/6.S897/readings/above-the-clouds.pdf")
				.addUrl("http://www.unf.edu/~sahuja/cis6302/AViewofCloudComputing.pdf")
				.build();
		if (task.startDownload()) Logger.log("下载完成");
		else Logger.log("下载失败");
	}
	
	/*获得DownloadTask的byte[]值,用于数据库的key*/
	public byte[] getMd5Bytes(){
		try {
			/*StringBuilder inStr=new StringBuilder();
			for (String backupUrl:backupUrls) inStr.append(backupUrl);
			String md5=Md5.get(inStr.toString());
			byte[] bytes=md5.getBytes("utf-8");*/
			StringBuilder inStr=new StringBuilder();
			for (java.lang.String backupUrl:backupUrls) inStr.append(backupUrl);
			byte[] bytes=inStr.toString().getBytes("utf-8");
			return bytes;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/*开始下载,backupUrls中有一个成功即停止*/
	public boolean startDownload(){
		boolean hasOneDownloaded=false;
		try{
			if (!toFile.isDirectory()) toFile.mkdirs();
			for (java.lang.String url:backupUrls){
				File finalFile=null;
				int finalFileName=1;
				for (;;finalFileName++){
					finalFile=new File(toFile,finalFileName+suffix);
					if (!finalFile.isFile()){
						finalFile.createNewFile();
						break;
					}
				}
				
				/*为当前url生成一个html重定向页面*/
				Util.generateRedirectHtml(url, toFile.getAbsolutePath(), finalFileName+"");
				
				/*每个url尝试多次*/
				boolean curLinkSuccess=false;
				for (int i=0;i<retryTimes;i++)
					if (startDownload(url,finalFileName+suffix)) {
						curLinkSuccess=true;
						hasOneDownloaded=true;
						break;
					}
				if (!curLinkSuccess && finalFile!=null && finalFile.isFile()) finalFile.delete(); //下载失败,删除
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		return hasOneDownloaded;
	}
	private boolean startDownload(java.lang.String url, java.lang.String finalFileName){
		HttpClientBuilder httpbuilder=HttpClientBuilder.create();
		CloseableHttpClient httpclient=httpbuilder.build();
		
	    try {
            HttpGet httpGet = new HttpGet(url);
            addHeaders(httpGet);  //模拟为浏览器登录
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(SOCKET_TIMEOUT).setConnectTimeout(CONNECT_TIMEOUT).build();//设置超时
            httpGet.setConfig(requestConfig);
            CloseableHttpResponse response = httpclient.execute(httpGet);
            try {
                if (response.getStatusLine().getStatusCode()==HttpStatus.SC_OK){
                	HttpEntity entity=null;
               	    FileOutputStream fos=null;
                	try{
                		entity = response.getEntity();
                		File finalFile=new File(toFile,finalFileName);
                		if (!finalFile.isFile()) finalFile.createNewFile();
                		fos=new FileOutputStream(finalFile);
                		entity.writeTo(fos);
      	                return true;
                	}
                	catch(SocketTimeoutException e1){
                		/*超时是最常见的情况*/
                		return false;
                	}
                	catch(Exception e){
                		e.printStackTrace();
                    	return false;
                	}
                	finally{
                		/*关闭Entity*/
      	                if (entity!=null) EntityUtils.consume(entity);
      	                if (fos!=null) fos.close();
                	}
                }
                else return false;
            }
            catch(Exception e){
            	e.printStackTrace();
            	return false;
            }
            finally {
				response.close();
            }
	    }
	    catch(ConnectTimeoutException e1){
	    	/*这是最常见的情况*/
	    	return false;
	    }
	    catch(SocketTimeoutException e1){
	    	/*这是最常见的情况*/
	    	return false;
	    }
	    catch(Exception e){
	    	Logger.log("[下载异常Url]"+url+"\n[下载异常文件]"+toFile.getName());
        	e.printStackTrace();
        	return false;
        }
	    finally{
	    	try {
				httpclient.close();
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
	    }
	}
	
	private void addHeaders(HttpRequestBase base){
		base.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0");
		base.setHeader("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		base.setHeader("Accept-Language","zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3");
		base.setHeader("Accept-Encoding","gzip, deflate ,br");
		if (refererUrl!=null) base.setHeader("Referer",refererUrl);
	}
	
	public static class Builder{
		private DownloadTask singleton=null;
		public Builder setReferer(java.lang.String refererUrl){
			if (singleton==null) singleton=new DownloadTask();
			if (!TextUtils.isEmpty(refererUrl)) singleton.refererUrl=refererUrl;
			return this;
		}
		public Builder addUrl(java.lang.String downloadUrl){
			if (singleton==null) singleton=new DownloadTask();
			if (!TextUtils.isEmpty(downloadUrl)) singleton.backupUrls.add(downloadUrl);
			return this;
		}
		public Builder addUrls(Collection<java.lang.String> downloadUrls){
			if (singleton==null) singleton=new DownloadTask();
			if (downloadUrls!=null)
				for (java.lang.String downloadUrl:downloadUrls)
					if (!TextUtils.isEmpty(downloadUrl)) 
						singleton.backupUrls.add(downloadUrl);
			return this;
		}
		public Builder toFile(java.lang.String rootDir, java.lang.String fileDir, java.lang.String suffix){
			if (singleton==null) singleton=new DownloadTask();
			File toFile=new File(rootDir+"/"+fileDir);
			if (!toFile.isDirectory()) toFile.mkdirs();
			singleton.toFile=toFile;
			if (suffix==null) suffix="";
			singleton.suffix=suffix;
			return this;
		}
		public DownloadTask build(){
			if (singleton==null 
					|| singleton.toFile==null
					|| singleton.backupUrls.size()==0)
				throw new RuntimeException("DownloadTask必须指定下载链接和下载文件名");
			return singleton;
		}
	}

	private DownloadTask() {}

	public Set<java.lang.String> getBackupUrls() {
		return backupUrls;
	}


	public File getToFile() {
		return toFile;
	}


	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	@Override
	public java.lang.String toString() {
		return "DownloadTask [backupUrls=" + backupUrls + ", \ntoFile=" + toFile.getName() + ", refererUrl=" + refererUrl
				+ ", \nweight=" + weight + "]";
	}

	
	
	
	
}
