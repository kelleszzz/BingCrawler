package com.kelles.crawler.bingcrawler.database;


import java.io.File;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.impl.Store;


public class UrlsDbManager {
	public static void main(String[] args) throws Exception{
		UrlsDbManager manager=null;
		try{
			manager=new UrlsDbManager("DbManagerTest");
			CrawlUrl u1=new CrawlUrl("http://www.hacg.fi/wp/23147.html#comment-62635");
			CrawlUrl u2=new CrawlUrl("http://www.bing.com");
			CrawlUrl u3=new CrawlUrl("http://www.w3school.com.cn/");
			CrawlUrl u4=new CrawlUrl("http://blog.csdn.net/shangboerds/article/details/7532676");
			CrawlUrl u5=new CrawlUrl("https://www.hacg.li");
			u3.setWeight(150);
			u4.setWeight(80);
			manager.putUrl(u1);
			manager.putUrl(u2);
			manager.putUrl(u3);
			manager.putUrl(u4);
			manager.putUrl(u5);
			
			VersionUtils.log("获取weight最高的条目:\n"+manager.getNextCrawlUrl());
			
			SecondaryCursor secCursor=manager.db.todoUrlsByWeight.openSecondaryCursor(null, null);
			DatabaseEntry searchKey=new DatabaseEntry(Utils.intToByteArray(100));
			DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundValue = new DatabaseEntry();
            VersionUtils.log("遍历weightSecDb的条目");
            OperationStatus retVal=secCursor.getFirst(foundKey, foundValue, LockMode.DEFAULT);
            for (;;){
            	if (retVal==OperationStatus.SUCCESS){
					CrawlUrl crawlUrl=(CrawlUrl) manager.db.serialBinding.entryToObject(foundValue); 
					VersionUtils.log("weight = "+crawlUrl.getWeight()+":\n"+crawlUrl);
//					secCursor.delete();
				}
				else break;
				retVal=secCursor.getNext(foundKey,foundValue, LockMode.DEFAULT);
            }
            VersionUtils.log("搜索weight=100的条目");
			retVal=secCursor.getSearchKey(searchKey, foundKey, foundValue,LockMode.DEFAULT);
			for (;;){
				if (retVal==OperationStatus.SUCCESS){
					CrawlUrl crawlUrl=(CrawlUrl) manager.db.serialBinding.entryToObject(foundValue); 
					VersionUtils.log("weight = "+crawlUrl.getWeight()+":\n"+crawlUrl);
//					secCursor.delete();
				}
				else break;
				retVal=secCursor.getNextDup(foundKey,foundValue, LockMode.DEFAULT);
			}
			manager.db.describe();
		}
		catch(Exception e){e.printStackTrace();}
		finally{
			manager.close();
		}		
	}

	private UrlsDb db=null;
	private int maxDepth=Integer.MAX_VALUE;
	private String homeDirPath=null;
	
	private void setup(){
		if (db==null) {
			db=new UrlsDb(homeDirPath);
			VersionUtils.log(11.14,"加载UrlsDbManager["+homeDirPath+"]");
		}
	}
	public void close(){
		if (db!=null){
			db.close();
			db=null;
		}
	}
	
	/*uniUrls条目数*/
	public long sizeUniUrls(){
		Cursor cursor;
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		OperationStatus retVal=null;
		cursor=db.uniUrls.openCursor(null, null);
		if (cursor.getFirst(key, value, LockMode.DEFAULT)!=OperationStatus.SUCCESS) return 0;
		long size=cursor.skipNext(Long.MAX_VALUE, key, value, LockMode.DEFAULT);
		if (cursor!=null) cursor.close();
		return (size+1);
	}
	
	//从uniUrls中获取messages
	public List<String> getMessages(String url){
		CrawlUrl crawlUrl=getCrawlUrlFromUniUrls(url);
		if (crawlUrl!=null && crawlUrl.getMessages()!=null) return crawlUrl.getMessages();
		return null;
	}
	
	//从todoUrls中获取已存在的SimHash
	public BigInteger getSimHash(String url){
		CrawlUrl crawlUrl=getCrawlUrlFromTodoUrls(url);
		if (crawlUrl!=null && crawlUrl.getSimHash()!=null) return crawlUrl.getSimHash();
		return null;
	}
	
	//获取所有uniUrls中的SimHash值
	public List<Map<String,Object>> getAllSimHashs(){
		Transaction txn=null;
		Cursor cursor=null;
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		List<Map<String,Object>> simHashs=new ArrayList<Map<String,Object>>();

		try{
			txn=db.env.beginTransaction(null, db.txnConf);
			cursor=db.uniUrls.openCursor(txn, null);
			OperationStatus retVal=cursor.getFirst(key, value, LockMode.DEFAULT);
	        for (;;){
	         	if (retVal==OperationStatus.SUCCESS){
	         		CrawlUrl crawlUrl=(CrawlUrl) db.serialBinding.entryToObject(value);
					if (crawlUrl.getSimHash()!=null){
						Map<String,Object> map=new HashMap();
						map.put("url", crawlUrl.getUrl());
						map.put("simHash", crawlUrl.getSimHash());
						simHashs.add(map);
					}
				}
				else break;
				retVal=cursor.getNext(key,value, LockMode.DEFAULT);
	        }
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
        
       return simHashs;
	}
	
	//更新相应todoUrls中的messages值
	public OperationStatus updateMessages(String url,List<String> messages){
		CrawlUrl crawlUrl=getCrawlUrlFromTodoUrls(url);
		if (crawlUrl!=null){
			if (messages==null && crawlUrl.getMessages()!=null) crawlUrl.getMessages().clear();
			else crawlUrl.setMessages(messages);
			return updateCrawlUrlFromTodoUrls(crawlUrl);
		}
		return OperationStatus.NOTFOUND;
	}
	
	//更新相应todoUrls中的weight值
	public OperationStatus updateWeight(String url,int weight){
		CrawlUrl crawlUrl=getCrawlUrlFromTodoUrls(url);
		if (crawlUrl!=null){
			crawlUrl.setWeight(weight);
			return updateCrawlUrlFromTodoUrls(crawlUrl);
		}
		return OperationStatus.NOTFOUND;
	}
	
	//更新相应uniUrls中的simHash值
	public OperationStatus updateSimHash(String url,BigInteger simHash){
		CrawlUrl crawlUrl=getCrawlUrlFromUniUrls(url);
		crawlUrl.setSimHash(simHash);
		return updateCrawlUrlFromUniUrls(crawlUrl);
	}
	
	//清除todoUrls,uniUrls所有条目
	public void clearDb(){
		Transaction txn=null;
		Cursor cursor=null;
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		OperationStatus retVal=null;
		
		try{
			txn=db.env.beginTransaction(null, db.txnConf);
			//清除todoUrls
			cursor=db.todoUrls.openCursor(txn, null);
			retVal=cursor.getFirst(key, value, LockMode.DEFAULT);
	        for (;;){
	         	if (retVal==OperationStatus.SUCCESS){
						cursor.delete();
				}
				else break;
				retVal=cursor.getNext(key,value, LockMode.DEFAULT);
	        }
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	    
		try{
			txn=db.env.beginTransaction(null, db.txnConf);
	       //清除uniUrls
	       cursor=db.uniUrls.openCursor(txn, null);
	       retVal=cursor.getFirst(key, value, LockMode.DEFAULT);
	          for (;;){
	           	if (retVal==OperationStatus.SUCCESS){
	  					cursor.delete();
	  			}
	  			else break;
	  			retVal=cursor.getNext(key,value, LockMode.DEFAULT);
	       }
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
		
	}
	
	//从todoUrls移除url,并添加到uniUrls
	public void settleUrl(String url,int statusCode){
		Transaction txn=null;
		if (url==null) return;
		CrawlUrl crawlUrl=null;
		Cursor cursor=null;
		DatabaseEntry key=null;
		try {key = new DatabaseEntry(url.getBytes("utf-8"));} catch (UnsupportedEncodingException e) {}
		//是否存在于todoUrls
		try{
			txn=db.env.beginTransaction(null, db.txnConf);
			DatabaseEntry value=new DatabaseEntry();
			cursor=db.todoUrls.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				crawlUrl=(CrawlUrl) db.serialBinding.entryToObject(value);
				crawlUrl.setStatusCode(statusCode);
				cursor.delete();
			}
			else return;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
		//添加到uniUrls
		try{
			txn=db.env.beginTransaction(null, db.txnConf);
			DatabaseEntry value=new DatabaseEntry();
			cursor=db.uniUrls.openCursor(txn, null);
			db.serialBinding.objectToEntry(crawlUrl, value);
			cursor.put(key, value);
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	//从todoUrls中获取weight最高的条目
	public String getNext(){
		CrawlUrl crawlUrl=getNextCrawlUrl();
		return crawlUrl==null?null:crawlUrl.getUrl();
	}
	protected CrawlUrl getNextCrawlUrl(){
		Transaction txn=null;
		SecondaryCursor secCursor=null;
		try{
			txn=db.env.beginTransaction(null, db.txnConf);
			DatabaseEntry searchKey = new DatabaseEntry();
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundValue=new DatabaseEntry();
			secCursor=db.todoUrlsByWeight.openCursor(txn, null);
			OperationStatus retVal=secCursor.getLast(searchKey,foundKey, foundValue, LockMode.DEFAULT);
			if (retVal==OperationStatus.SUCCESS){
				CrawlUrl crawlUrl=(CrawlUrl) db.serialBinding.entryToObject(foundValue);
				return crawlUrl;
			}
			return null;
		}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	//判断url是否在todoUrls中,若存在则返回相应的CrawlUrl,不存在返回null
	public CrawlUrl getCrawlUrlFromTodoUrls(String url){
		Transaction txn=null;
		try {
			txn=db.env.beginTransaction(null, db.txnConf);
			DatabaseEntry key = new DatabaseEntry(url.getBytes("utf-8"));
			DatabaseEntry value=new DatabaseEntry();
			Cursor cursor=null;
			try{
				cursor=db.todoUrls.openCursor(txn, null);
				if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
					CrawlUrl crawlUrl=(CrawlUrl) db.serialBinding.entryToObject(value);
					return crawlUrl;
				}
				return null;
			}
			catch(Exception e){throw new RuntimeException(e);}
			finally{
				if (cursor!=null) cursor.close();
				if (txn!=null) txn.commit();
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}
	
	//判断url是否在uniUrls中,若存在则返回相应的CrawlUrl,不存在返回null
	public CrawlUrl getCrawlUrlFromUniUrls(String url){
		Transaction txn=null;
		try {
			txn=db.env.beginTransaction(null, db.txnConf);
			DatabaseEntry key = new DatabaseEntry(url.getBytes("utf-8"));
			DatabaseEntry value=new DatabaseEntry();
			Cursor cursor=null;
			try{
				cursor=db.uniUrls.openCursor(txn, null);
				if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
					CrawlUrl crawlUrl=(CrawlUrl) db.serialBinding.entryToObject(value);
					return crawlUrl;
				}
				return null;
			}
			catch(Exception e){throw new RuntimeException(e);}
			finally{
				if (cursor!=null) cursor.close();
				if (txn!=null) txn.commit();
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}	
	}
	
	//更新todoUrls中的CrawlUrl
		private OperationStatus updateCrawlUrlFromTodoUrls(CrawlUrl crawlUrl){
			Transaction txn=null;
			try {
				txn=db.env.beginTransaction(null, db.txnConf);
				DatabaseEntry key = new DatabaseEntry(crawlUrl.getUrl().getBytes("utf-8"));
				DatabaseEntry value=new DatabaseEntry();
				Cursor cursor=null;
				try{
					cursor=db.todoUrls.openCursor(txn, null);
					if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
						db.serialBinding.objectToEntry(crawlUrl, value);
						return cursor.putCurrent(value);
					}
					else return OperationStatus.NOTFOUND;
				}
				catch(Exception e){throw new RuntimeException(e);}
				finally{
					if (cursor!=null) cursor.close();
					if (txn!=null) txn.commit();
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return OperationStatus.NOTFOUND;
			}	
		}
	
	//更新uniUrls中的CrawlUrl
	private OperationStatus updateCrawlUrlFromUniUrls(CrawlUrl crawlUrl){
		Transaction txn=null;
		try {
			txn=db.env.beginTransaction(null, db.txnConf);
			DatabaseEntry key = new DatabaseEntry(crawlUrl.getUrl().getBytes("utf-8"));
			DatabaseEntry value=new DatabaseEntry();
			Cursor cursor=null;
			try{
				cursor=db.uniUrls.openCursor(txn, null);
				if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
					db.serialBinding.objectToEntry(crawlUrl, value);
					return cursor.putCurrent(value);
				}
				else return OperationStatus.NOTFOUND;
			}
			catch(Exception e){throw new RuntimeException(e);}
			finally{
				if (cursor!=null) cursor.close();
				if (txn!=null) txn.commit();
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return OperationStatus.NOTFOUND;
		}	
	}
	
	public void describe(){
		db.describe();
	}
	

	//添加一个url至todoUrls,深度为urlReferedTo+1,hasDepthRestriction时有深度限制
	public OperationStatus putUrl(String url){return putUrl(new CrawlUrl(url),null,true);}
	public OperationStatus putUrl(String url,String urlReferedTo){
		return putUrl(new CrawlUrl(url),urlReferedTo,true);
	}
	public OperationStatus putUrl(String url,String urlReferedTo,boolean hasDepthRestriction){
		return putUrl(new CrawlUrl(url),urlReferedTo,hasDepthRestriction);
	}
	private OperationStatus putUrl(CrawlUrl crawlUrl){return putUrl(crawlUrl,null,true);}
	private OperationStatus putUrl(CrawlUrl crawlUrl,String urlReferedTo,boolean hasDepthRestriction){
		Transaction txn=null;
		try {
			DatabaseEntry key=new DatabaseEntry(crawlUrl.getUrl().getBytes("utf-8"));
			DatabaseEntry value=new DatabaseEntry();
			db.serialBinding.objectToEntry(crawlUrl, value);
			DatabaseEntry searchedValue=new DatabaseEntry();
			Cursor cursor=null;
			//是否存在于uniUrls
			try{
				txn=db.env.beginTransaction(null, db.txnConf);
				cursor=db.uniUrls.openCursor(txn, null);
				if (cursor.getSearchKey(key, searchedValue, LockMode.DEFAULT)!=OperationStatus.NOTFOUND){
//					VersionUtils.log("已存在于uniUrls url = "+crawlUrl.getUrl()); //
					if (urlReferedTo!=null){
						CrawlUrl crawlUrlSearched=(CrawlUrl) db.serialBinding.entryToObject(searchedValue);
						crawlUrlSearched.getUrlsReferedTo().add(urlReferedTo);
						db.serialBinding.objectToEntry(crawlUrlSearched, searchedValue);
						cursor.putCurrent(searchedValue);
					}
					return OperationStatus.KEYEXIST;
				}
			}
			catch(Exception e){throw new RuntimeException(e);}
			finally{
				if (cursor!=null) cursor.close();
				if (txn!=null) txn.commit();
			}
			//添加至todoUrls
			try{
				txn=db.env.beginTransaction(null, db.txnConf);
				cursor=db.todoUrls.openCursor(txn, null);
				if (cursor.getSearchKey(key, searchedValue, LockMode.DEFAULT)!=OperationStatus.NOTFOUND){
//					VersionUtils.log("已存在于todoUrls url = "+crawlUrl.getUrl()); //
					if (urlReferedTo!=null){
						CrawlUrl crawlUrlSearched=(CrawlUrl) db.serialBinding.entryToObject(searchedValue);
						crawlUrlSearched.getUrlsReferedTo().add(urlReferedTo);
						db.serialBinding.objectToEntry(crawlUrlSearched, searchedValue);
						cursor.putCurrent(searchedValue);
					}
					return OperationStatus.KEYEXIST;
				}
			}
			catch(Exception e){throw new RuntimeException(e);}
			finally{
				if (cursor!=null) cursor.close();
				if (txn!=null) txn.commit();
			}
			//添加url
			try{
				txn=db.env.beginTransaction(null, db.txnConf);
				//当前url是第一次添加,查找urlReferedTo的深度,并设置当前深度+1
				if (urlReferedTo!=null){
					CrawlUrl sourceCrawlUrl=getCrawlUrlFromUniUrls(urlReferedTo);
					if (sourceCrawlUrl!=null){
						if (sourceCrawlUrl.getDepth()+1>maxDepth
								&& hasDepthRestriction) 
							return OperationStatus.KEYEXIST; //超过搜索深度
						crawlUrl.setDepth(sourceCrawlUrl.getDepth()+1);
						db.serialBinding.objectToEntry(crawlUrl, value);
					}
				}
				cursor=db.todoUrls.openCursor(txn, null);
//				VersionUtils.log("添加至todoUrls url = "+crawlUrl.getUrl()); //
				cursor.put(key, value);
				return OperationStatus.SUCCESS;
			}
			catch(Exception e){throw new RuntimeException(e);}
			finally{
				if (cursor!=null) cursor.close();
				if (txn!=null) txn.commit();
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return OperationStatus.KEYEMPTY;
		}
	}
	
	public int getMaxDepth() {
		return maxDepth;
	}
	public void setMaxDepth(int maxDepth) {
		this.maxDepth = maxDepth;
	}
	public UrlsDbManager(String homeDirPath) {
		super();
		this.homeDirPath = homeDirPath;
		setup();
	}
	
	
	
}
