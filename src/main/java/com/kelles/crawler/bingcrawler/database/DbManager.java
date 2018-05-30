package com.kelles.crawler.bingcrawler.database;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class DbManager<ValueObjectClass>{
	public static void main(String[] args) throws Exception{
		DbManager<CrawlUrl> manager=null;
		try{
			manager=new DbManager<CrawlUrl>("DbManagerTest",CrawlUrl.class,true);
			manager.describe();
//			manager.clearDb();
			CrawlUrl u1=new CrawlUrl("http://www.hacg.fi/wp/23147.html#comment-62635");
			CrawlUrl u2=new CrawlUrl("http://www.bing.com");
			CrawlUrl u3=new CrawlUrl("http://www.w3school.com.cn/");
			CrawlUrl u4=new CrawlUrl("http://blog.csdn.net/shangboerds/article/details/7532676");
			CrawlUrl u5=new CrawlUrl("https://www.hacg.li");
			u3.setWeight(150);
			u4.setWeight(80);
			manager.put(u2.getUrl().getBytes("utf-8"),u5);
			manager.put(u1.getUrl().getBytes("utf-8"),u1);
			manager.put(u2.getUrl().getBytes("utf-8"),u2);
			manager.put(u3.getUrl().getBytes("utf-8"),u3);
			manager.put(u4.getUrl().getBytes("utf-8"),u4);
			manager.put(u5.getUrl().getBytes("utf-8"),u5);
			manager.delete(u2.getUrl().getBytes("utf-8"));
			
			/*DatabaseEntry searchKey=new DatabaseEntry(Util.intToByteArray(100));
			DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundValue = new DatabaseEntry();*/
//			manager.describe();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			manager.close();
		}		
	}

	private Db db=null;
	private String homePath=null;
	private Class objCls=null;
	private boolean allowDuplicates=false;
	
	private void setup(){
		if (db==null) {
			db=new Db(homePath,objCls,allowDuplicates);
			VersionUtils.log(11.14,"加载DbManager["+homePath+"]");
		}
	}
	public void close(){
		if (db!=null){
			db.close();
			db=null;
		}
	}
	
	/*数据库条目数*/
	public long size(){
		Cursor cursor;
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		OperationStatus retVal=null;
		cursor=db.mainDb.openCursor(null, null);
		if (cursor.getFirst(key, value, LockMode.DEFAULT)!=OperationStatus.SUCCESS) return 0;
		long size=cursor.skipNext(Long.MAX_VALUE, key, value, LockMode.DEFAULT);
		if (cursor!=null) cursor.close();
		return (size+1);
	}
	
	
	/*清除mainDb所有条目*/
	public void clearDb(){
	    Transaction txn=db.env.beginTransaction(null, db.txnConf);
		Cursor cursor;
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		OperationStatus retVal=null;
		cursor=db.mainDb.openCursor(txn, null);
		retVal=cursor.getFirst(key, value, LockMode.DEFAULT);
		for (;;){
           	if (retVal==OperationStatus.SUCCESS){
  					cursor.delete();
  			}
  			else break;
  			retVal=cursor.getNext(key,value, LockMode.DEFAULT);
  		}
		if (cursor!=null) cursor.close();
		if (txn!=null) txn.commit();
	}
	
	/*库是否为空*/
	public boolean isEmpty(){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getNext(key, value, LockMode.DEFAULT)==OperationStatus.NOTFOUND) return true;
			else return false;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*从库中取出一个条目,没有任何条目返回null*/
	public ValueObjectClass takeOut(){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getNext(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				ValueObjectClass valueObj=(ValueObjectClass)db.serialBinding.entryToObject(value);
				cursor.delete(); //移除当前条目
				return valueObj;
			}
			return null;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*存入条目,不允许重复数据时,已存在则返回OperationStatus.KEYEXIST
	 * 允许重复数据时,任何情况下都添加
	 */
	public OperationStatus put(String keyStr,ValueObjectClass valueObj){
		try {
			byte[] keyBytes=keyStr.getBytes("utf-8");
			return put(keyBytes,valueObj);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return OperationStatus.NOTFOUND;
		}
	}
	public OperationStatus put(byte[] keyBytes,ValueObjectClass valueObj){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key=new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		db.serialBinding.objectToEntry(valueObj, value);
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (!db.allowDuplicates) return cursor.putNoOverwrite(key, value);
			else return cursor.put(key, value);
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*根据key删除所有相应的条目*/
	public OperationStatus delete(byte[] keyBytes){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				if (!db.allowDuplicates) return cursor.delete();
				else for (;;){
					/*可能存在重复的情况*/
					OperationStatus retVal=cursor.delete();
					if (retVal==OperationStatus.KEYEMPTY) return retVal;
					retVal=cursor.getNextDup(key, value, LockMode.DEFAULT);
					if (retVal==OperationStatus.NOTFOUND) return OperationStatus.SUCCESS;
				}
			}
			else return OperationStatus.NOTFOUND;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*根据key取出一个相应的value,不存在返回null*/
	public ValueObjectClass get(byte[] keyBytes){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				ValueObjectClass valueObj=(ValueObjectClass)db.serialBinding.entryToObject(value);
				return valueObj;
			}
			return null;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	/*根据key所有取出相应的value,不存在返回null*/
	public List<ValueObjectClass> getDuplicates(byte[] keyBytes){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		List<ValueObjectClass> results=new ArrayList();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				ValueObjectClass valueObj=(ValueObjectClass)db.serialBinding.entryToObject(value);
				results.add(valueObj);
				for (;;){
					OperationStatus retVal=cursor.getNextDup(key, value, LockMode.DEFAULT);
					if (retVal==OperationStatus.SUCCESS){
						valueObj=(ValueObjectClass)db.serialBinding.entryToObject(value);
						results.add(valueObj);
					}
					else break;
				}
				return results;
			}
			else return null;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*根据key更新所有相应value*/
	public OperationStatus update(byte[] keyBytes,ValueObjectClass valueObj){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		OperationStatus retVal=null;
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				db.serialBinding.objectToEntry(valueObj, value);
				retVal=cursor.putCurrent(value);
				if (!db.allowDuplicates) return retVal;
				else for (;;){
					/*可能存在重复数据*/
					retVal=cursor.getNextDup(key, value, LockMode.DEFAULT);
					if (retVal==OperationStatus.NOTFOUND) return OperationStatus.SUCCESS;
					else cursor.putCurrent(value);
				}
			}
			else return OperationStatus.NOTFOUND;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	
	public void describe(){
		db.describe();
	}
	
	/*构造函数*/
	public DbManager(String homePath,Class objCls){
		this(homePath,objCls,false);
	}
	public DbManager(String homePath,Class objCls,boolean allowDuplicates){
		super();
		this.homePath = homePath;
		this.objCls=objCls;
		this.allowDuplicates=allowDuplicates;
		setup();
	}
	
	
	public Db getDb() {
		return db;
	}
	public String getHomePath() {
		return db.homePath;
	}
	public boolean isAllowDuplicates() {
		return db.allowDuplicates;
	}
	
}
