package com.kelles.crawler.bingcrawler.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.kelles.crawler.bingcrawler.download.DownloadTask;
import com.kelles.crawler.bingcrawler.util.*;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

public class DownloadTaskDbManager{
	
	public static void main(String[] args) throws Exception{
		DownloadTaskDbManager manager=null;
		try{
			manager=new DownloadTaskDbManager("DbManagerTest");
			manager.clearDb();
			DownloadTask task1=new DownloadTask.Builder()
					.toFile("DbManagerTest",new Random().nextInt(100)+"",".html")
					.addUrl("http://cn.bing.com/academic/profile?id=2114296561&encoded=0&v=paper_preview&mkt=zh-cn")
					.build();
			DownloadTask task2=new DownloadTask.Builder()
					.toFile("DbManagerTest",new Random().nextInt(100)+"",".html")
					.addUrl("http://cs.nyu.edu/courses/spring15/CSCI-GA.3033-011/ViewofCloud.pdf")
					.build();
			DownloadTask task3=new DownloadTask.Builder()
					.toFile("DbManagerTest",new Random().nextInt(100)+"",".html")
					.addUrl("https://www.bing.com/search?q=httpclient+setSocketTimeout&pc=MOZI&form=MOZSBR")
					.build();
			DownloadTask task4=new DownloadTask.Builder()
					.toFile("DbManagerTest",new Random().nextInt(100)+"",".html")
					.addUrl("http://www.baeldung.com/httpclient-timeout")
					.build();
			manager.put(task1.getMd5Bytes(),task1);
			manager.put(task2.getMd5Bytes(),task2);
			manager.put(task3.getMd5Bytes(),task3);
			manager.put(task4.getMd5Bytes(),task4);
			manager.updateWeight(task1.getToFile().getName().getBytes("utf-8"), 120);

			
			List<byte[]> listBytes=new ArrayList();
			listBytes.add(task1.getToFile().getName().getBytes("utf-8"));
			listBytes.add(task2.getToFile().getName().getBytes("utf-8"));
			listBytes.add(task3.getToFile().getName().getBytes("utf-8"));
			System.out.println("获取weight最高的条目:\n"+manager.takeoutNextNoDup(listBytes));
			
			manager.describe();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			manager.close();
		}		
	}
	
	private DownloadTaskDb db=null;
	private String homePath=null;
	/*作用于takeoutNextNoDup,优先级过低时不再取出*/
	private int priorityBottomLine=DownloadTask.DEFAULT_WEIGHT-6; 
	
	private void setup(){
		if (db==null) {
			db=new DownloadTaskDb(homePath);
			Logger.log(11.14,"加载DbManager["+homePath+"]");
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
	
	/*更新相应weight值*/
	public OperationStatus updateWeight(byte[] keyBytes,int weight){
		DownloadTask task=get(keyBytes);
		if (task!=null){
			task.setWeight(weight);
			return update(keyBytes,task);
		}
		return OperationStatus.NOTFOUND;
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
	
	/*根据weight从库中获取一个不和传入参数相同的条目,没有任何条目返回null
	 * 若priorityDown为true,取出时优先级减1
	 */
	public DownloadTask takeoutNextNoDup(List<byte[]> listKeyBytes){
		return takeoutNextNoDup(listKeyBytes,false);
	}
	public DownloadTask takeoutNextNoDup(List<byte[]> listKeyBytes,boolean priorityDown){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry searchKey = new DatabaseEntry();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundValue=new DatabaseEntry();
		DownloadTask valueObj=null;
		SecondaryCursor secCursor=null;
		try{
			secCursor=db.secDbByWeight.openSecondaryCursor(txn, null);
			if (secCursor.getLast(searchKey,foundKey, foundValue, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				for (boolean noDup;;){
					noDup=true;
					byte[] curBytes=foundKey.getData();
					if (listKeyBytes!=null)
						for (byte[] existedBytes:listKeyBytes)
							if (Util.byteArrayEquals(curBytes, existedBytes)){
								/*出现了重复条目*/
								noDup=false;
								break;
							}
					/*不重复的DownloadTask*/
					if (noDup){
						valueObj=(DownloadTask)db.serialBinding.entryToObject(foundValue);
						if (valueObj.getWeight()<priorityBottomLine) return null; //所有随后的任务优先级过低
						else return valueObj;
					}
					/*没有更多条目了*/
					if (secCursor.getPrev(searchKey,foundKey, foundValue, LockMode.DEFAULT)!=OperationStatus.SUCCESS)
						return null;
				}
			}
			return null;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
			/*优先级自减*/
			if (priorityDown && valueObj!=null)
				if (updateWeight(foundKey.getData(),valueObj.getWeight()-1)!=OperationStatus.SUCCESS){
					throw new RuntimeException("优先级自减失败");
				}
		}
	}
	
	
	/*测试,输出所有输入key和计算key不一样的条目*/
	public void testShowAllError(){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry searchKey = new DatabaseEntry();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundValue=new DatabaseEntry();
		SecondaryCursor secCursor=null;
		try{
			secCursor=db.secDbByWeight.openCursor(txn, null);
			for (;;){
				if (secCursor.getNext(searchKey,foundKey, foundValue, LockMode.DEFAULT)!=OperationStatus.SUCCESS)
					return;
				DownloadTask valueObj=(DownloadTask)db.serialBinding.entryToObject(foundValue);
				if (!Util.byteArrayEquals(foundKey.getData(), valueObj.getMd5Bytes())){
					System.out.println("数据库key: "+new String(foundKey.getData(),"utf-8")); //
					System.out.println("数据库计算出md5: "+new String(valueObj.getMd5Bytes(),"utf-8")); //
					System.out.println("数据库task: "+valueObj); //
				}
			}
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*根据weight从库中获取一个条目,没有任何条目返回null*/
	public DownloadTask getNext(){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry searchKey = new DatabaseEntry();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundValue=new DatabaseEntry();
		SecondaryCursor secCursor=null;
		try{
			secCursor=db.secDbByWeight.openCursor(txn, null);
			if (secCursor.getLast(searchKey,foundKey, foundValue, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				DownloadTask valueObj=(DownloadTask)db.serialBinding.entryToObject(foundValue);
				return valueObj;
			}
			return null;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	/*存入条目,不允许重复数据时,已存在则返回OperationStatus.KEYEXIST
	 * 允许重复数据时,任何情况下都添加
	 */
	public OperationStatus put(byte[] keyBytes,DownloadTask valueObj){
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
	public DownloadTask get(byte[] keyBytes){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				DownloadTask valueObj=(DownloadTask)db.serialBinding.entryToObject(value);
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
	public List<DownloadTask> getDuplicates(byte[] keyBytes){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry key = new DatabaseEntry(keyBytes);
		DatabaseEntry value=new DatabaseEntry();
		List<DownloadTask> results=new ArrayList();
		Cursor cursor=null;
		try{
			cursor=db.mainDb.openCursor(txn, null);
			if (cursor.getSearchKey(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				DownloadTask valueObj=(DownloadTask)db.serialBinding.entryToObject(value);
				results.add(valueObj);
				for (;;){
					OperationStatus retVal=cursor.getNextDup(key, value, LockMode.DEFAULT);
					if (retVal==OperationStatus.SUCCESS){
						valueObj=(DownloadTask)db.serialBinding.entryToObject(value);
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
	public OperationStatus update(byte[] keyBytes,DownloadTask valueObj){
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
	public DownloadTaskDbManager(String homePath){
		super();
		this.homePath = homePath;
		setup();
	}
	
	public String getHomePath() {
		return db.homePath;
	}
	public boolean isAllowDuplicates() {
		return db.allowDuplicates;
	}
	public DownloadTaskDb getDb() {
		return db;
	}
	public int getPriorityBottomLine() {
		return priorityBottomLine;
	}
	public void setPriorityBottomLine(int priorityBottomLine) {
		this.priorityBottomLine = priorityBottomLine;
	}
	
	
}
