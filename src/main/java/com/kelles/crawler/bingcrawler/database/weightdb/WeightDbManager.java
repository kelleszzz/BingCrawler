package com.kelles.crawler.bingcrawler.database.weightdb;

import java.util.ArrayList;
import java.util.List;

import com.kelles.crawler.bingcrawler.dataanalysis.bean.Keyword;
import com.kelles.crawler.bingcrawler.setting.Setting;
import com.kelles.crawler.bingcrawler.util.*;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

public class WeightDbManager <ValueObjectClass extends WeightInterface>{
	
	public static void main(java.lang.String[] args) throws Exception{
		WeightDbManager<Keyword> manager=new WeightDbManager<Keyword>(Setting.ROOT+"/DataAnalysis/WeightDbManagerTest",Keyword.class);
		manager.clearDb();
		Keyword a1=new Keyword("Tom");
		a1.getProfiles().add("article1");
		a1.getProfiles().add("article2");
		a1.setWeight(100);
		
		Keyword a2=new Keyword("To");
		a2.getProfiles().add("article3");
		a2.getProfiles().add("article4");
		a2.setWeight(101);
		
		Keyword a3=new Keyword("Too");
		a3.getProfiles().add("article5");
		a3.getProfiles().add("article6");
		a3.setWeight(102);
		
		manager.put(a1.getName().getBytes("utf-8"), a1);
		manager.put(a2.getName().getBytes("utf-8"), a2);
		manager.put(a3.getName().getBytes("utf-8"), a3);
		
		manager.describe();
		manager.close();
	}
	
	protected WeightDb<ValueObjectClass> db=null;
	protected java.lang.String homePath=null;
	protected Class objCls;
	
	protected void setup(){
		if (db==null) {
			db=new WeightDb(homePath,objCls);
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
		ValueObjectClass valueObj=get(keyBytes);
		if (valueObj!=null){
			valueObj.setWeight(weight);
			return update(keyBytes,valueObj);
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
	
	

	/*根据weight从库中获取前n个条目,没有任何条目返回null*/
	public ValueObjectClass getNext(){
		List<ValueObjectClass> nextObjs=getNext(1);
		if (nextObjs==null) return null;
		else return nextObjs.get(0);
	}
	public List<ValueObjectClass> getNext(int nextCount){
		Transaction txn=db.env.beginTransaction(null, db.txnConf);
		DatabaseEntry searchKey = new DatabaseEntry();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundValue=new DatabaseEntry();
		List<ValueObjectClass> nextObjs=new ArrayList();
		SecondaryCursor secCursor=null;
		try{
			secCursor=db.secDbByWeight.openCursor(txn, null);
			if (secCursor.getLast(searchKey,foundKey, foundValue, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				for (int i=0;i<nextCount;i++){
					ValueObjectClass valueObj=(ValueObjectClass)db.serialBinding.entryToObject(foundValue);
					nextObjs.add(valueObj);
					OperationStatus retVal=secCursor.getPrev(searchKey,foundKey, foundValue, LockMode.DEFAULT);
					if (retVal!=OperationStatus.SUCCESS) break;
				}
				
				return nextObjs;
			}
			return null;
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	
	
	/*存入条目,不允许重复数据时,已存在则返回OperationStatus.KEYEXIST
	 * 允许重复数据时,任何情况下都添加
	 */
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
		catch(Exception e){
			e.printStackTrace();
			return OperationStatus.KEYEMPTY;
		}
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
		catch(Exception e){
			e.printStackTrace();
			return OperationStatus.KEYEMPTY;
		}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	
	public void describe(){
		db.describe();
	}
	
	/*构造函数*/
	public WeightDbManager(java.lang.String homePath, Class objCls){
		super();
		this.homePath = homePath;
		this.objCls=objCls;
		setup();
	}
	
	public java.lang.String getHomePath() {
		return db.homePath;
	}
	public boolean isAllowDuplicates() {
		return db.allowDuplicates;
	}
	public WeightDb getDb() {
		return db;
	}
	
	
}
