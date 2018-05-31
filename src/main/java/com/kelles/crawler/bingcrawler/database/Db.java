package com.kelles.crawler.bingcrawler.database;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.kelles.crawler.bingcrawler.util.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.TransactionConfig;

public class Db{
	protected String homePath=null; //数据库文件夹
	public static String MAINDB_PATH="main_db";
	public static String CLASSCATALOGDB_PATH="classcatalog_db";
	public static String WEIGHTSECDB_PATH="weightsecdb";
	protected static boolean readOnly=false;
	protected boolean allowDuplicates=false;

	protected Environment env;
	protected EnvironmentConfig envConf=new EnvironmentConfig();
	protected DatabaseConfig dbConf=new DatabaseConfig();;
	protected Database mainDb;
	protected Database classCatalogDb;
	protected StoredClassCatalog classCatalog;
	protected SerialBinding serialBinding;
	protected Class objCls;
	protected TransactionConfig txnConf; //事务设置,通过事务同步至disk
	
	//用于close函数
	private Set<Database> databasesToClose=new HashSet();
		
	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		super.finalize();
		close();
	}

	protected void setup(){
		File file=new File(homePath);
		if (!file.isDirectory()) file.mkdirs();
		
		envConf.setAllowCreate(!readOnly);
		envConf.setReadOnly(readOnly);
		envConf.setTransactional(true); //事务
		env=new Environment(file,envConf);
		
		dbConf.setAllowCreate(!readOnly);
		dbConf.setReadOnly(readOnly);
		dbConf.setSortedDuplicates(allowDuplicates); //是否允许重复数据
		dbConf.setTransactional(true); //事务
		
		mainDb=env.openDatabase(null, MAINDB_PATH, dbConf);
		
		dbConf.setSortedDuplicates(false);
		classCatalogDb=env.openDatabase(null, CLASSCATALOGDB_PATH, dbConf);
		dbConf.setSortedDuplicates(allowDuplicates);
		classCatalog=new StoredClassCatalog(classCatalogDb);
		serialBinding=new SerialBinding(classCatalog,objCls);
		
		/*事务设置*/
		Durability tranDura = 
		        new Durability(Durability.SyncPolicy.SYNC, 
		                       null,    // unused by non-HA applications. 
		                       null);   // unused by non-HA applications.
	    txnConf = new TransactionConfig();
	    txnConf.setDurability(tranDura);
	    
	    databasesToClose.add(mainDb);
		databasesToClose.add(classCatalogDb);
	}
	
	public void close(){
		try{
			Set<Database> secDbs=new HashSet();
			//先关闭SecondaryDatabase
			for (Database databaseToClose:databasesToClose) 
				if (databaseToClose instanceof SecondaryDatabase) {
					databaseToClose.close();
					secDbs.add(databaseToClose);
				}
			databasesToClose.removeAll(secDbs);
			//再关闭PrimaryDatabase
			for (Database databaseToClose:databasesToClose) databaseToClose.close();
			databasesToClose.clear();
			//关闭环境
			env.close();
		}
		catch (Exception e){}
	}
	
	public void describe(){
		if (Logger.check(1)){
			Logger.log("数据库路径: "+env.getHome().getAbsolutePath());
			List<String> dbNames=env.getDatabaseNames();
			Logger.log("数据库名: "+dbNames);
		}
		describeDb(mainDb);
	}
	
	private void describeDb(Database db){
		Cursor cursor=null;
		try{
			Logger.log("数据库"+db.getDatabaseName()+"包含Url:");
			cursor=db.openCursor(null, null);
			DatabaseEntry key=new DatabaseEntry();
			DatabaseEntry value=new DatabaseEntry();
			int i=0;
			while (cursor.getNext(key, value, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				try {
					Logger.log("["+(++i)+"]"+"key = "+new String(key.getData(),"utf-8"));
				} catch (UnsupportedEncodingException e) {}
				Logger.log("value = "+serialBinding.entryToObject(value));
			};
		}
		finally{
			if (cursor!=null) cursor.close();
		}
	}
	
	

	public Database getMainDb() {
		return mainDb;
	}
	
	

	public SerialBinding getSerialBinding() {
		return serialBinding;
	}

	public TransactionConfig getTxnConf() {
		return txnConf;
	}

	public Environment getEnv() {
		return env;
	}

	/*构造函数*/
	public Db(String homePath,Class objClass) {
		this(homePath,objClass,false);
	}
	public Db(String homePath,Class objClass,boolean allowDuplicates) {
		super();
		this.homePath = homePath;
		this.objCls=objCls;
		this.allowDuplicates=allowDuplicates;
		setup();
	}
}
