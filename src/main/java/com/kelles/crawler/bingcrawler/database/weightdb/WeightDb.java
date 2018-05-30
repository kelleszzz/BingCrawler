package com.kelles.crawler.bingcrawler.database.weightdb;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.kelles.crawler.bingcrawler.util.VersionUtils;
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
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class WeightDb <ValueObjectClass extends WeightInterface>{
	protected String homePath=null; //数据库文件夹
	public static String MAINDB_PATH="main_db";
	public static String CLASSCATALOGDB_PATH="classcatalog_db";
	protected static boolean readOnly=false;
	protected static boolean allowDuplicates=false;

	protected Environment env;
	protected EnvironmentConfig envConf=new EnvironmentConfig();
	protected DatabaseConfig dbConf=new DatabaseConfig();;
	protected Database mainDb;
	protected Database classCatalogDb;
	protected StoredClassCatalog classCatalog;
	protected SerialBinding serialBinding;
	protected TransactionConfig txnConf; //事务设置,通过事务同步至disk
	protected Class objCls;
	
	
	//用于close函数
	private Set<Database> databasesToClose=new HashSet();
	
	/*weight的SecondaryDatabase*/
	public static String WEIGHTSECDB_PATH="weightsecdb";
	protected SecondaryDatabase secDbByWeight;
		
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
		dbConf.setTransactional(true); //事务
		
		mainDb=env.openDatabase(null, MAINDB_PATH, dbConf);
		
		classCatalogDb=env.openDatabase(null, CLASSCATALOGDB_PATH, dbConf);
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
		
		/*创建weight的secondaryKey*/
		WeightKeyCreator<ValueObjectClass> weightKeyCreator=new WeightKeyCreator<ValueObjectClass>(serialBinding);
		SecondaryConfig weightSecConf=getSecConf(weightKeyCreator);
		secDbByWeight=env.openSecondaryDatabase(null, WEIGHTSECDB_PATH, mainDb, weightSecConf);
		databasesToClose.add(secDbByWeight);
	}
	
	protected SecondaryConfig getSecConf(SecondaryKeyCreator secKeyCreator){
		SecondaryConfig secConf=new SecondaryConfig();
		secConf.setTransactional(true); //事务
		secConf.setAllowPopulate(true);
		secConf.setAllowCreate(!readOnly);
		secConf.setReadOnly(readOnly);
		secConf.setKeyCreator(secKeyCreator);
		secConf.setSortedDuplicates(true);
		return secConf;
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
		if (VersionUtils.check(1)){
			VersionUtils.log("数据库路径: "+env.getHome().getAbsolutePath());
			List<String> dbNames=env.getDatabaseNames();
			VersionUtils.log("数据库名: "+dbNames);
		}
		describeDb(mainDb);
	}
	
	private void describeDb(Database db){
		Transaction txn=env.beginTransaction(null, txnConf);
		DatabaseEntry searchKey = new DatabaseEntry();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundValue=new DatabaseEntry();
		SecondaryCursor secCursor=null;
		try{
			secCursor=secDbByWeight.openCursor(txn, null);
			if (secCursor.getLast(searchKey,foundKey, foundValue, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				for (int i=0;;){
					ValueObjectClass valueObj=(ValueObjectClass)serialBinding.entryToObject(foundValue);
					VersionUtils.log("["+(++i)+"]"+"key = "+new String(foundKey.getData(),"utf-8"));
					VersionUtils.log("value = "+serialBinding.entryToObject(foundValue));
					OperationStatus retVal=secCursor.getPrev(searchKey,foundKey, foundValue, LockMode.DEFAULT);
					if (retVal!=OperationStatus.SUCCESS) break;
				}
				
			}
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
		}
	}
	
	

	/*构造函数*/
	public WeightDb(String homePath,Class objCls) {
		super();
		this.homePath = homePath;
		this.objCls=objCls;
		setup();
	}

	public Environment getEnv() {
		return env;
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

	public SecondaryDatabase getSecDbByWeight() {
		return secDbByWeight;
	}

	
	
}
