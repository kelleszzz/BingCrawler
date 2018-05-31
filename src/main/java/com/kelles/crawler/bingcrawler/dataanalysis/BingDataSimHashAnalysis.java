package com.kelles.crawler.bingcrawler.dataanalysis;

import com.kelles.crawler.bingcrawler.dataanalysis.bean.ProfileAbstract;
import com.kelles.crawler.bingcrawler.dataanalysis.bean.ProfileSimHash;
import com.kelles.crawler.bingcrawler.database.Db;
import com.kelles.crawler.bingcrawler.database.DbManager;
import com.kelles.crawler.bingcrawler.database.weightdb.WeightDbManager;
import com.kelles.crawler.bingcrawler.setting.Setting;
import com.kelles.crawler.bingcrawler.threadpool.ThreadPool;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.kelles.crawler.bingcrawler.dataanalysis.bean.ProfileSimHashTask;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BingDataSimHashAnalysis {
	
	protected DbManager<Profile> profilesManager=null;
	public final static java.lang.String BING_PARSER_PROFILESDB_PATH= Setting.ROOT+"bing_profilesdb_home"; //Profiles数据库文件夹路径
	public final static java.lang.String PROFILES_PATH= Setting.ROOT+"Profiles"; //下载论文文件夹
	
	
	/*SimHash分析*/
	public static final java.lang.String BINGDATAANALYSIS_SIMHASHPOOL_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/simhash_analysis/simhash_pool";
	public static final java.lang.String BINGDATAANALYSIS_SIMHASHMANAGER_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/simhash_analysis/simhash_manager";
	private ThreadPool<ProfileSimHashTask> simHashPool=null;
	private WeightDbManager<ProfileSimHash> simHashManager=null;
	
	
	public static void main(java.lang.String[] args) throws Exception{
		DbManager<Profile> profilesManager=new DbManager<Profile>(BING_PARSER_PROFILESDB_PATH,Profile.class);
		WeightDbManager<ProfileSimHash> simHashManager=new WeightDbManager<ProfileSimHash>(BINGDATAANALYSIS_SIMHASHMANAGER_PATH,ProfileSimHash.class);
		BingDataSimHashAnalysis analysis=new BingDataSimHashAnalysis(profilesManager,simHashManager);
		
		Logger.log("[条目数]"+simHashManager.size()); //
		
		analysis.exportData();
		analysis.simHashPool.tryStart();
		
		for (;;){
			Thread.sleep(5000);
			int currentThreadCount=analysis.currentThreadCount();
			if (currentThreadCount==0) {
				simHashManager.describe();
				break;
			}
			else if (currentThreadCount<0) break;
			else{
				Logger.log("[等待SimHash计算完]"+currentThreadCount);
			}
		}
		analysis.close();
		profilesManager.close();
		simHashManager.close();
	}
	
	public void exportData(){
		exportData(Integer.MAX_VALUE);
	}
	public void exportData(int dataCount){
		Db db=profilesManager.getDb();
		/*导出数据*/
		Transaction txn=db.getEnv().beginTransaction(null, db.getTxnConf());
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		Cursor cursor=null;
		try{
			cursor=db.getMainDb().openCursor(txn, null);
			for (OperationStatus retVal=null;;){
				retVal=cursor.getNext(key, value, LockMode.DEFAULT);
				if (retVal!=OperationStatus.SUCCESS) break;
				Profile valueObj=(Profile)db.getSerialBinding().entryToObject(value);
				exportProfile(valueObj);
				if ((--dataCount)<=0) return;
			}
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (cursor!=null) cursor.close();
			if (txn!=null) txn.commit();
		}
	}
	private static int profilesCount=0;
	private void exportProfile(Profile profile){
		profilesCount++;
		/*SimHash分析*/
		try{
			if (simHashManager.get(profile.getTitle().getBytes("utf-8"))==null){
				Logger.log("[导出第"+profilesCount+"个SimHash]"); //
				ProfileSimHashTask task=new ProfileSimHashTask(new ProfileAbstract(profile));
				simHashPool.addTask(task);
			}
		}
		catch(Exception e){e.printStackTrace();}
	}
	
	public long remainingTaskCount(){
		return simHashPool.remainingTaskCount();
	}
	
	public int currentThreadCount(){
		return simHashPool.currentThreadCount();
	}
	
	/*private void setup(){
		profilesManager=new DbManager<Profile>(BING_PARSER_PROFILESDB_PATH,Profile.class);
		simHashPool=new ThreadPool<ProfileSimHashTask>(BINGDATAANALYSIS_SIMHASHPOOL_PATH,ProfileSimHashTask.class);
		simHashPool.setMaxThreads(5);
		simHashPool.setPriorityBottomLine(90);
		simHashManager=new DbManager<ProfileSimHash>(BINGDATAANALYSIS_SIMHASHMANAGER_PATH,ProfileSimHash.class);
		ProfileSimHashTask.setup(PROFILES_PATH, simHashManager);
	}*/
	public void setup(){
		simHashPool=new ThreadPool<ProfileSimHashTask>(BINGDATAANALYSIS_SIMHASHPOOL_PATH,ProfileSimHashTask.class);
		simHashPool.getManager().clearDb();
		simHashPool.setMaxThreads(5);
		simHashPool.setPriorityBottomLine(98);
		ProfileSimHashTask.setup(PROFILES_PATH, simHashManager);
	}
	
	public void close(){
		if (simHashPool!=null) simHashPool.close();
		/*if (simHashManager!=null)
			for (;;){
				if (simHashPool.currentThreadCount()<=0) {
					simHashManager.close();
					break;
				}
				try {Thread.sleep(500);} catch (InterruptedException e) {e.printStackTrace();}
			}*/
	}
	public BingDataSimHashAnalysis(DbManager<Profile> profilesManager,WeightDbManager<ProfileSimHash> simHashManager) {
		this.simHashManager=simHashManager;
		this.profilesManager=profilesManager;
		setup();
	}

	public ThreadPool<ProfileSimHashTask> getSimHashPool() {
		return simHashPool;
	}
	
	
}
