package com.kelles.crawler.crawler.threadpool;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.kelles.crawler.crawler.setting.Setting;
import com.kelles.crawler.crawler.util.*;
import com.kelles.crawler.crawler.bean.*;
import com.kelles.crawler.crawler.dataanalysis.bean.*;
import com.kelles.crawler.crawler.database.weightdb.WeightDbManager;
import com.sleepycat.je.OperationStatus;

public class ThreadPool<TaskObjectType extends TaskInterface> {
	
	public static void main(java.lang.String[] args) throws InterruptedException{
		java.lang.String profilesDir="D:\\Others\\Workplace\\Java\\2016\\GoCrawl\\MyData\\Profiles";
		ThreadPool<ProfileSimHashTask> pool=
				new ThreadPool<ProfileSimHashTask>(Setting.ROOT+ Setting.DATA_ANALYSIS+"/ThreadPoolTest", ProfileSimHashTask.class);
		pool.getManager().clearDb();
		pool.setMaxThreads(4);
		Profile profile;
		
		profile=new Profile();
		profile.setTitle("[Neural Networks~ A Review from Statistical Perspective]~ Rejoinder");
		ProfileSimHashTask task1=new ProfileSimHashTask(new ProfileAbstract(profile));
		task1.setWeight(4);
		
		profile=new Profile();
		profile.setTitle("‘Special agents’ trigger social waves in giant honeybees (Apis dorsata)");
		ProfileSimHashTask task2=new ProfileSimHashTask(new ProfileAbstract(profile));
		task2.setWeight(3);
		
		profile=new Profile();
		profile.setTitle("“Squeaky Wheel” optimization");
		ProfileSimHashTask task3=new ProfileSimHashTask(new ProfileAbstract(profile));
		task3.setWeight(2);
		
		profile=new Profile();
		profile.setTitle("3-D Image Denoising by Local Smoothing and Nonparametric Regression");
		ProfileSimHashTask task4=new ProfileSimHashTask(new ProfileAbstract(profile));
		task4.setWeight(1);
		
		pool.addTask(task1);
		pool.addTask(task2);
		pool.addTask(task3);
		pool.addTask(task4);
	}
	
	private TaskWeightDbManager<TaskObjectType> manager=null;
	private final Mutex mutex=new Mutex();
	private List<TaskThread> threads=new LinkedList(); //正在下载的任务
	private volatile ThreadPool blinker=this;
	private Class objCls;
	private java.lang.String homePath;
	private int maxThreads=1; //一次最多下载的线程数
	
	public long remainingTaskCount(){
		synchronized (mutex){
			return manager.size();
		}
	}
	
	public int currentThreadCount(){
		synchronized (mutex){
			if (blinker==this) return threads.size();
			else return -1;
		}
	}
	
	/*等待当前下载任务完成后关闭线程池*/
	public void close(){
		if (blinker==this){
			blinker=null;
			/*首次尝试关闭,输出正在下载的线程*/
			synchronized (mutex){
				if (threads.size()==0) {
					manager.close();
					manager=null;
					return;
				}
				else for (TaskThread thread:threads)
					Logger.log("[等待任务结束]"+thread.getTask().describe());
			}
			new Thread(new Runnable(){
				@Override
				public void run() {
					for (;;){
						try {Thread.sleep(500);} catch (InterruptedException e) {e.printStackTrace();}
						synchronized (mutex){
							if (threads.size()==0) {
//								Logger.log(11.27,"线程池"+manager.getHomePath()+"关闭");
								manager.close();
								manager=null;
								return;
							}
						}
					}
				}
			}).start();
		}
		else{
			try{throw new Exception("线程池已关闭");}
			catch(Exception e){e.printStackTrace();}
		}
	}

	
	public void describe(){
		manager.describe();
	}
	
	/*添加一个任务*/
	public boolean addTask(TaskObjectType task){
		if (blinker==this){
			byte[] keyBytes=task.getKeyBytes();
			if (keyBytes==null) return false;
			synchronized(mutex){
				/*先保存任务至数据库中,下载完成时再从数据库中移除*/
				if (manager.put(keyBytes, task)==OperationStatus.SUCCESS) {
					tryStart();
					return true;
				}
			}
		}
		else{
			try{throw new Exception("线程池已关闭");}
			catch(Exception e){e.printStackTrace();}
		}
		return false;
	}
	
	/*若没有任务在下载,开始下载*/
	public void tryStart(){
		synchronized(mutex){
			while (blinker==this && threads.size()<maxThreads){
				List<byte[]> listKeyBytes=new ArrayList();
				for (TaskThread thread:threads)
					listKeyBytes.add(thread.getTask().getKeyBytes());
				/*取出任务后,优先级自减;优先级过低的任务不再取出*/
				TaskObjectType newTask=manager.takeoutNextNoDup(listKeyBytes, true);
				if (newTask!=null){	
					manager.updateWeight(newTask.getKeyBytes(), newTask.getWeight()-1);
					/*检测现有线程是否有已经在进行的任务*/
					boolean existsThread=false;
					if (!existsThread){
						TaskThread newThread=new TaskThread(newTask);
						threads.add(newThread);
						newThread.start();
					}
				}
				else break; //数据库中无可用任务
			}
		}
	}
	
	/*下载成功*/
	private void taskSuccess(TaskObjectType task){
		byte[] keyBytes=task.getKeyBytes(); 
		synchronized(mutex){
//			Logger.log(12.11,"[任务完成]"+task.describe()); //
			 /*从数据库中移除该任务*/
			if (manager.delete(keyBytes)!=OperationStatus.SUCCESS){
				throw new RuntimeException("[任务完成,但未从数据库中正确移除]"+task.getKeyBytes());
			}
			/*从threads中移除*/
			for (TaskThread thread:threads){
				byte[] threadKeyBytes=thread.getTask().getKeyBytes();
				if (Util.byteArrayEquals(keyBytes, threadKeyBytes)){
					threads.remove(thread);
					break;
				}
			}
		}
		tryStart();
	}
	
	/*下载失败,降低优先级但不删除任务*/
	private void taskFailure(TaskObjectType task){
		byte[] keyBytes=task.getKeyBytes();
		synchronized(mutex){
			/*从数据库中降低该任务优先级*/
			if (manager.updateWeight(keyBytes, task.getWeight()-1)!=OperationStatus.SUCCESS){
				throw new RuntimeException("[任务失败,且未从数据库中正确降低优先级]"+task.describe());
			}
			/*从threads中移除*/
			for (TaskThread thread:threads){
				byte[] threadKeyBytes=thread.getTask().getKeyBytes();
				if (Util.byteArrayEquals(keyBytes, threadKeyBytes)){
					threads.remove(thread);
					break;
				}
			}
		}
		tryStart();
	}
	
	/*下载线程*/
	private class TaskThread extends Thread{
		private TaskObjectType task=null;

		@Override
		public void run() {
			try{
				if (task.execute()) {
					/*下载完成*/
					taskSuccess(task);
				}
				else{
					/*下载失败*/
					taskFailure(task);
				}
			}
			catch(Exception e){
				e.printStackTrace();
				taskFailure(task);
			}
		}

		public TaskThread(TaskObjectType task) {
			super();
			this.task = task;
		}

		public TaskObjectType getTask() {
			return task;
		}
	}
	

	
	/*构造函数*/
	public ThreadPool(java.lang.String homePath, Class objCls) {
		super();
		this.objCls=objCls;
		this.homePath=homePath;
		setup();
	}
	private void setup(){
		manager=new TaskWeightDbManager<TaskObjectType>(homePath, objCls);
	}

	
	/*Mutex*/
	private static class Mutex{}
	
	/*数据库路径*/
	public java.lang.String getHomePath(){
		return manager.getHomePath();
	}

	public int getMaxThreads() {
		return maxThreads;
	}

	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}

	public WeightDbManager<TaskObjectType> getManager() {
		return manager;
	}


	public int getPriorityBottomLine() {
		return manager.getPriorityBottomLine();
	}

	public void setPriorityBottomLine(int priorityBottomLine) {
		manager.setPriorityBottomLine(priorityBottomLine);
	}


	
	
	
}
