package com.kelles.crawler.crawler.download;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.kelles.crawler.crawler.database.DownloadTaskDbManager;
import com.kelles.crawler.crawler.setting.Setting;
import com.kelles.crawler.crawler.util.*;
import com.sleepycat.je.OperationStatus;

public class DownloadPool {
	
	public static void main(java.lang.String args[]) throws InterruptedException{
		DownloadPool pool=new DownloadPool(Setting.ROOT+"/DownloadPoolTest");
		pool.manager.clearDb();
		pool.setMaxThreads(5);
		pool.manager.clearDb();
		DownloadTask task=new DownloadTask.Builder()
				.toFile(Setting.ROOT+"/DownloadPoolPics","Neural Networks~ A Comprehensive Foundation",".pdf")
				.setReferer("http://cn.bing.com/academic/profile?id=1556847948&encoded=0&v=paper_preview&mkt=zh-cn")
				.addUrl("http://www.ejournal.unam.mx/cys/vol04-02/CYS04211.pdf")
				.addUrl("http://www.cys.cic.ipn.mx/ojs/index.php/CyS/article/download/942/1038")
				.addUrl("http://www.journals.unam.mx/index.php/cys/article/download/2512/2074")
				.build();
		DownloadTask task2=new DownloadTask.Builder()
				.toFile(Setting.ROOT+"/DownloadPoolPics","Deformation Prediction of Landslide Based on Improved Back-propagation Neural Network",".pdf")
				.setReferer("http://cn.bing.com/academic/profile?id=2091881089&encoded=0&v=paper_preview&mkt=zh-cn")
				.addUrl("https://www.researchgate.net/profile/Zhigang_Zeng/publication/257788412_Deformation_Prediction_of_Landslide_Based_on_Improved_Back-propagation_Neural_Network/links/54c83e740cf289f0ced06085.pdf")
				.addUrl("http://rd.springer.com/content/pdf/10.1007%2Fs12559-012-9148-1.pdf")
				.build();
		DownloadTask task3=new DownloadTask.Builder()
				.toFile(Setting.ROOT+"/DownloadPoolPics","Genetic Algorithms in Search of Optimization and Machine Learning",".pdf")
				.setReferer("http://cn.bing.com/academic/profile?id=2091881089&encoded=0&v=paper_preview&mkt=zh-cn")
				.addUrl("https://www.researchgate.net/profile/Zhigang_Zeng/publication/257788412_Deformation_Prediction_of_Landslide_Based_on_Improved_Back-propagation_Neural_Network/links/54c83e740cf289f0ced06085.pdf")
				.build();
		DownloadTask task4=new DownloadTask.Builder()
				.toFile(Setting.ROOT+"/DownloadPoolPics","Data Mining: Concepts and Techniques",".pdf")
				.setReferer("http://cn.bing.com/academic/profile?id=2091881089&encoded=0&v=paper_preview&mkt=zh-cn")
				.addUrl("http://www.gbv.de/dms/ilmenau/toc/01600020X.PDF")
				.addUrl("http://rd.springer.com/content/pdf/10.1007%2Fs12559-012-9148-1.pdf")
				.build();
		pool.addTask(task);
		pool.addTask(task2);
		pool.addTask(task3);
		pool.addTask(task4);
		Thread.sleep(30000);
	}
	
	private DownloadTaskDbManager manager=null;
	private final Mutex mutex=new Mutex();
	private List<DownloadThread> threads=new LinkedList(); //正在下载的任务
	private volatile DownloadPool blinker=this;
	private int maxThreads=1; //一次最多下载的线程数
	
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
				else for (DownloadThread thread:threads)
					Logger.log("[等待下载结束]"+thread.getTask().getToFile().getName());
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
	public void addTask(DownloadTask task){
		if (blinker==this){
			byte[] md5Bytes=task.getMd5Bytes();
			if (md5Bytes==null) return;
			synchronized(mutex){
				manager.put(md5Bytes, task); //先保存任务至数据库中,下载完成时再从数据库中移除
			}
			tryStart();
		}
		else{
			try{throw new Exception("线程池已关闭");}
			catch(Exception e){e.printStackTrace();}
		}
	}
	
	/*若没有任务在下载,开始下载*/
	public void tryStart(){
		synchronized(mutex){
			while (blinker==this && threads.size()<maxThreads){
				List<byte[]> listKeyBytes=new ArrayList();
				for (DownloadThread thread:threads)
					listKeyBytes.add(thread.getTask().getMd5Bytes());
				/*取出任务后,优先级自减;优先级过低的任务不再取出*/
				DownloadTask newTask=manager.takeoutNextNoDup(listKeyBytes,true); 
				if (newTask!=null){
					DownloadThread newThread=new DownloadThread(newTask);
					threads.add(newThread);
					newThread.start();
				}
				else break; //数据库中无可用任务
			}
		}
	}
	
	/*下载成功*/
	private void taskSuccess(DownloadTask task){
		byte[] md5Bytes=task.getMd5Bytes(); 
		synchronized(mutex){
			Logger.log(11.27,"[下载完成]"+task.getToFile().getName()); //
			 /*从数据库中移除该任务*/
			if (manager.delete(md5Bytes)!=OperationStatus.SUCCESS){
				throw new RuntimeException("[文件已下载,但未从数据库中正确移除]"+task.getToFile().getName());
			}
			/*从threads中移除*/
			for (DownloadThread thread:threads){
				byte[] threadKeyBytes=thread.getTask().getMd5Bytes();
				if (Util.byteArrayEquals(md5Bytes, threadKeyBytes)){
					threads.remove(thread);
					break;
				}
			}
		}
		tryStart();
	}
	
	/*下载失败,降低优先级但不删除任务*/
	private void taskFailure(DownloadTask task){
		byte[] md5Bytes=task.getMd5Bytes(); 
		synchronized(mutex){
			Logger.log(11.27,"[下载失败]"+task.getToFile().getName()); //
			/*从数据库中降低该任务优先级*/
			if (manager.updateWeight(md5Bytes, task.getWeight()-1)!=OperationStatus.SUCCESS){
				throw new RuntimeException("[文件下载失败,且未从数据库中正确降低优先级]"+task.getToFile().getName());
			}
			/*从threads中移除*/
			for (DownloadThread thread:threads){
				byte[] threadKeyBytes=thread.getTask().getMd5Bytes();
				if (Util.byteArrayEquals(md5Bytes, threadKeyBytes)){
					threads.remove(thread);
					break;
				}
			}
		}
		tryStart();
	}
	
	/*下载线程*/
	private class DownloadThread extends Thread{
		private DownloadTask task=null;

		@Override
		public void run() {
			Logger.log("[开始下载]"+task.getToFile().getName()); //
			if (task.startDownload()) taskSuccess(task); //下载完成
			else taskFailure(task); //下载失败
		}

		public DownloadThread(DownloadTask task) {
			super();
			this.task = task;
		}

		public DownloadTask getTask() {
			return task;
		}
	}
	
	
	/*构造函数*/
	public DownloadPool(java.lang.String homePath) {
		super();
		manager=new DownloadTaskDbManager(homePath);
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

	public DownloadTaskDbManager getManager() {
		return manager;
	}


	
	
	
}
