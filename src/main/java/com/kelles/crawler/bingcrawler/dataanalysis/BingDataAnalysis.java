package com.kelles.crawler.bingcrawler.dataanalysis;


import java.io.UnsupportedEncodingException;
import java.util.List;

import com.kelles.crawler.bingcrawler.analysis.CommonAnalysis;
import com.kelles.crawler.bingcrawler.dataanalysis.bean.*;
import com.kelles.crawler.bingcrawler.database.Db;
import com.kelles.crawler.bingcrawler.database.DbManager;
import com.kelles.crawler.bingcrawler.database.UrlsDbManager;
import com.kelles.crawler.bingcrawler.database.weightdb.WeightDbManager;
import com.kelles.crawler.bingcrawler.setting.Setting;
import org.apache.http.util.TextUtils;

import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BingDataAnalysis {
	
	protected UrlsDbManager urlsManager=null;
	protected DbManager<Profile> profilesManager=null;
	
	public final static java.lang.String BING_PARSER_URLSDB_PATH= Setting.ROOT+"bing_urlsdb_home"; //Urls数据库文件夹路径
	public final static java.lang.String BING_PARSER_PROFILESDB_PATH= Setting.ROOT+"bing_profilesdb_home"; //Profiles数据库文件夹路径
	public final static java.lang.String BING_PARSER_DOWNLOADPOOL_PATH= Setting.ROOT+"bing_downloadpool_home"; //下载线程池数据库地址
	public final static java.lang.String PROFILES_PATH= Setting.ROOT+"Profiles"; //下载论文文件夹
	public final static java.lang.String HTMLURLS_PATH= Setting.ROOT+"html_urls"; //下载html源代码文件夹
	
	/*作者分析*/
	public static final java.lang.String BINGDATAANALYSIS_AUTHORS_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/authors_analysis";
	private WeightDbManager<Author> authorsManager=null;
	private boolean analyzeAuthors=true;
	
	/*关键词分析*/
	public static final java.lang.String BINGDATAANALYSIS_KEYWORDS_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/keywords_analysis";
	private WeightDbManager<Keyword> keywordsManager=null;
	private boolean analyzeKeywords=true;
	
	/*论文排序*/
	public static final java.lang.String BINGDATAANALYSIS_PROFILES_CITEDBY_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"profiles_citedby_analysis";
	public static final java.lang.String BINGDATAANALYSIS_PROFILES_LINKEDCITEDBY_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"profiles_linkedcitedby_analysis";
	public static final java.lang.String BINGDATAANALYSIS_PROFILES_SOURCES_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"profiles_sources_analysis";
	private WeightDbManager<ProfileAbstract> profilesCitedByManager=null;
	private WeightDbManager<ProfileAbstract> profilesLinkedCitedByManager=null;
	private WeightDbManager<ProfileAbstract> profilesSourcesManager=null;
	private boolean analyzeProfiles=true;
	
	/*期刊会议*/
	public static final java.lang.String BINGDATAANALYSIS_JOURNALS_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/journals_analysis";
	private WeightDbManager<Journal> journalsManager=null;
	private boolean analyzeJournals=true;
	
	/*时间线*/
	public static final java.lang.String BINGDATAANALYSIS_TIMELINE_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/timeline_analysis";
	private WeightDbManager<Timeline> timelineManager=null;
	private boolean analyzeTimeline=true;
	
	/*爬取数量*/
	private boolean analyzeCrawledCount=true;
	
	/*SimHash分析*/
	public static final java.lang.String BINGDATAANALYSIS_SIMHASHMANAGER_PATH= Setting.ROOT+ Setting.DATA_ANALYSIS+"/simhash_analysis/simhash_manager";
	private WeightDbManager<ProfileSimHash> simHashManager=null;
	BingDataSimHashAnalysis simHashAnalysis=null;
	private boolean analyzeSimHash=true;
	
	
	/*设置分析哪些部分*/
	private void setAllAnalyze(boolean ifAnalyze){
		analyzeAuthors=ifAnalyze;
		analyzeKeywords=ifAnalyze;
		analyzeProfiles=ifAnalyze;
		analyzeJournals=ifAnalyze;
		analyzeTimeline=ifAnalyze;
		analyzeCrawledCount=ifAnalyze;
		analyzeSimHash=ifAnalyze;
	}
	
	public static void main(java.lang.String[] args) throws UnsupportedEncodingException{
		BingDataAnalysis analysis=new BingDataAnalysis();
		
		/*分析数据*/
		analysis.setAllAnalyze(true);
		analysis.exportData();
		analysis.analyzeData();
		
		analysis.close();
	}
	
	/*分析数据*/
	private static final int topCount=50;
	public void analyzeData(){
		/*爬取论文数*/
		if (analyzeCrawledCount){
			Logger.log("[爬取论文数]"); //
			StringBuilder sb=new StringBuilder();
			sb.append("[爬取的链接数]\r\n"+urlsManager.sizeUniUrls()+"\r\n");
			sb.append("[分析的论文数]\r\n"+profilesManager.size()+"\r\n");
			sb.append("[下载成功的论文数]\r\n"+BingDataAnalysisUtils.getDownloadProfilesCount(PROFILES_PATH)+"\r\n");
			CommonAnalysis.strToFile(sb.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS, "爬取总数.txt");
		}
		/*时间线*/
		if (analyzeTimeline){
			try{
				Logger.log("[时间线分析]"); //
				List<Timeline> timelineGroup=timelineManager.getNext(Integer.MAX_VALUE);
				/*整体时间线分析*/
				BingDataAnalysisUtils.analyzeTimelineTotalAnalysis(timelineGroup); 
				/*年份详细*/
				StringBuilder sb=new StringBuilder();
				for (Timeline timeline:timelineGroup){
					sb.append("====================YEAR "+timeline.getYear()+"====================\r\n");
					sb.append(timeline+"\r\n\r\n");
				}
				CommonAnalysis.strToFile(sb.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS+"/时间线分析", "年份详细.txt");
			}catch(Exception e){e.printStackTrace();}
		}
		/*论文排序*/
		if (analyzeProfiles){
			try{
				Logger.log("[论文排序]"); //
				java.lang.String result=null;
				result=BingDataAnalysisUtils.analyzeProfiles(profilesCitedByManager, null ,topCount);
				CommonAnalysis.strToFile(result, Setting.ROOT+ Setting.DATA_ANALYSIS+"/论文排序", "引用数最高的论文.txt");
				result=BingDataAnalysisUtils.analyzeProfiles(profilesLinkedCitedByManager,"(有记录可查询的)引用次数", topCount);
				CommonAnalysis.strToFile(result, Setting.ROOT+ Setting.DATA_ANALYSIS+"/论文排序", "(有记录可查询的)引用数最高论文.txt");
				result=BingDataAnalysisUtils.analyzeProfiles(profilesSourcesManager,"来源次数",topCount);
				CommonAnalysis.strToFile(result, Setting.ROOT+ Setting.DATA_ANALYSIS+"/论文排序", "网络来源最多的论文.txt");
			}catch(Exception e){e.printStackTrace();}
		}
		/*作者(期刊会议依赖作者,关键词依赖作者)*/
		if (analyzeAuthors||analyzeJournals||analyzeKeywords){
			try{
				Logger.log("[作者分析]"); //
				/*按爬取的该作者的论文总数排序*/
				StringBuilder sb=new StringBuilder();
				List<Author> topAuthors=authorsManager.getNext(topCount);
				if (topAuthors!=null){
					int topCur=0;
					for (Author author:topAuthors){
						BingDataAnalysisUtils.analyzeAuthor(author,profilesManager);
						/*同步到数据库*/
						authorsManager.update(author.getName().getBytes("utf-8"), author);
						sb.append("====================TOP "+(++topCur)+"====================\r\n");
						sb.append(author+"\r\n\r\n");
					}
//					Logger.log(sb.toString());//
					CommonAnalysis.strToFile(sb.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS, "影响力最高的作者.txt");
				}
			}catch(Exception e){e.printStackTrace();}
		}
		/*关键词(期刊会议依赖关键词,分析顺序必须在作者之后)*/
		if (analyzeKeywords||analyzeJournals){
			try{
				Logger.log("[关键词分析]"); //
				/*按爬取的该关键词的论文总数排序*/
				StringBuilder sb=new StringBuilder();
				List<Keyword> topKeywords=keywordsManager.getNext(topCount);
				if (topKeywords!=null){
					int topCur=0;
					for (Keyword keyword:topKeywords){
						BingDataAnalysisUtils.analyzeKeyword(keyword,profilesManager,authorsManager);
						/*同步到数据库*/
						keywordsManager.update(keyword.getName().getBytes("utf-8"), keyword);
						sb.append("====================TOP "+(++topCur)+"====================\r\n");
						sb.append(keyword+"\r\n\r\n");
					}
//					Logger.log(sb.toString());//
					CommonAnalysis.strToFile(sb.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS, "人工智能领域.txt");
				}
			}catch(Exception e){e.printStackTrace();}
		}
		/*期刊会议(分析顺序必须在依赖项之后)*/
		if (analyzeJournals){
			try{
				Logger.log("[期刊会议分析]"); //
				StringBuilder sb=new StringBuilder();
				List<Journal> topJournals=journalsManager.getNext(topCount);
				if (topJournals!=null){
					int topCur=0;
					for (Journal journal:topJournals){
						BingDataAnalysisUtils.analyzeJournal(journal,profilesManager,keywordsManager,authorsManager);
						/*同步到数据库*/
						journalsManager.update(journal.getName().getBytes("utf-8"), journal);
						sb.append("====================TOP "+(++topCur)+"====================\r\n");
						sb.append(journal+"\r\n\r\n");
					}
//					Logger.log(sb.toString());//
					CommonAnalysis.strToFile(sb.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS, "期刊会议.txt");
				}
			}catch(Exception e){e.printStackTrace();}
		}
		/*SimHash分析*/
		if (analyzeSimHash){
			try{
				Logger.log("[论文相似度分析]"); //
				/*等待所有SimHash被计算出*/
				for (int accumulated=0,previousCount=-1;;accumulated++){
					int currentThreadCount=simHashAnalysis.currentThreadCount();
					int remainingTaskCount=(int)simHashAnalysis.remainingTaskCount();
					if (previousCount!=remainingTaskCount){
						previousCount=remainingTaskCount;
						accumulated=0;
					}
					if (currentThreadCount<=0 && remainingTaskCount==0) break;
					if (accumulated>(currentThreadCount+remainingTaskCount)) break;
					try {Thread.sleep(500);} catch (InterruptedException e) {}
				}
				/*每篇和其它的相似度对比*/
				List<ProfileSimHash> profilesSimHash=simHashManager.getNext(Integer.MAX_VALUE);
				if (profilesSimHash!=null){
					StringBuilder sb=new StringBuilder();
					for (int i=0,topCur=0;topCur<topCount && i<profilesSimHash.size();i++){
						ProfileSimHash profileSimHash=profilesSimHash.get(i);
						if (BingDataAnalysisUtils.analyzeSimHash(profileSimHash,profilesSimHash)){
							/*同步到数据库*/
							simHashManager.update(profileSimHash.getTitle().getBytes("utf-8"), profileSimHash);
						}
						/*至少含有摘要*/
						if (profileSimHash.getSimHash()==null) continue; 
						Logger.log("[相似度分析]"+profileSimHash.getTitle()); //
						sb.append("====================TOP "+(++topCur)+"====================\r\n");
						sb.append(profileSimHash+"\r\n\r\n");
					}
					CommonAnalysis.strToFile(sb.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS, "论文相似度.txt");
				}
			}catch(Exception e){e.printStackTrace();}
		}
	}
	
	
	
	/*导出数据到各个Db*/
	public void exportData(){
		exportData(Integer.MAX_VALUE);
	}
	public void exportData(int dataCount){
		/*导出SimHash*/
		if (analyzeSimHash){
			simHashAnalysis.exportData(dataCount);
			simHashAnalysis.getSimHashPool().tryStart();
		}
		/*导出数据*/
		Db db=profilesManager.getDb();
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
	/*导出单个Profile数据*/
	private static int profilesCount=0;
	private void exportProfile(Profile profile){
		Logger.log("[导出第"+(++profilesCount)+"个作品数据]"+profile.getTitle()); //
		/*整体时间线*/
		if (analyzeTimeline){
			try {
				if (profile.getYear()!=null && Integer.parseInt(profile.getYear())>0){
					boolean timelineCreated=false;
					Timeline timeline=null;
					int year=Integer.parseInt(profile.getYear());
					timeline=timelineManager.get(Util.intToByteArray(year));
					if (timeline==null) {
						/*不存在则创建新timeline*/
						timeline=new Timeline(year);
						timelineCreated=true;
					}
					/*添加不重复的时间线*/
					if (!timeline.getProfiles().contains(profile.getTitle())){
						/*导出数据到时间线中*/
						BingDataAnalysisUtils.exportTimeline(timeline, profile);
						if (timelineCreated) {
							timelineManager.put(Util.intToByteArray(timeline.getYear()), timeline);
//							Logger.log("添加了时间线"+timeline.getYear()+",作品"+profile.getTitle()); //
						}
						else {
							timelineManager.update(Util.intToByteArray(timeline.getYear()), timeline);
//							Logger.log("更新了时间线"+timeline.getYear()+",作品"+profile.getTitle()); //
						}
					}
				}
			} catch (Exception e) {e.printStackTrace();}
		}
		/*论文排序*/
		if (analyzeProfiles){
			try{
				if (Integer.parseInt(profile.getCitedBy())>0){
					ProfileAbstract profileCitedBy=null;
					profileCitedBy=new ProfileAbstract(profile);
					profileCitedBy.setWeight(Integer.parseInt(profile.getCitedBy()));
					if (profilesCitedByManager.put(profile.getTitle().getBytes("utf-8"), profileCitedBy)!=OperationStatus.SUCCESS){
//						profilesCitedByManager.update(profile.getTitle().getBytes("utf-8"), profileCitedBy);
					}
				}
				if (profile.getCitedPapers().size()>0){
					ProfileAbstract profileLinkedCitedBy=null;
					profileLinkedCitedBy=new ProfileAbstract(profile);
					profileLinkedCitedBy.setWeight(profile.getCitedPapers().size());
					if (profilesLinkedCitedByManager.put(profile.getTitle().getBytes("utf-8"), profileLinkedCitedBy)!=OperationStatus.SUCCESS){
//						profilesLinkedCitedByManager.update(profile.getTitle().getBytes("utf-8"), profileLinkedCitedBy);
					}
				}
				if (profile.getDownloadUrls().size()>0 || profile.getSourceUrls().size()>0){
					ProfileAbstract profileSources=null;
					profileSources=new ProfileAbstract(profile);
					profileSources.setWeight(profile.getDownloadUrls().size()+profile.getSourceUrls().size());
					if (profilesSourcesManager.put(profile.getTitle().getBytes("utf-8"), profileSources)!=OperationStatus.SUCCESS){
//						profilesSourcesManager.update(profile.getTitle().getBytes("utf-8"), profileSources);
					}
				}
			}
			catch(Exception e){e.printStackTrace();}
		}
		/*作者(期刊会议分析依赖作者,关键词依赖作者)*/
		if (analyzeAuthors||analyzeJournals||analyzeKeywords){
			try {
				List<java.lang.String> authorsStr=profile.getAuthors();
				if (authorsStr!=null) for (java.lang.String authorStr:authorsStr){
					boolean authorCreated=false;
					Author author=null;
					author=authorsManager.get(authorStr.getBytes("utf-8"));
					if (author==null) {
						/*不存在则创建新Author*/
						author=new Author(authorStr);
						authorCreated=true;
					}
					/*添加不重复的作品*/
					if (!author.getProfiles().contains(profile.getTitle())){
						author.getProfiles().add(profile.getTitle());
						/*按爬取的作品数排序*/
						author.setWeight(author.getProfiles().size()); 
						if (authorCreated) {
							authorsManager.put(author.getName().getBytes("utf-8"), author);
//							Logger.log("添加了作者"+author.getName()+",作品"+profile.getTitle()); //
						}
						else {
							authorsManager.update(author.getName().getBytes("utf-8"), author);
//							Logger.log("更新了作者"+author.getName()+",作品"+profile.getTitle()); //
						}
					}
				}
			} catch (Exception e) {e.printStackTrace();}
		}
		/*关键词(期刊会议分析依赖关键词)*/
		if (analyzeKeywords||analyzeJournals){
			try{
				List<java.lang.String> keywordsStr=profile.getKeywords();
				if (keywordsStr!=null) for (java.lang.String keywordStr:keywordsStr){
					boolean keywordCreated=false;
					Keyword keyword=null;
					keyword=keywordsManager.get(keywordStr.getBytes("utf-8"));
					if (keyword==null){
						/*不存在则创建新Keyword*/
						keyword=new Keyword(keywordStr);
						keywordCreated=true;
					}
					/*添加不重复的作品*/
					if (!keyword.getProfiles().contains(profile.getTitle())){
						keyword.getProfiles().add(profile.getTitle());
						/*按该关键字下爬取的论文总数排序*/
						keyword.setWeight(keyword.getProfiles().size());
						if (keywordCreated) {
							keywordsManager.put(keyword.getName().getBytes("utf-8"), keyword);
//							Logger.log("添加了关键词"+keyword.getName()+",作品"+profile.getTitle()); //
						}
						else {
							keywordsManager.update(keyword.getName().getBytes("utf-8"), keyword);
//							Logger.log("更新了关键词"+keyword.getName()+",作品"+profile.getTitle()); //
						}
					}
				}
			}
			catch (Exception e) {e.printStackTrace();}
		}
		/*期刊会议*/
		if (analyzeJournals){
			try {
				java.lang.String journalName=profile.getJournal();
				if (!TextUtils.isEmpty(journalName)){
					boolean journalCreated=false;
					Journal journal=null;
					journal=journalsManager.get(journalName.getBytes("utf-8"));
					if (journal==null) {
						/*不存在则创建新Journal*/
						journal=new Journal(journalName);
						journalCreated=true;
					}
					/*添加不重复的作品*/
					if (!journal.getProfiles().contains(profile.getTitle())){
						journal.getProfiles().add(profile.getTitle());
						/*按照包含的论文数排序*/
						journal.setWeight(journal.getProfiles().size()); 
						if (journalCreated) {
							journalsManager.put(journal.getName().getBytes("utf-8"), journal);
//							Logger.log("添加了期刊会议"+journal.getName()+",作品"+profile.getTitle()); //
						}
						else {
							journalsManager.update(journal.getName().getBytes("utf-8"), journal);
//							Logger.log("更新了期刊会议"+journal.getName()+",作品"+profile.getTitle()); //
						}
					}
				}
			} catch (Exception e) {e.printStackTrace();}
		}
		
	}
	
	private void setup(){
		try{
			timelineManager=new WeightDbManager<Timeline>(BINGDATAANALYSIS_TIMELINE_PATH,Timeline.class);
			journalsManager=new WeightDbManager<Journal>(BINGDATAANALYSIS_JOURNALS_PATH,Journal.class);
			profilesCitedByManager=new WeightDbManager<ProfileAbstract>(BINGDATAANALYSIS_PROFILES_CITEDBY_PATH,ProfileAbstract.class);
			profilesLinkedCitedByManager=new WeightDbManager<ProfileAbstract>(BINGDATAANALYSIS_PROFILES_LINKEDCITEDBY_PATH,ProfileAbstract.class);
			profilesSourcesManager=new WeightDbManager<ProfileAbstract>(BINGDATAANALYSIS_PROFILES_SOURCES_PATH,ProfileAbstract.class);
			authorsManager=new WeightDbManager<Author>(BINGDATAANALYSIS_AUTHORS_PATH,Author.class);
			keywordsManager=new WeightDbManager<Keyword>(BINGDATAANALYSIS_KEYWORDS_PATH,Keyword.class);
			profilesManager=new DbManager<Profile>(BING_PARSER_PROFILESDB_PATH,Profile.class);
			urlsManager=new UrlsDbManager(BING_PARSER_URLSDB_PATH);
			urlsManager.setMaxDepth(3); //深度
			if (analyzeSimHash){
				/*必须在profilesManager之后*/
				simHashManager=new WeightDbManager<ProfileSimHash>(BINGDATAANALYSIS_SIMHASHMANAGER_PATH,ProfileSimHash.class);
				simHashAnalysis=new BingDataSimHashAnalysis(profilesManager,simHashManager);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public void close(){
		if (analyzeSimHash){
			if (simHashAnalysis!=null) simHashAnalysis.getSimHashPool().getManager().close();
			if (simHashManager!=null) simHashManager.close();
		}
		if (timelineManager!=null) timelineManager.close();
		if (journalsManager!=null) journalsManager.close();
		if (profilesCitedByManager!=null) profilesCitedByManager.close();
		if (profilesLinkedCitedByManager!=null) profilesLinkedCitedByManager.close();
		if (profilesSourcesManager!=null) profilesSourcesManager.close();
		if (keywordsManager!=null) keywordsManager.close();
		if (authorsManager!=null) authorsManager.close();
		if (profilesManager!=null) profilesManager.close();
		if (urlsManager!=null) urlsManager.close();
	}

	public BingDataAnalysis() {
		super();
		this.setup();
	}
	
	
	
}
