package com.kelles.crawler.bingcrawler.dataanalysis;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.kelles.crawler.bingcrawler.dataanalysis.bean.*;
import com.kelles.crawler.bingcrawler.dataanalysis.bean.ProfileSimHash;
import com.kelles.crawler.bingcrawler.database.weightdb.WeightDbManager;
import com.kelles.crawler.bingcrawler.setting.Setting;
import org.apache.http.util.TextUtils;
import com.kelles.crawler.bingcrawler.util.*;
import com.kelles.crawler.bingcrawler.bean.*;
import com.kelles.crawler.bingcrawler.database.*;
import com.kelles.crawler.bingcrawler.analysis.CommonAnalysis;

public class BingDataAnalysisUtils {
	
	/*SimHash分析*/
	public static boolean analyzeSimHash(ProfileSimHash profileSimHash, List<ProfileSimHash> profilesSimHash){
		if (profileSimHash.getSimHash()==null) return false;
		if (profileSimHash.getDistances().size()>=profilesSimHash.size()) return false;
		boolean changed=false;
		for (ProfileSimHash curSimHash:profilesSimHash)
			if (!profileSimHash.getTitle().equals(curSimHash.getTitle())
					&& !profileSimHash.getDistances().containsKey(curSimHash.getTitle())){
				changed=true;
				BigInteger simHash1=profileSimHash.getSimHash(),simHash2=curSimHash.getSimHash();
				if (simHash1!=null && simHash2!=null)
					profileSimHash.getDistances().put(curSimHash.getTitle(), TextAnalysis.hammingDistance(simHash1, simHash2));
				else 
					profileSimHash.getDistances().put(curSimHash.getTitle(), -1); //不存在pdf或摘要,海明距离为-1
			}
		return changed;
	}
	
	/*下载的论文数*/
	public static int getDownloadProfilesCount(java.lang.String dirPath){
		File dir=new File(dirPath);
		if (!dir.isDirectory()) return -1;
		return dir.listFiles().length;
	}
	
	/*整体时间线分析*/
	private static final int timeInterval=5;
	private static java.lang.String addTimeInterval(int startYear, int endYear){
		if (startYear!=endYear) return("["+startYear+"-"+endYear+"]");
		else return("["+startYear+"]");
	}
	public static void analyzeTimelineTotalAnalysis(List<Timeline> timelineGroup){
		int startYear=0;
		int arg1=0,arg2=0,arg3=0;
		Map<java.lang.String,Integer> arg4=new HashMap();
		StringBuilder sb1,sb2,sb3,sb4;
		sb1=new StringBuilder();sb2=new StringBuilder();sb3=new StringBuilder();sb4=new StringBuilder();
		for (int i=timelineGroup.size()-1;i>=0;i--){
			Timeline timeline=timelineGroup.get(i);
			if (startYear==0) startYear=timeline.getYear();
			/*统计数据*/
			arg1+=timeline.getProfiles().size(); //按年代分布的论文发表数
			arg2+=timeline.getTotalCitedBy(); //按年代分布的论文总引用数
			arg3+=timeline.getTotalCitedBy()/timeline.getProfileCitedBy().size(); //按年代分布的论文平均被引用数
			if (timeline.getKeywordsTotal().size()>0) 
				for (java.lang.String key:timeline.getKeywordsTotal().keySet()){
					/*按年代分布的论文平均发表数*/
					if (arg4.containsKey(key)) arg4.put(key, arg4.get(key)+timeline.getKeywordsTotal().get(key));
					else arg4.put(key, timeline.getKeywordsTotal().get(key));
				}
			/*输出数据到StringBuilder*/
			while (startYear+timeInterval-1<timeline.getYear() || i==0){
				StringBuilder sb=null; int arg=0;
				int endYear=startYear+timeInterval-1;
				if (i==timelineGroup.size()) endYear=timelineGroup.size();
				
				sb=sb1;arg=arg1;
				sb.append(addTimeInterval(startYear,startYear+timeInterval-1));
				sb.append(arg+"\r\n");
				
				sb=sb2;arg=arg2;
				sb.append(addTimeInterval(startYear,startYear+timeInterval-1));
				sb.append(arg+"\r\n");
				
				sb=sb3;arg=arg3;
				sb.append(addTimeInterval(startYear,startYear+timeInterval-1));
				sb.append(arg+"\r\n");
				
				sb=sb4;
				sb.append(addTimeInterval(startYear,startYear+timeInterval-1));
				sb.append("\r\n");
				sb.append(Util.formatTopMapStr(arg4, null, "论文数",5));
				sb.append("\r\n");
				
				arg1=arg2=arg3=0;
				arg4.clear();
				
				startYear+=timeInterval;
				if (i==0) break;
			}
		}
		CommonAnalysis.strToFile(sb1.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS+"/时间线分析", "各年代论文发表数.txt");
		CommonAnalysis.strToFile(sb2.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS+"/时间线分析", "各年代论文总引用数.txt");
		CommonAnalysis.strToFile(sb3.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS+"/时间线分析", "各年代论文平均引用数.txt");
		CommonAnalysis.strToFile(sb4.toString(), Setting.ROOT+ Setting.DATA_ANALYSIS+"/时间线分析", "各年代活跃的主题.txt");
	}
	
	
	/*更新Profile数据到相应的timeline中*/
	public static void exportTimeline(Timeline timeline,Profile profile){
		if (profile.getYear()!=null && Integer.parseInt(profile.getYear())==timeline.getYear()){
			timeline.getProfiles().add(profile.getTitle());
			/*按年代分布的论文总引用数*/
			java.lang.String citedByStr=profile.getCitedBy();
			if (citedByStr!=null){
				int citedBy=Integer.parseInt(citedByStr);
				if (citedBy>0) timeline.setTotalCitedBy(timeline.getTotalCitedBy()+citedBy);
			}
			/*按年代分布的有记录可查询的论文总引用数*/
			if (profile.getCitedPapers().size()>0)
				timeline.setTotalLinkedCitedBy(timeline.getTotalLinkedCitedBy()+profile.getCitedPapers().size());
			/*当年引用量最高的论文*/
			if (timeline.getProfileCitedBy().containsKey(profile.getTitle()))
				timeline.getProfileCitedBy().put(profile.getTitle(),timeline.getProfileCitedBy().get(profile.getTitle()));
			else timeline.getProfileCitedBy().put(profile.getTitle(), 1);
			/*沿时间线出现的各个主题论文发表总数(主题论文活跃年代)*/
			List<java.lang.String> keywords=profile.getKeywords();
			if (keywords!=null && keywords.size()>0)
				for (java.lang.String keyword:keywords){
					if (timeline.getKeywordsTotal().containsKey(keyword))
						timeline.getKeywordsTotal().put(keyword, timeline.getKeywordsTotal().get(keyword)+1);
					else timeline.getKeywordsTotal().put(keyword, 1);
				}
			/*沿时间线出现的各个作者论文发表总数(作者活跃年代)*/
			List<java.lang.String> authors=profile.getAuthors();
			if (authors!=null && authors.size()>0)
				for (java.lang.String author:authors){
					if (timeline.getAuthorsTotal().containsKey(author))
						timeline.getAuthorsTotal().put(author, timeline.getAuthorsTotal().get(author)+1);
					else timeline.getAuthorsTotal().put(author, 1);
				}
		}
	}
	
	/*分析期刊会议*/
	protected static void analyzeJournal(Journal journal,DbManager<Profile> profilesManager,WeightDbManager<Keyword> keywordsManager,WeightDbManager<Author> authorsManager){
		try{
			/*获取Profiles*/
			List<Profile> profiles=new ArrayList();
			for (java.lang.String profileStr:journal.getProfiles()){
				Profile profile=profilesManager.get(profileStr.getBytes("utf-8"));
				if (profile!=null){
					profiles.add(profile);
				}
			}
			
			/*引用数最高的论文*/
			for (Profile profile:profiles){
				int citedBy=Integer.parseInt(profile.getCitedBy());
				if (citedBy>0) journal.getProfilesCitedBy().put(profile.getTitle(), citedBy);
			}
			
			/*所有涉及的关键词(按包含该关键词的论文总引用数排序,依赖关键词分析)*/
			for (Profile profile:profiles)
				if (profile.getKeywords()!=null)
					for (java.lang.String keywordStr:profile.getKeywords()){
						Keyword keyword=keywordsManager.get(keywordStr.getBytes("utf-8"));
						if (keyword==null) continue;
						journal.getKeywords().put(keyword.getName(), keyword.getTotalCitedBy());
					}
			
			/*文章被引用最多的作者(依赖作者分析)*/
			for (Profile profile:profiles)
				if (profile.getAuthors()!=null)
					for (java.lang.String authorStr:profile.getAuthors()){
						Author author=authorsManager.get(authorStr.getBytes("utf-8"));
						if (author==null) continue;
						journal.getAuthors().put(author.getName(), author.getTotalCitedBy());
					}
			
			/*发表年代*/
			for (Profile profile:profiles)
				if (profile.getYear()!=null){
					int year=Integer.parseInt(profile.getYear());
					if (year>0){
						if (journal.getYears().containsKey(year))
							journal.getYears().put(year, journal.getYears().get(year)+1);
						else journal.getYears().put(year, 1);
					}
				}

		}catch(Exception e){e.printStackTrace();}
	}
	
	/*分析论文排序*/
	protected static java.lang.String analyzeProfiles(WeightDbManager<ProfileAbstract> manager, java.lang.String rankName, int topCount){
		StringBuilder sb=new StringBuilder();
		List<ProfileAbstract> profilesAbstract=manager.getNext(topCount);
		if (profilesAbstract!=null){
			int topCur=0;
			for (ProfileAbstract profileAbstract:profilesAbstract){
				sb.append("====================TOP "+(++topCur)+"====================\r\n");
				if (profileAbstract.getWeight()>0 && !TextUtils.isEmpty(rankName))
					sb.append("["+rankName+"]\r\n"+profileAbstract.getWeight()+"\r\n");
				sb.append(profileAbstract+"\r\n");
				sb.append("\r\n");
			}
		}
		return sb.toString();
	}
	
	/*分析Keyword*/
	protected static void analyzeKeyword(Keyword keyword,DbManager<Profile> profilesManager,WeightDbManager<Author> authorsManager){
		try{
			/*获取Profiles*/
			List<Profile> profiles=new ArrayList();
			for (java.lang.String profileStr:keyword.getProfiles()){
				Profile profile=profilesManager.get(profileStr.getBytes("utf-8"));
				if (profile!=null){
					profiles.add(profile);
				}
			}
			
			/*每篇包含该关键字的论文引用数*/
			int total=0;
			for (Profile profile:profiles){
				if (Integer.parseInt(profile.getCitedBy())>0) keyword.getProfilesCitedBy().put(profile.getTitle(), Integer.parseInt(profile.getCitedBy()));
				else keyword.getProfilesCitedBy().put(profile.getTitle(), 0);
			}
			
			/*(有记录可查询)包含该关键字的论文总引用数*/
			for (Profile profile:profiles)
				keyword.setTotalLinkedCitedBy(keyword.getTotalLinkedCitedBy()+profile.getCitedPapers().size());
			
			/*包含该关键词的论文的发表年代*/
			for (Profile profile:profiles){
				int year=Integer.parseInt(profile.getYear());
				if (year>0){
					if (keyword.getYears().containsKey(year))
						keyword.getYears().put(year, keyword.getYears().get(year)+1);
					else keyword.getYears().put(year, 1);
				}
			}
			
			/*包含该关键词的论文的会议(按包含的论文数排序)*/
			for (Profile profile:profiles){
				java.lang.String journal=profile.getJournal();
				if (journal!=null){
					if (keyword.getJournals().containsKey(journal))
						keyword.getJournals().put(journal, keyword.getJournals().get(journal)+1);
					else keyword.getJournals().put(journal, 1);
				}
			}
			
			/*该关键词下最有影响力的作者(按引用总数排序,依赖作者分析)*/
			for (Profile profile:profiles){
				List<java.lang.String> authors=profile.getAuthors();
				if (authors!=null)
					for (java.lang.String authorStr:authors){
						Author author=authorsManager.get(authorStr.getBytes("utf-8"));
						if (author==null) continue;
						keyword.getAuthors().put(author.getName(), author.getTotalCitedBy());
					}
			}
			
			/*包含该关键词的论文中，包含的其它关键词,以相关度形式表现(按照包含的论文数排序)*/
			for (Profile profile:profiles){
				if (profile.getKeywords()!=null)
					for (java.lang.String relatedKeyword:profile.getKeywords()){
						if (keyword.getName().equals(relatedKeyword)) continue;
						if (keyword.getRelatedKeywords().containsKey(relatedKeyword)){
							keyword.getRelatedKeywords().put(relatedKeyword,keyword.getRelatedKeywords().get(relatedKeyword)+1);
						}
						else keyword.getRelatedKeywords().put(relatedKeyword,1);
					}
			}
			
		}
		catch(Exception e){e.printStackTrace();}
	}
	
	/*分析Author*/
	protected static void analyzeAuthor(Author author, DbManager<Profile> profilesManager){
		try{
			/*获取Profiles*/
			List<Profile> profiles=new ArrayList();
			for (java.lang.String profileStr:author.getProfiles()){
				Profile profile=profilesManager.get(profileStr.getBytes("utf-8"));
				if (profile!=null){
					profiles.add(profile);
				}
			}
			/*总引用量和平均引用量*/
			int total=0;
			for (Profile profile:profiles){
				if (profile.getCitedBy()!=null 
				&& Integer.parseInt(profile.getCitedBy())>0){
					total++;
					author.setTotalCitedBy(author.getTotalCitedBy()+Integer.parseInt(profile.getCitedBy()));
				}
			}
			if (total>0) author.setAverageCitedBy(author.getTotalCitedBy()/total);
			/*有记录可查询的总引用文章和平均引用文章*/
			total=0;
			for (Profile profile:profiles){
				if (profile.getCitedPapers().size()>0){
					total++;
					author.setTotalLinkedCitedBy(author.getTotalLinkedCitedBy()+profile.getCitedPapers().size());
				}
			}
			if (total>0) author.setAverageLinkedCitedBy(author.getTotalLinkedCitedBy()/total);
			/*作者的作品关键词*/
			for (Profile profile:profiles)
				if (profile.getKeywords()!=null)
					author.getKeywords().addAll(profile.getKeywords());
			/*作者的写作年代分布*/
			for (Profile profile:profiles){
				int year=Integer.parseInt(profile.getYear());
				if (year>0){
					if (author.getYears().containsKey(year))
						author.getYears().put(year, author.getYears().get(year)+1);
					else author.getYears().put(year, 1);
				}
			}
			/*和该作者合作过的作者*/
			for (Profile profile:profiles)
				if (profile.getAuthors()!=null)
					author.getCo_authors().addAll(profile.getAuthors());
			/*刊登过的杂志*/
			for (Profile profile:profiles)
				if (profile.getJournal()!=null)
					author.getJournals().add(profile.getJournal());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
