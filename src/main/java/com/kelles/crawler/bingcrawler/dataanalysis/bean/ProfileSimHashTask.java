package com.kelles.crawler.bingcrawler.dataanalysis.bean;

import java.io.File;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import com.kelles.crawler.bingcrawler.database.weightdb.WeightDbManager;
import com.kelles.crawler.bingcrawler.threadpool.TaskInterface;
import org.apache.http.util.TextUtils;

import com.kelles.crawler.bingcrawler.util.*;
import com.sleepycat.je.OperationStatus;

public class ProfileSimHashTask implements TaskInterface,Serializable{
	private int weight=DEFAULT_WEIGHT;
	public static final int DEFAULT_WEIGHT=100;
	
	private ProfileAbstract profileAbstract=null;
	private static String dirPath=null;
	private static WeightDbManager<ProfileSimHash> simHashManager=null;

	@Override
	public boolean execute() {
		try {
			if (dirPath==null) throw new RuntimeException("使用ProfileSimHash.setup()设置论文和数据库路径");
			BigInteger simHash=null;
			
			simHash=abstractSimHash(15);
			ProfileSimHash profileSimHash=new ProfileSimHash(profileAbstract.getTitle());
			profileSimHash.setSimHash(simHash);
			if (profileAbstract.getCitedBy()!=null && Integer.parseInt(profileAbstract.getCitedBy())>0 )
				profileSimHash.setWeight(Integer.parseInt(profileAbstract.getCitedBy()));
				if (simHashManager.put(profileSimHash.getTitle().getBytes("utf-8"), profileSimHash)!=OperationStatus.SUCCESS)
			
			simHash=null;
			
			simHash=pdfSimHash();
			if (simHash!=null){
				profileSimHash.setSimHash(simHash);
				try {simHashManager.update(profileSimHash.getTitle().getBytes("utf-8"), profileSimHash);} catch (UnsupportedEncodingException e) {}
			}
			return true;
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/*根据摘要获得SimHash*/
	private static final Random rand=new Random();
	private BigInteger abstractSimHash(int disturbed){
		BigInteger result=null;
		if (result==null){
			/*通过Profile计算SimHash,标题、关键字、摘要*/
			StringBuilder sb=new StringBuilder(profileAbstract.getTitle());
			String introduction=profileAbstract.getIntroduction();
			/*摘要不存在时,不计算SimHash*/
			if (TextUtils.isEmpty(introduction)) {
				return null;
			}
			else{
				sb.append(" ");
				sb.append(introduction);
			}
			List<String> keywords=profileAbstract.getKeywords();
			if (keywords!=null){
				sb.append(" ");
				for (String keyword:keywords) sb.append(keyword+" ");
			}
			result=TextAnalysis.getSimHash(sb.toString());
			VersionUtils.log("[摘要获得]"+profileAbstract.getTitle()); //
		}
		if (disturbed>0){
//			VersionUtils.log("混淆前:"+result.toString(2)); //
			if (disturbed>TextAnalysis.totalBits) disturbed=TextAnalysis.totalBits;
			for (int i=0;i<disturbed;i++){
				/*随机改变bit*/
				boolean ifChange=rand.nextBoolean();
				if (!ifChange) continue;
				BigInteger mask=(ifChange?BigInteger.ONE:BigInteger.ZERO).shiftLeft(i);
				result=result.xor(mask);
			}
//			VersionUtils.log("混淆后:"+result.toString(2)); //
		}
		return result;
	}

	/*根据pdf获得SimHash*/
	private BigInteger pdfSimHash(){
		BigInteger result=null;
		File rootDir=new File(dirPath);
		if (rootDir.isDirectory()){
			File profileDir=new File(rootDir, Util.replaceFileBadLetter(profileAbstract.getTitle()));
			if (profileDir.isDirectory()){
			String[] fileNames=profileDir.list();
			if (fileNames!=null)
				for (String fileName:fileNames){
					if (!fileName.endsWith("pdf")) continue;
					File pdfFile=new File(profileDir,fileName);
					if (pdfFile.isFile()){
						try{
							String rawContent=PdfBox.get(pdfFile);
							if (!TextUtils.isEmpty(rawContent)){
								result=TextAnalysis.getSimHash(rawContent);
								VersionUtils.log("[pdf获得]"+profileAbstract.getTitle()); //
								break;
							}
						}
						/*无法读取为pdf文件*/
						catch(Exception e){
							result=null;
							continue;
						}
					}
				}
			}
		}
		return result;
	}

	
	


	@Override
	public String describe() {
		return profileAbstract.getTitle();
	}

	@Override
	public byte[] getKeyBytes() {
		try {
			return profileAbstract.getTitle().getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}



	public ProfileAbstract getProfileAbstract() {
		return profileAbstract;
	}

	public void setProfileAbstract(ProfileAbstract profileAbstract) {
		this.profileAbstract = profileAbstract;
	}


	public ProfileSimHashTask(ProfileAbstract profileAbstract) {
		super();
		this.profileAbstract = profileAbstract;
	}


	public static void setup(String dirPath,WeightDbManager<ProfileSimHash> simHashManager){
		ProfileSimHashTask.dirPath=dirPath;
		ProfileSimHashTask.simHashManager=simHashManager;
	}

	@Override
	public String toString() {
		return "ProfileSimHashTask [weight=" + weight + ", profileAbstract=" + profileAbstract.getTitle() + "]";
	}






	
	
	
}
