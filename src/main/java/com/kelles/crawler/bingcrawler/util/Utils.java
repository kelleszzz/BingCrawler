package com.kelles.crawler.bingcrawler.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.kelles.crawler.bingcrawler.setting.Constant;
import org.apache.http.util.TextUtils;
import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.lexer.Lexer;
import org.htmlparser.nodes.TagNode;
import org.htmlparser.nodes.TextNode;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

public class Utils {
	
	/*按照固定格式输出Map*/
	public static java.lang.String formatTopMapStr(Map<java.lang.String,Integer> map, java.lang.String msg1, java.lang.String msg2){
		return formatTopMapStr(map,msg1,msg2,Integer.MAX_VALUE);
	}
	public static java.lang.String formatTopMapStr(Map<java.lang.String,Integer> map, java.lang.String msg1, java.lang.String msg2, int topCount){
		StringBuilder sb=new StringBuilder();
		if (!map.isEmpty()){
			if (!TextUtils.isEmpty(msg1))
				if (topCount==Integer.MAX_VALUE || topCount<=0)
					sb.append("["+msg1+"]\r\n");
				else sb.append("["+msg1+"(top"+topCount+")]\r\n");
			List<java.lang.String> keys=new ArrayList(map.keySet());
			Collections.sort(keys, new Comparator<java.lang.String>(){
				@Override
				public int compare(java.lang.String arg0, java.lang.String arg1) {
					int i1=map.get(arg0),i2=map.get(arg1);
					if (i1>i2) return -1;
					else if (i1<i2) return 1;
					else return 0;
				}
			});
			int topCur=0;
			for (java.lang.String key:keys){
				if ((++topCur)>topCount) break;
				sb.append(key);
				if (map.get(key)>0 && !TextUtils.isEmpty(msg2)) sb.append(" ("+msg2+""+map.get(key)+")");
				sb.append("\r\n");
			}
		}
		return sb.toString();
	}
	
	/*获取格式化的系统时间*/
	public static java.lang.String getSystemTime(){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		return df.format(new Date());
	}
	
	/*根据给定的url生成一个html重定向文件*/
	public static void generateRedirectHtml(java.lang.String url, java.lang.String fileDir, java.lang.String fileName){
		FileOutputStream fos=null;
			try {
				java.lang.String html= Constant.REDIRECT_HTML;
				Pattern p=Pattern.compile("YOURURLHERE");
				Matcher m=p.matcher(html);
				if (m.find()) html=m.replaceFirst(url);
				
				File dirFile=new File(fileDir);
				if (!dirFile.isDirectory()) dirFile.mkdirs();
				File htmlFile=new File(dirFile,fileName+".html");
				if (!htmlFile.isFile()) htmlFile.createNewFile();
				fos=new FileOutputStream(htmlFile);
				fos.write(html.getBytes("utf-8"));
			} catch (IOException e) {
				e.printStackTrace();
			}
			finally{
				if (fos!=null)
					try {
						fos.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
	}
	
	/*去除/\:*?"<>|这些特殊符号,并用~代替*/	
	public static java.lang.String replaceFileBadLetter(java.lang.String str){
		Pattern p=Pattern.compile("/|\\\\|:|\\*|\\?|\"|\\<|\\>|\\|");
		System.out.println(str);
		Matcher m=p.matcher(str);
		if (m.find()) str=m.replaceAll("~");
		return str;
	}
			
			
	public static java.lang.String byteArrayToString(byte[] bytes){
		if (bytes==null) return null;
		java.lang.String str="{";
		for (byte b:bytes)
			str+=b+" ";
		str=str.substring(0, str.length()-1);
		str+="}";
		return str;
		
	}
	
	public static boolean byteArrayEquals(byte[] b1,byte[] b2){
		if (b1==null || b2==null) return false;
		if (b1.length!=b2.length) return false;
		for (int i=0,len=b1.length;i<len;i++)
			if (b1[i]!=b2[i]) return false;
		return true;
	}
	
	private static java.lang.String a(byte[] b){
		java.lang.String str="{";
		for (byte c:b)
			str+=c+" ";
		str+="}";
		return str;
	}
	
	/*转换如&amp; &nbsp; 等html字符实体*/
	public static java.lang.String convertHtmlCharEntity(java.lang.String url){
		url=url.replaceAll("&amp;", "&");
		url=url.replaceAll("&lt;", "<");
		url=url.replaceAll("&gt;", ">");
		url=url.replaceAll("&yen;", "¥");
		url=url.replaceAll("&cent;", "¢");
		url=url.replaceAll("&copy;", "©");
		url=url.replaceAll("&reg;", "®");
		url=url.replaceAll("&trade;", "™");
		return url;
	}
			
	
	//格式化html
	private static final int STEP=3;
	public static java.lang.String htmlFormatting(java.lang.String html){
		try {
			if (html==null) return null;
			Parser parser=new Parser(new Lexer(html));
			NodeList baseList=parser.parse(null);
			int baseNodes=baseList.size();
			List<Integer> depths=new ArrayList();
			for (int i=0;i<baseList.size();i++) depths.add(STEP);
			for (int p=0;p<baseList.size();p++){
				//若存在子Tag节点,且不存在子\n节点,添加子\n节点
				Node baseNode=baseList.elementAt(p);
				int depth=depths.get(p);
				NodeList childrenList=baseNode.getChildren();
				if (childrenList!=null){
					NodeList newChildrenList=new NodeList();
					for (int i=0;i<childrenList.size();i++){
						Node childNode=childrenList.elementAt(i);
						//前置换行
						if (childNode instanceof TagNode 
								&& !(i>=1 
								&& (childrenList.elementAt(i-1) instanceof TextNode)
								&& "".equals(childrenList.elementAt(i-1).getText().trim()))){
							StringBuilder builder=new StringBuilder();
							builder.append("\n"); for (int j=0;j<depth;j++) builder.append(" ");
							TextNode newLineNode=new TextNode(builder.toString());
							newChildrenList.add(newLineNode);
						}
						newChildrenList.add(childNode);
						//后置换行(仅最后一个子元素添加换行)
						if (childNode instanceof TagNode 
								&& i==childrenList.size()-1){
							StringBuilder builder=new StringBuilder();
							builder.append("\n"); for (int j=0;j<depth-STEP;j++) builder.append(" ");
							TextNode newLineNode=new TextNode(builder.toString());
							newChildrenList.add(newLineNode);
						}
					}
					baseNode.setChildren(newChildrenList);
				}
				NodeList baseNodeChildrenList=baseNode.getChildren();
				if (baseNodeChildrenList!=null)
					for (int i=0;i<baseNodeChildrenList.size();i++){
						baseList.add(baseNodeChildrenList.elementAt(i));
						depths.add(depth+STEP);
					}
			}
			NodeList finalList=new NodeList();
			for (int i=0;i<baseNodes;i++) finalList.add(baseList.elementAt(i));
			return finalList.toHtml();
		} catch (ParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public static void addChildrenNodesToNodeList(Node node,NodeList nodeList){
		NodeList childrenList=node.getChildren();
		if (childrenList!=null)
			for (int i=0;i<childrenList.size();i++)
				nodeList.add(childrenList.elementAt(i));
	}
	
	//找到第一个符合条件的Node
	public static Node extractOneNodeThatMatch(Node node,NodeFilter filter){
		NodeList nodeList=new NodeList();
		nodeList.add(node);
		return extractOneNodeThatMatch(nodeList,filter);
	}
	public static Node extractOneNodeThatMatch(NodeList nodeList,NodeFilter filter){
		NodeList baseList=new NodeList();
		for (int i=0;i<nodeList.size();i++) baseList.add(nodeList.elementAt(i));
		for (int p=0;p<baseList.size();p++){
			Node node=baseList.elementAt(p);
			if (filter.accept(node)) return node;
			NodeList childrenList=node.getChildren();
			if (childrenList!=null) baseList.add(childrenList);
		}
		return null;
	}
	
	//把一个HTMLParser节点的子节点全部加入到指定的NodeList中
	public static void addChildrenNodeToList(Node node,NodeList nodeList){
		NodeList childrenList=node.getChildren();
		if (childrenList!=null)
			for (int i=0;i<childrenList.size();i++)
				nodeList.add(childrenList.elementAt(i));
	}
	
	public static byte[] intToByteArray(int i){
		ByteArrayOutputStream baos=new ByteArrayOutputStream();
		DataOutputStream dos=new DataOutputStream(baos);
		try {
			dos.writeInt(i);
			byte[] bytes=baos.toByteArray();
			return bytes;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	public static int byteArrayToInt(byte[] b, int offset) {
	       int value= 0;
	       for (int i = 0; i < 4; i++) {
	           int shift= (4 - 1 - i) * 8;
	           value +=(b[i + offset] & 0x000000FF) << shift;
	       }
	       return value;
	 }
	
	
	public static java.lang.String getHostUrl(java.lang.String sourceUrl){
		Pattern p=Pattern.compile("(?<protocol>(http|https)://)?(?<host>.*)");
		Matcher m=p.matcher(sourceUrl);
		java.lang.String protocol,host,hostUrl="";
		if (m.matches()){
			if ((protocol=m.group("protocol"))!=null) {
//				VersionUtils.log(11.10,"protocol = "+protocol);
				hostUrl+=protocol;
			}
			if ((host=m.group("host"))!=null) {
				int anIndex;
				if ((anIndex=host.indexOf("/"))!=-1) host=host.substring(0, anIndex);
//				VersionUtils.log(11.10,"host = "+host);
				hostUrl+=host;
			}
			if (TextUtils.isEmpty(hostUrl)) hostUrl=sourceUrl;
//			VersionUtils.log(11.10,"还原为hostUrl = "+hostUrl);
		}
		return hostUrl;
	}
	

	
	//对"http://www.hacg.fi/wp/23147.html#comment-62755"去除"#comment-62755"
		public static java.lang.String removeSuffix(java.lang.String url){
			Pattern p;
			Matcher m;
			while (url.endsWith("/")) url=url.substring(0, url.length()-1);
			p=Pattern.compile("(.+)#(.*)");
			m=p.matcher(url);
			if (m.matches()) url=m.group(1);
			return url;
		}
}
