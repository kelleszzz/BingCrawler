package com.kelles.crawler.crawler.threadpool;

import java.util.List;

import com.kelles.crawler.crawler.database.weightdb.WeightDbManager;
import com.kelles.crawler.crawler.database.weightdb.WeightInterface;
import com.kelles.crawler.crawler.util.Util;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.Transaction;

public class TaskWeightDbManager<ValueObjectClass extends WeightInterface>
extends WeightDbManager<ValueObjectClass> {

	private int priorityBottomLine=Integer.MIN_VALUE;
	
	public TaskWeightDbManager(String homePath, Class objCls) {
		super(homePath, objCls);
	}
	
	

	@Override
	public long size() {
		long size=0;
		Cursor cursor;
		DatabaseEntry key=new DatabaseEntry();
		DatabaseEntry value=new DatabaseEntry();
		OperationStatus retVal=null;
		cursor=db.getMainDb().openCursor(null, null);
		retVal=cursor.getFirst(key, value, LockMode.DEFAULT);
		for (;;){
           	if (retVal==OperationStatus.SUCCESS){
           		ValueObjectClass valueObj=(ValueObjectClass) db.getSerialBinding().entryToObject(value);
           		if (valueObj.getWeight()>=priorityBottomLine)
           			size++;
  			}
  			else break;
  			retVal=cursor.getNext(key,value, LockMode.DEFAULT);
  		}
		if (cursor!=null) cursor.close();
		return size;
	}



	/*根据weight从库中获取一个不和传入参数相同的条目,没有任何条目返回null
	 * 若priorityDown为true,取出时优先级减1
	 */
	public ValueObjectClass takeoutNextNoDup(List<byte[]> listKeyBytes){
		return takeoutNextNoDup(listKeyBytes,false);
	}
	public ValueObjectClass takeoutNextNoDup(List<byte[]> listKeyBytes,boolean priorityDown){
		Transaction txn=db.getEnv().beginTransaction(null, db.getTxnConf());
		DatabaseEntry searchKey = new DatabaseEntry();
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundValue=new DatabaseEntry();
		ValueObjectClass valueObj=null;
		SecondaryCursor secCursor=null;
		try{
			secCursor=db.getSecDbByWeight().openSecondaryCursor(txn, null);
			if (secCursor.getLast(searchKey,foundKey, foundValue, LockMode.DEFAULT)==OperationStatus.SUCCESS){
				for (boolean noDup;;){
					noDup=true;
					byte[] curBytes=foundKey.getData();
					if (listKeyBytes!=null)
						for (byte[] existedBytes:listKeyBytes)
							if (Util.byteArrayEquals(curBytes, existedBytes)){
								/*出现了重复条目*/
								noDup=false;
								break;
							}
					/*不重复的ValueObjectClass*/
					if (noDup){
						valueObj=(ValueObjectClass)db.getSerialBinding().entryToObject(foundValue);
						if (valueObj.getWeight()<priorityBottomLine) return null; //所有随后的任务优先级过低
						else return valueObj;
					}
					/*没有更多条目了*/
					if (secCursor.getPrev(searchKey,foundKey, foundValue, LockMode.DEFAULT)!=OperationStatus.SUCCESS)
						return null;
				}
			}
			return null;
		}
		catch(Exception e){throw new RuntimeException(e);}
		finally{
			if (secCursor!=null) secCursor.close();
			if (txn!=null) txn.commit();
			/*优先级自减*/
			if (priorityDown && valueObj!=null)
				if (updateWeight(foundKey.getData(),valueObj.getWeight()-1)!=OperationStatus.SUCCESS){
					throw new RuntimeException("优先级自减失败");
				}
		}
	}

	public int getPriorityBottomLine() {
		return priorityBottomLine;
	}

	public void setPriorityBottomLine(int priorityBottomLine) {
		this.priorityBottomLine = priorityBottomLine;
	}
	
	
	
}
