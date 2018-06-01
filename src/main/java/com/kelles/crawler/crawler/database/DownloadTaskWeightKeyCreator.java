package com.kelles.crawler.crawler.database;

import com.kelles.crawler.crawler.download.DownloadTask;
import com.kelles.crawler.crawler.util.*;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;

public class DownloadTaskWeightKeyCreator  implements SecondaryKeyCreator{
	public static SerialBinding serialBinding;

	public DownloadTaskWeightKeyCreator(SerialBinding serialBinding) {
		super();
		this.serialBinding = serialBinding;
	}
	

	@Override
	public boolean createSecondaryKey(SecondaryDatabase secDb,
    DatabaseEntry keyEntry, 
    DatabaseEntry dataEntry,
    DatabaseEntry resultEntry) {
		DownloadTask task=(DownloadTask) serialBinding.entryToObject(dataEntry);
		resultEntry.setData(Util.intToByteArray(task.getWeight()));
		return true;
	}
}
