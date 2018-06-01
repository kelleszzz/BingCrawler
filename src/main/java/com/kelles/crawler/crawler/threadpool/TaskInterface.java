package com.kelles.crawler.crawler.threadpool;

import com.kelles.crawler.crawler.database.weightdb.WeightInterface;

import java.io.Serializable;

public interface TaskInterface extends WeightInterface,Serializable{
	public boolean execute();
	public String describe();
	public byte[] getKeyBytes();
}
