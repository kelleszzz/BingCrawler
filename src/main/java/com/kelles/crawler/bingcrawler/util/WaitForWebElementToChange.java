package com.kelles.crawler.bingcrawler.util;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;

import com.google.common.base.Function;

public class WaitForWebElementToChange {
	private WebDriver driver=null;
	private By by=null;
	private String originalText=null;

	
	
	public static WaitForWebElementToChange newInstance(WebDriver driver,By by) {
		WaitForWebElementToChange instance=new WaitForWebElementToChange();
		instance.driver = driver;
		instance.by=by;
		instance.originalText=instance.getCurText();
//		System.out.println("[原始内容]\n"+instance.originalText); //
		return instance;
	}

	private WaitForWebElementToChange() {
		super();
	}

	private String getCurText(){
		try{
			String curText=null;
			WebElement target=driver.findElement(by);
			curText=target.getText();
			return curText;
		}catch(NoSuchElementException e){return null;}
	}
	
	//等待getText改变
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean waitForChange(long milliseconds){
		if (originalText==null) return false; //找不到Element,不进行等待
		try{
			Wait wait=new FluentWait(driver)
					.withTimeout(milliseconds, TimeUnit.MILLISECONDS)
					.pollingEvery(milliseconds/3,TimeUnit.MILLISECONDS)
					.ignoring(NoSuchElementException.class);
			boolean success=(boolean) wait.until(new Function<WebDriver,Boolean>(){
				@Override
				public Boolean apply(WebDriver driver) {
					String curText=getCurText();
//					VersionUtils.log(11.26,"[此刻内容]\n"+curText); //
					//无法找到Element
					if (originalText==null || curText==null) {
//						VersionUtils.log(11.26,"[无法找到]\n");//
						return Boolean.FALSE;
					}
					//内容改变
					else if (originalText!=null && curText!=null && !originalText.equals(curText)){
//						VersionUtils.log(11.26,"[内容改变]\n"+curText);//
						return Boolean.TRUE;
					}
					else throw new NoSuchElementException("");
				}
			});
		}catch(TimeoutException e){return false;}
		return false;
	}
}
