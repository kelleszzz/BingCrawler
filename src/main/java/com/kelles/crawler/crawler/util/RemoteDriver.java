package com.kelles.crawler.crawler.util;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.kelles.crawler.crawler.setting.Setting;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriverService;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;

public class RemoteDriver {
	
	private static volatile int usingServiceCount=0; //调用stopService()后减1,降为0才关闭service
	
	//使用指南
	public static void main(java.lang.String[] args) throws Exception{
		//静态初始化服务
		RemoteDriver.startService();
		//初始化一个浏览器,且不加载图片
		RemoteDriver remoteDriver=new RemoteDriver(false);
		//获取创建的WebDriver
		WebDriver driver=remoteDriver.getDriver();
		driver.get("http://www.baidu.com");
		Thread.sleep(5000);
		//关闭这个浏览器
		remoteDriver.quitDriver();
		//静态关闭服务
		RemoteDriver.stopService();
		
		//静态初始化服务
		RemoteDriver.startService();
		//初始化一个浏览器,且不加载图片
		RemoteDriver remoteDriver2=new RemoteDriver(false);
		//获取创建的WebDriver
		WebDriver driver2=remoteDriver.getDriver();
		driver2.get("http://www.bing.com");
		Thread.sleep(5000);
		//关闭这个浏览器
		remoteDriver2.quitDriver();
		//静态关闭服务
		RemoteDriver.stopService();
	}
	
	public WebDriver getDriver() {
		return driver;
	}



	private static ChromeDriverService service=null;
	  private WebDriver driver;
	   public static java.lang.String DRIVER_PATH= Setting.CHROME_DRIVER_PATH;
	   public static java.lang.String USER_DATA= Setting.CHROME_USER_DATA;
	   
	   public static void startService() throws IOException {
		   if (service==null){
			   service = new ChromeDriverService.Builder()
				         .usingDriverExecutable(new File(DRIVER_PATH))
				         .usingAnyFreePort()
				         .build();
				     service.start();
		   }
		   usingServiceCount++;
	   }


	   public static void stopService() {
		   if (service!=null && (--usingServiceCount)<=0){
			   service.stop();
			   service=null;
		   }
	   }

	   public RemoteDriver(boolean loadImages) {
		   ChromeOptions options = new ChromeOptions();
		   options.addArguments("user-data-dir="+USER_DATA); //使用已有配置,不加载图片
		   DesiredCapabilities capabilities = DesiredCapabilities.chrome();
		   capabilities.setCapability(ChromeOptions.CAPABILITY, options);
		   //获取driver
		   driver = new RemoteWebDriver(service.getUrl(),capabilities);
		   //findElement等待超时
		   driver.manage().timeouts().implicitlyWait(500, TimeUnit.MILLISECONDS); 
	}

	public void quitDriver() {
	     driver.quit();
	   }
}
