package com.kelles.crawler.crawler.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

public class PdfBox {
	public static String get(String pdfPath) throws Exception {
        File pdfFile = new File( pdfPath );
        return get(pdfFile);
	}
	public static String get(File pdfFile) throws Exception {
        InputStream input = null;
        PDDocument document = null;
        try
        {
            input = new FileInputStream( pdfFile );
            //加载 pdf 文档
            document = PDDocument.load(pdfFile);
            //获取内容信息
            PDFTextStripper pts = new PDFTextStripper();
            String content = "";
            try
            {
                content = pts.getText( document );
            }
            catch(Exception e)
            {
                throw e;
            }
            return content;
    
        }
        catch(Exception e)
        {
            throw e;
        }
        finally
        {
            if( null != input )
                input.close();
            if( null != document )
                document.close();
        }
    }
}
