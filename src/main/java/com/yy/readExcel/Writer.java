package com.yy.readExcel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;

public class Writer {
	private static Logger logger = Logger.getLogger(Writer.class);
		
	public static void writeFiles(Path path,String output) {
		try {
			InputStream is = new FileInputStream(path.toFile());
			Workbook wb = WorkbookFactory.create(is);
			FileOutputStream excelFileOutPutStream = new FileOutputStream(output);//写数据到这个路径上
			
			LinkedHashMap<String,LinkedHashMap<Integer,LinkedHashMap<String,String>>> rowsTotalMap = ReadExcel.rowsTotalMap;
			
			for(Entry<String, LinkedHashMap<Integer, LinkedHashMap<String, String>>> entry0 : rowsTotalMap.entrySet()){
				String k = entry0.getKey();  
				LinkedHashMap<Integer, LinkedHashMap<String, String>> v = entry0.getValue();
				XSSFSheet sheet =  (XSSFSheet) wb.getSheet(k);
				int rowNum = 2;
				for(Entry<Integer, LinkedHashMap<String, String>> entry : v.entrySet()){
				    LinkedHashMap<String, String> mapValue = entry.getValue();
				    XSSFRow hssfRow = sheet.getRow(rowNum);//得到行
				   
				    boolean isNewCreate =false;
				    if(hssfRow==null) {
				    	hssfRow = sheet.createRow(rowNum);
				    	isNewCreate=true;
				    }
				    
				    CellStyle style = wb.createCellStyle();
				    style.setBorderBottom(HSSFCellStyle.BORDER_THIN); //下边框
				    style.setBorderLeft(HSSFCellStyle.BORDER_THIN);//左边框
				    style.setBorderTop(HSSFCellStyle.BORDER_THIN);//上边框
				    style.setBorderRight(HSSFCellStyle.BORDER_THIN);//右边框
				    				    
				    for(Entry<String, String> entry2 : mapValue.entrySet()) {
				    	String name = entry2.getKey();
					    String value = entry2.getValue();

					    if(name.equals("编码（金蝶）")) {
							Writer.setCellValue(hssfRow, 0, value, isNewCreate,style);
					    }else if(name.equals("名称")) {
					    	Writer.setCellValue(hssfRow, 1, value, isNewCreate,style);
					    }else if(name.equals("规格")) {
					    	Writer.setCellValue(hssfRow, 2, value, isNewCreate,style);
					    }else if(name.equals("厂家")) {
					    	Writer.setCellValue(hssfRow, 3, value, isNewCreate,style);
					    }else if("单位（整）".contains(name)) {
					    	Writer.setCellValue(hssfRow, 4, value, isNewCreate,style);
					    }else if(name.equals("上次盘点量")) {
					    	Writer.setCellValue(hssfRow, 5, value, isNewCreate,style);
					    }else if(name.equals("本月实领")) {
					    	Writer.setCellValue(hssfRow, 6, value, isNewCreate,style);
					    }else if(name.contains("盘点数量")) {
					    	Writer.setCellValue(hssfRow, 7, value, isNewCreate,style);
					    }
					    
				    }
				    
				    Writer.setCellValue(hssfRow, 8, "0", isNewCreate,style);
				    
				    rowNum++;
				}
			}
			
			wb.write(excelFileOutPutStream);
			excelFileOutPutStream.flush();
			excelFileOutPutStream.close();

		} catch (Exception e) {
			e.printStackTrace();
		} 
		
	}

	public static void setCellValue (XSSFRow hssfRow,int index,String value,boolean isNewCreate,CellStyle style) {
		if(isNewCreate) {
			XSSFCell hssfCell = hssfRow.createCell(index);
			hssfCell.setCellValue(value);
			hssfCell.setCellStyle(style);
		}else {
			XSSFCell hssfCell = hssfRow.getCell(index);//得到列
			hssfCell.setCellValue(value);
			hssfCell.setCellStyle(style);
		}
		
	}
}
