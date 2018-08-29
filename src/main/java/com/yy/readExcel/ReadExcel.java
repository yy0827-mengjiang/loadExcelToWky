package com.yy.readExcel;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.google.common.collect.Lists;



public class ReadExcel {
	private static Logger logger = Logger.getLogger(ReadExcel.class);
	
	public static LinkedHashMap<String,String> baseMap = new LinkedHashMap<String,String>();
	public static LinkedHashMap<String,LinkedHashMap<Integer,LinkedHashMap<String,String>>> rowsTotalMap = new LinkedHashMap<String,LinkedHashMap<Integer,LinkedHashMap<String,String>>>();	
	
	public static boolean readExcelBase(File file){
		boolean is_success=true;
		
		if (file == null) {
			is_success=false;
			return is_success;
		}

		try{
			Workbook wb = WorkbookFactory.create(new FileInputStream(file));
			Sheet sheet = wb.getSheetAt(0);
			Iterator<Row> row = sheet.rowIterator();
			
			// 跳过几行
			for (int i = 0; i < 1; i++) {
				row.next();
			}
			
			int rno = 0;
			LinkedHashMap<Integer,String> titleMap = new LinkedHashMap<Integer,String>();
			LinkedHashMap<Integer,LinkedHashMap<String,String>> rowsMap = new LinkedHashMap<Integer,LinkedHashMap<String,String>>();
			int cellLength = 0 ; 
			LinkedHashMap<String,String> rowMap = new LinkedHashMap<String,String>();
			
			//从标题开始
			while (row.hasNext()) {
				Row rown = row.next();
				rno = rown.getRowNum() + 1;

				Iterator<Cell> itList = rown.cellIterator();
                List<Cell> cellList = Lists.newArrayList(itList);
				if(titleMap.isEmpty()) {
					for (int i = 0; i < cellList.size(); i++) {
						Cell cell = cellList.get(i);
	                    	if(cell.getCellType()==Cell.CELL_TYPE_NUMERIC) {
	                    		titleMap.put(i, cell.getNumericCellValue()+"");
	                    	}else {
	                    		titleMap.put(i, cell.getStringCellValue());
	                    	}
					}
					cellLength = cellList.size();
				}else {
					if(cellLength <= cellList.size()) {

						StringBuilder wlmc = new StringBuilder();
						StringBuilder xbm = new StringBuilder();
						titleMap.forEach((k,v) ->{
							Cell cell = cellList.get(k);
							if(v.trim().equals("物料名称")) {
								if(cell.getCellType()==Cell.CELL_TYPE_NUMERIC) {
									wlmc.append(cell.getNumericCellValue()+"");
		                    	}else {
		                    		wlmc.append(cell.getStringCellValue().trim());
		                    	}
							}else if(v.trim().equals("新编码")) {
								if(cell.getCellType()==Cell.CELL_TYPE_NUMERIC) {
									xbm.append(cell.getNumericCellValue()+"");
		                    	}else {
		                    		xbm.append(cell.getStringCellValue().trim());
		                    	}
							}							
						});
						rowMap.put(wlmc.toString(), xbm.toString());
						rowsMap.put(rno, rowMap);
					}else {
						System.out.println("break rows:"+rno+" cellLength:"+cellLength+" cellList.size():"+cellList.size());
						break;
					}					
				}
			}
			
//			rowMap.forEach((k,v) ->{
//				System.out.println(k+":"+v.toString());
//			});

			baseMap = rowMap;
			
		} catch (Exception ex) {
			logger.error("出错", ex);
			ex.printStackTrace();
			return false;
		}
		return is_success;
	}
	

	public static boolean readExcelAndInsert(File file){
		boolean is_success=true;
		
		if (file == null) {
			is_success=false;
			return is_success;
		}

		try {
			Workbook wb = WorkbookFactory.create(new FileInputStream(file));
			Sheet sheet = wb.getSheetAt(0);
			Iterator<Row> row = sheet.rowIterator();
			String[] fileNames = file.getName().trim().split("燕岭");
			String[] monthAndYear =fileNames[0].trim().split("-");
			
			String sheetName = monthAndYear[0]+(monthAndYear[1].length()==1?"0"+monthAndYear[1]:monthAndYear[1]);
			System.out.println("文件名："+sheetName);
			
			if(sheetName.equals("201710")) {
				for (int i = 0; i < 1; i++) {
					row.next();
				}
			}else {
				for (int i = 0; i < 2; i++) {
					row.next();
				}
			}			
			
			int rno = 0;
			LinkedHashMap<Integer,String> titleMap = new LinkedHashMap<Integer,String>();
			LinkedHashMap<Integer,LinkedHashMap<String,String>> rowsMap = new LinkedHashMap<Integer,LinkedHashMap<String,String>>();
			int cellLength = 0 ; 
			
			//从标题开始
			while (row.hasNext()) {
				Row rown = row.next();
				rno = rown.getRowNum() + 1;

				Iterator<Cell> itList = rown.cellIterator();
                List<Cell> cellList = Lists.newArrayList(itList);
				if(titleMap.isEmpty()) {
					for (int i = 0; i < cellList.size(); i++) {
						Cell cell = cellList.get(i);
	                    	if(cell.getCellType()==Cell.CELL_TYPE_NUMERIC) {
	                    		titleMap.put(i, cell.getNumericCellValue()+"");
	                    	}else {
	                    		titleMap.put(i, cell.getStringCellValue());
	                    	}
					}
					cellLength = cellList.size();
				}else {
					if(cellLength <= cellList.size()) {
						LinkedHashMap<String,String> rowMap = new LinkedHashMap<String,String>();
						titleMap.forEach((k,v) ->{
							Cell cell = cellList.get(k);
							if(cell.getCellType()==Cell.CELL_TYPE_NUMERIC) {
								rowMap.put(v, cell.getNumericCellValue()+"");
	                    	}else {
	                    		rowMap.put(v, cell.getStringCellValue());
	                    	}
						});
						
						String mc = rowMap.get("名称");
//						String bm = rowMap.get("编码");
						String newBm = baseMap.get(mc.trim());
						if(newBm!=null && !newBm.equals("")) {
							rowMap.put("编码（金蝶）", newBm);
						}
						rowsMap.put(rno, rowMap);
					}else {
						System.out.println("break rows:"+rno+" cellLength:"+cellLength+" cellList.size():"+cellList.size());
						break;
					}					
				}
			}
			
//			rowsMap.forEach((k,v) ->{
//				System.out.println("第"+k+"行："+v.toString());
//			});

			rowsTotalMap.put(sheetName, rowsMap);
		} catch (Exception ex) {
			logger.error("出错", ex);
			ex.printStackTrace();
			return false;
		}
		return is_success;
	}
	
}
