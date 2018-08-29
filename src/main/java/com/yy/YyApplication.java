package com.yy;

import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.yy.readExcel.FileVisitor;
import com.yy.readExcel.FileVisitorBase;
import com.yy.readExcel.Reader;
import com.yy.readExcel.Writer;
import com.yy.util.PropertiesReader;


public class YyApplication {

	public static void main(String[] args) {
		FileVisitor fileVisitor = new FileVisitor();
		FileVisitorBase fileVisitorBase = new FileVisitorBase();
		String upload_path=PropertiesReader.getProperty("upload_path");
		String base_path=PropertiesReader.getProperty("base_path");
		
		Path basePath =FileSystems.getDefault().getPath(base_path);
		Reader.visitFiles(basePath, fileVisitorBase);
		
		Path path =FileSystems.getDefault().getPath(upload_path);
		Reader.visitFiles(path, fileVisitor); 
		
		String output_path=PropertiesReader.getProperty("output_path");
		String output_result_path=PropertiesReader.getProperty("output_result_path");
		Path outputPath =FileSystems.getDefault().getPath(output_path);
		
		Writer.writeFiles(outputPath, output_result_path);
	}
}
