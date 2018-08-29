package com.yy.readExcel;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.log4j.Logger;

public class Reader {
	private static Logger logger = Logger.getLogger(Reader.class);
		
	public static void visitFiles(Path path, FileVisitor fileVisitor) {
		try {
			Files.walkFileTree(path, fileVisitor);
		} catch (Exception e) {
			if (e.getMessage() != null) {
				logger.error("", e);
			}
		}
	}
	
	public static void visitFiles(Path path, FileVisitorBase fileVisitor) {
		try {
			Files.walkFileTree(path, fileVisitor);
		} catch (Exception e) {
			if (e.getMessage() != null) {
				logger.error("", e);
			}
		}
	}
	
}
