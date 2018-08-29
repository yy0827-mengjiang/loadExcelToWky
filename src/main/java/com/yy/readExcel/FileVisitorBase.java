package com.yy.readExcel;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.log4j.Logger;

import com.yy.util.PropertiesReader;

public class FileVisitorBase extends SimpleFileVisitor<Path> {
	private static Logger logger = Logger.getLogger(FileVisitorBase.class);

	@Override
	public FileVisitResult postVisitDirectory(Path arg0, IOException arg1) throws IOException {
		// TODO Auto-generated method stub
		String upload_path = PropertiesReader.getProperty("base_path");
		String fileName = arg0.getFileName().toString();
		if (upload_path.endsWith(fileName)) {
			logger.info("整个文件夹遍历完成！");
		}
		return super.postVisitDirectory(arg0, arg1);
	}

	@Override
	public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
		// TODO Auto-generated method stub
		// return super.preVisitDirectory(dir, attrs);
		logger.info("正在访问：" + dir + "目录");
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile(Path arg0, BasicFileAttributes arg1) throws IOException {
		logger.info("正在访问" + arg0 + "文件");
		String fileName = arg0.getFileName().toString();
		logger.info("开始读取" + arg0.getFileName().toString() + "文件");
		File oldFile = arg0.toFile();
		boolean is_success = ReadExcel.readExcelBase(oldFile);

		if (!is_success) {// 如果读取插入失败
			logger.error(fileName + "文件失败！");
		} else {// 如果读取插入成功
			logger.info(fileName+"文件成功！");
		}
		return FileVisitResult.CONTINUE;
	}

	// 在访问失败时触发该方法
	@Override
	public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
		// 写一些具体的业务逻辑
		logger.info("访问失败！");
		return super.visitFileFailed(file, exc);
	}

}
