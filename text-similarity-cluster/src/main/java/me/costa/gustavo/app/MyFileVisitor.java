package me.costa.gustavo.app;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

public class MyFileVisitor extends SimpleFileVisitor<Path> {
	public FileVisitResult visitFile(Path path, BasicFileAttributes fileAttributes){
        System.out.println("Nome do arquivo:" + path.getFileName());
        try {
			App.includeRow(App.autoDetectParseToStringExample(path.toUri().getPath()));
		} catch (IOException | SAXException | TikaException e) {
			e.printStackTrace();
		}
        return FileVisitResult.CONTINUE;
    }
    public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes fileAttributes){
        System.out.println("----------Nome do diretório:" + path + "----------");
        return FileVisitResult.CONTINUE;
    }
}
