package me.costa.gustavo.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class App {
	private final static Logger LOGGER = Logger.getLogger(App.class.getName());
	
	public static void main(String[] args) throws IOException, SAXException, TikaException {
		LOGGER.info("Hello World!");
		LOGGER.info("------------------------------------------------------------------");
		LOGGER.info(autoDetectParseToStringExample().trim());
		
	}
	
	public static String autoDetectParseToStringExample() throws IOException, SAXException, TikaException {
		BodyContentHandler handler = new BodyContentHandler();
	    AutoDetectParser parser = new AutoDetectParser();
	    Metadata metadata = new Metadata();
	    try (InputStream stream = new FileInputStream(App.class.getClassLoader().getResource("teste.pdf").getPath())) {
	        parser.parse(stream, handler, metadata);
	        stream.close();
	        return handler.toString();
	    }
	    
	}
}
