package me.costa.gustavo.app;

import static spark.Spark.get;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

public class App {
	private final static Logger LOGGER = Logger.getLogger(App.class.getName());
	
	public static void main(String[] args) throws IOException, SAXException, TikaException {
		LOGGER.info("Hello World!");
		get("/hello", (req, res) -> "Hello World");
		LOGGER.info(parseToStringExample());
		
	}
	
	public static String parseToStringExample() throws IOException, SAXException, TikaException {
	    Tika tika = new Tika();
	    try (InputStream stream = new FileInputStream(App.class.getClassLoader().getResource("teste.pdf").getPath())) {
	        return tika.parseToString(stream);
	    }
	}
}
