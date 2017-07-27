package me.costa.gustavo.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

public class App {
	private final static Logger LOGGER = Logger.getLogger(App.class.getName());
	private static List<Row> data = new ArrayList<Row>();
	private static StructType schema = new StructType(new StructField[] {
			new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });
	private static Word2Vec word2Vec;
	private static SparkConf conf = new SparkConf().setAppName("ClusterScore").setMaster("local").set("spark.sql.warehouse.dir","file///d:/temp");//.set("spark.storage.memoryFraction", "1");
	private static SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

	public static void main(String[] args) throws IOException, SAXException, TikaException {
		LOGGER.info("Hello World!");
		LOGGER.info("------------------------------------------------------------------");
		//LOGGER.info(autoDetectParseToStringExample("teste.pdf").trim());

		Path source = Paths.get("D:\\Desenvolvimento\\Projetos\\Text-Similarity-Cluster\\docs_example");
		try {
			Files.walkFileTree(source, new MyFileVisitor());
			treinar();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String autoDetectParseToStringExample(String path) throws IOException, SAXException, TikaException {
		BodyContentHandler handler = new BodyContentHandler();
		AutoDetectParser parser = new AutoDetectParser();
		org.apache.tika.metadata.Metadata metadata = new org.apache.tika.metadata.Metadata();
		try (InputStream stream = new FileInputStream(path)) {
			parser.parse(stream, handler, metadata);
			stream.close();
			return handler.toString();
		}

	}

	public static void treinar() {

		Dataset<Row> documentDF = spark.createDataFrame(data, schema);
		// Learn a mapping from words to Vectors.
		word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(data.size())
				.setMinCount(0);

		Word2VecModel model = word2Vec.fit(documentDF);
		Dataset<Row> result = model.transform(documentDF);
		LOGGER.info(model.getVectors()+"");
		imprimir(result);
		spark.stop();
	}

	private static void imprimir(Dataset<Row> result) {
		for (Row row : result.collectAsList()) {
			List<String> text = row.getList(0);
			DenseVector vector = (DenseVector) row.get(1);
			LOGGER.info("Text: " + text + " => \nVector: " + vector + "\n");
			LOGGER.info("=======================================================================================================================");
		}
	}

	public static void includeRow(String text) {
		data.add(RowFactory.create(Arrays.asList(text.split(" "))));
	}

}
