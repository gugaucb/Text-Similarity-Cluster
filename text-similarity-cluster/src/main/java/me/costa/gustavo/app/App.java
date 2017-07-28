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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

public class App {
	private final static Logger LOGGER = Logger.getLogger(App.class.getName());
	private static List<Row> data = new ArrayList<Row>();
	private static StructType schema = new StructType(new StructField[] {
			new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });
	private static Word2Vec word2Vec;
	private static SparkConf conf = new SparkConf().setAppName("ClusterScore").setMaster("local").set("spark.sql.warehouse.dir","file///d:/temp");//.set("spark.storage.memoryFraction", "1");
	private static SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
	private static KMeansModel clusters;
	private static Word2VecModel model;
	public static void main(String[] args) throws IOException, SAXException, TikaException {
		LOGGER.info("Hello World!");
		LOGGER.info("------------------------------------------------------------------");
		//LOGGER.info(autoDetectParseToStringExample("teste.pdf").trim());

		Path source = Paths.get("D:\\Desenvolvimento\\Projetos\\Text-Similarity-Cluster\\docs_example");
		try {
			Files.walkFileTree(source, new MyFileVisitor());
			treinar(data);
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

	
	
	public static void treinar(List<Row> data) {

		Dataset<Row> documentDF = spark.createDataFrame(data, schema);
		// Learn a mapping from words to Vectors.
		word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(9)
				.setMinCount(10);

		model = word2Vec.fit(documentDF);
		Dataset<Row> result = model.transform(documentDF);
		JavaRDD<Vector> countVectors = model.transform(documentDF)
	              .select("result").toJavaRDD()
	              .map(new Function<Row, Vector>() {
	                public Vector call(Row row) throws Exception {
	                    return Vectors.dense(((DenseVector)row.get(0)).values());
	                }
	              });
		treinarKMeans(countVectors);
		LOGGER.info(model.getVectors()+"");
		imprimir(result);
		testa();
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
	
	private static void testa(){
		List<Row> dataTemp = new ArrayList<Row>();
		try {
			dataTemp.add(RowFactory.create(Arrays.asList(autoDetectParseToStringExample("D:\\Desenvolvimento\\Projetos\\Text-Similarity-Cluster\\docs_example\\REGRAS-PARA-ELABORACAO-DE-ACOMPANHAMENTO-ESPECIAL.pdf").split(" "))));
			dataTemp.add(RowFactory.create(Arrays.asList(autoDetectParseToStringExample("D:\\Desenvolvimento\\Projetos\\Text-Similarity-Cluster\\docs_example\\teste.pdf").split(" "))));
			Dataset<Row> documentDF = spark.createDataFrame(dataTemp, schema);
			JavaRDD<Vector> countVectors = model.transform(documentDF)
		              .select("result").toJavaRDD()
		              .map(new Function<Row, Vector>() {
		                public Vector call(Row row) throws Exception {
		                    return Vectors.dense(((DenseVector)row.get(0)).values());
		                }
		              });
			LOGGER.info("Predict: "+predictKMeans(countVectors).collect());
		} catch (IOException | SAXException | TikaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static JavaRDD<Integer> predictKMeans(JavaRDD<Vector> resultRDD1){
		return clusters.predict(resultRDD1);
	}
	private static void treinarKMeans(JavaRDD<Vector> resultRDD){
		  
		resultRDD.cache();
		
		
		 // Cluster the data into two classes using KMeans
	    int numClusters = 2;
	    int numIterations = 20;
	    clusters = KMeans.train(resultRDD.rdd(), numClusters, numIterations);

	    System.out.println("Cluster centers:");
	    for (Vector center: clusters.clusterCenters()) {
	      System.out.println(" " + center);
	    }
	    double cost = clusters.computeCost(resultRDD.rdd());
	    System.out.println("Cost: " + cost);

	    // Evaluate clustering by computing Within Set Sum of Squared Errors
	    double WSSSE = clusters.computeCost(resultRDD.rdd());
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

	    // Save and load model
	    /* clusters.save(spark.sparkContext(), "file:///d://temp//KMeansModel1");
	   KMeansModel sameModel = KMeansModel.load(spark.sparkContext(),
	      "file:///d://temp//KMeansModel1");*/
	    // $example off$
	}

	public static void includeRow(String text) {
		data.add(RowFactory.create(Arrays.asList(text.split(" "))));
	}

}
