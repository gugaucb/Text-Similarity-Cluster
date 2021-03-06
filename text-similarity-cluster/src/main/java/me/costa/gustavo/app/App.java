package me.costa.gustavo.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
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

public class App {
	/*private static final int NUM_CLUSTERS = 3;
	private static final int NUM_INTERACOES = 20;
	private static final int MIN_COUNT = 15;
	private static final int TAMANHO_VETOR = 5;
	*/
	private static final int NUM_CLUSTERS = 3;
	private static final int NUM_INTERACOES = 20;
	private static final int MIN_COUNT = 15;
	private static final int TAMANHO_VETOR = 1000;
	
	private final static Logger LOGGER = Logger.getLogger(App.class.getName());
	private static List<Row> data = new ArrayList<Row>();
	private static StructType schema = new StructType(new StructField[] {
			new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });
	private static Word2Vec word2Vec;
	private static SparkConf conf = new SparkConf().setAppName("ClusterScore").setMaster("local").set("spark.sql.warehouse.dir","file///d:/temp");//.set("spark.storage.memoryFraction", "1");
	private static SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
	private static KMeansModel clusters;
	private static Word2VecModel model;
	private static Map<String, String> arquivos = new HashMap<String, String>(); 
	
	public static void main(String[] args) throws IOException, SAXException, TikaException {
		
		
		LOGGER.info("Hello World!");
		LOGGER.info("------------------------------------------------------------------");
		//LOGGER.info(autoDetectParseToStringExample("teste.pdf").trim());

		Path source = Paths.get("D:\\Users\\tr300869\\Documents\\docs");
		try {
			Files.walkFileTree(source, new MyFileVisitor());
			treinar(data);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String autoDetectParseToStringExample(String path) throws IOException, SAXException, TikaException {
		BodyContentHandler handler = new BodyContentHandler(-1);
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
		word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(TAMANHO_VETOR)
				.setMinCount(MIN_COUNT);

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
			LOGGER.info("Vector: " + vector + "\n");
			//LOGGER.info("Text: " + text + " => \nVector: " + vector + "\n");
			LOGGER.info("=======================================================================================================================");
		}
	}
	
	private static void testa(){
		List<List<String>> listas_predict = initList();
		
		
		for(Map.Entry<String, String> entry : arquivos.entrySet()) {
			List<Row> dataTemp = new ArrayList<Row>();
			try {
				
				    String nome = entry.getKey();
				    String path = entry.getValue();
					//dataTemp.add(RowFactory.create(Arrays.asList(autoDetectParseToStringExample("D:\\Users\\tr300869\\Documents\\docs\\Atendimento\\6 - Termo de Referência - Serviço de Atendimento ao Usuário_0.1.doc").split(" "))));
					//dataTemp.add(RowFactory.create(Arrays.asList(autoDetectParseToStringExample("D:\\Users\\tr300869\\Documents\\docs\\Demandas\\6 - TR - Solucao de Gestao de Demandas_v1 (2).docx").split(" "))));
				    dataTemp.add(RowFactory.create(Arrays.asList(autoDetectParseToStringExample(path).split(" "))));
					
					Dataset<Row> documentDF = spark.createDataFrame(dataTemp, schema);
					JavaRDD<Vector> countVectors = model.transform(documentDF)
				              .select("result").toJavaRDD()
				              .map(new Function<Row, Vector>() {
				                public Vector call(Row row) throws Exception {
				                    return Vectors.dense(((DenseVector)row.get(0)).values());
				                }
				              });
					Integer predict = predictKMeans(countVectors).collect().get(0);
					//copyFileAfterPredict(nome, path, predict);
					listas_predict.get(predict).add(nome);
					LOGGER.info("Predict: "+nome+" - "+predictKMeans(countVectors).collect().get(0));
					LOGGER.info("PredictAll: "+predictKMeans(countVectors).collect());
				} catch (IOException | SAXException | TikaException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		for (int i = 0; i < listas_predict.size(); i++) {
			List<String> list = listas_predict.get(i);
			LOGGER.warning("Predictions "+i+": "+Arrays.toString(list.toArray())+" Quantidade: "+list.size());
			
		}
	}

	private static List<List<String>> initList() {
		List<List<String>> listas_predict = new ArrayList<List<String>>(4);
		for (int i = 0; i < NUM_CLUSTERS; i++) {
			listas_predict.add(new ArrayList<String>());
			
		}
		return listas_predict;
	}

	private static void copyFileAfterPredict(String nome, String path, Integer predict) throws IOException, FileNotFoundException {
		File diretorio = new File("D:\\Users\\tr300869\\Documents\\docs\\"+predict+"\\");
		diretorio.mkdirs();
		
		Files.copy(new FileInputStream(new File(path)), Paths.get("D:\\Users\\tr300869\\Documents\\docs\\"+predict+"\\"+nome), StandardCopyOption.REPLACE_EXISTING);
	}
	
	
	private static JavaRDD<Integer> predictKMeans(JavaRDD<Vector> resultRDD1){
		return clusters.predict(resultRDD1);
	}
	private static void treinarKMeans(JavaRDD<Vector> resultRDD){
		  
		resultRDD.cache();
		
		
		 // Cluster the data into two classes using KMeans
	    int numClusters = NUM_CLUSTERS;
	    int numIterations = NUM_INTERACOES;
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
	
	public static void incluirArquivo(String nome, String path){
		arquivos.put(nome, path);
	}

}
