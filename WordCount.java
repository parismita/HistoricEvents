
//Set appropriate package name

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;



/**
 * This class uses RDDs to obtain word count for each word; json files are treated as text file
 * The year-month is obtained as a dataset of String
 * */


public class WordCount {
	static//Input dir - should contain all input json files
	String inputPath="/home/parismita/spark"; //Use absolute paths 
	
	static //Output dir - this directory will be created by spark. Delete this directory between each run
	String outputPath="/home/parismita/spark";   //Use absolute paths
	
	static
	SparkSession sparkSession = SparkSession.builder()
			.appName("CS 631")		//Name of application
			.master("local")								//Run the application on local node
			.config("spark.sql.shuffle.partitions","2")		//Number of partitions
			.getOrCreate();
	static
	JavaRDD<Row> inputDataset = sparkSession.read().option("multiLine", true).json(inputPath+"/history_english.json").javaRDD();
	
	//not in use
	public static void wordCount(String [] args) {
		//Read lines from input file and create a JavaRDD
		JavaRDD<String> lines = sparkSession.read().textFile(inputPath+"/a.txt").javaRDD();

		
		//Apply the flatMap function  
	    JavaRDD<String> words = lines.flatMap(
	    			new FlatMapFunction<String, String>(){
	    				public Iterator<String> call(String line) throws Exception {
	    					line = line.toLowerCase().replaceAll("[^A-Za-z]", " ");  //Remove all punctuation and convert to lower case
	    					line = line.replaceAll("( )+", " ");   //Remove all double spaces
	    					line = line.trim(); 
	    					List<String> wordList = Arrays.asList(line.split(" ")); //Get words
	    					return wordList.iterator();
	    				}
	    			});

		//Emit a <Word, 1> tuple for each word
	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
	    
	    //Aggregate the count into a JavaPairRdd
	    JavaPairRDD<String,Integer> wordCount = ones.reduceByKey(new Function2<Integer,Integer,Integer>(){
			public Integer call(Integer i1, Integer i2) throws Exception {
				return i1+i2;
			}
	    	
	    });
	    
	    //Alternate way to run reduceByKey using lambda function
	    //JavaPairRDD<String, Integer> wordCount = ones.reduceByKey((i1, i2) -> i1 + i2);
	    
	    //Save the output file
	    wordCount.saveAsTextFile(outputPath+"/b.txt");
	
	}
	
	//not in use
	public static void entityCount(String [] args) {// TODO Auto-generated method stub

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		StructType obj = new StructType();
		obj = obj.add("category1", DataTypes.StringType, true); // false => not nullable
		obj = obj.add("category2", DataTypes.StringType, true); // false => not nullable
		ExpressionEncoder<Row> tupleRowEncoder = RowEncoder.apply(obj);

		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);;

		Dataset<Row> ds = inputDataset.map(new MapFunction<Row,Row>(){
			public Row call(Row row) throws Exception{
				String category1 =((String)row.getAs("category1"));
				String category2 = ((String)row.getAs("category2"));
				return RowFactory.create(category1,category2);
			}
		}, tupleRowEncoder);

		// Delete all rows with null values in category1 column
		Dataset<Row> dfs = ds.filter(ds.col("category1"). isNotNull());
		// Delete all rows with null values in category2 column
		Dataset<Row> dfs2 = dfs.filter(dfs.col("category2"). isNotNull());

		// Count all values grouped by "category1","category2", and
		// Rename the resulting count column as count
		Dataset<Row> count = dfs2.groupBy("category1","category2").count().alias("count");
		
		// Ouputs the result to the folder "outputPath"
		count.toJavaRDD().saveAsTextFile(outputPath);
		
		// Outputs the dataset to the standard output
		count.show((int)count.count());
	}

	//in query 1, we only take 1 string as arg and output count of that entity with diff categories
	public static void query1(String args) {
		//entity count or category 2 count?
		//key = entity, cat1, cat2, if entity occur more than once, consider only once
		//reduce - get count of the key
		//Read lines from input file and create a JavaRDD
		//data : [category1, category2, date, description, granularity, lang]
		
		JavaRDD<Row> lines = inputDataset
				.filter(i -> (i.getAs("category1")!=null))
				.filter(i -> (i.getAs("category2")!=null))
				.filter(i -> (((String)i.getAs("description"))).contains(args));
				
		JavaPairRDD<Row, Integer> ones = lines.map(i -> RowFactory.create(i.getAs("category1"),i.getAs("category2"), args))
											.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairRDD<Row, Integer> count = ones.reduceByKey((i1, i2) -> i1 + i2);
	    
		
		//print liness
		count.foreach(item -> {
            System.out.println(item); 
        });
		
		count.saveAsTextFile(outputPath+"/b1.txt");
	}
	
	public static void query2(JavaRDD<String> scan){
		JavaPairRDD<Row, Integer> count = null;
		JavaRDD<Row> lines = inputDataset
				.filter(i -> (i.getAs("category1")!=null))
				.filter(i -> (i.getAs("category2")!=null));
		for(String item: scan.collect()) {
			JavaRDD<Row> newlines = lines.filter(i -> (((String)i.getAs("description"))).contains(item));
					
			JavaPairRDD<Row, Integer> ones = newlines.map(i -> RowFactory.create(i.getAs("category1"),i.getAs("category2"), item))
												.mapToPair(s -> new Tuple2<>(s, 1));
			JavaPairRDD<Row, Integer> red = ones.reduceByKey((i1, i2) -> i1 + i2);
			if(count==null) count=red;
			else count = count.union(red);
			
		}
		//print liness
		count.foreach(items -> {
            System.out.println(items); 
        });
		count.saveAsTextFile(outputPath+"/b2.txt");
		
	}
	
	public static void query3() {
		//granularity - year if statement
		//emit date on map - count date on reduce
		JavaRDD<Row> lines = inputDataset
				.filter(i -> (i.getAs("date")!=null))
				.filter(i -> (i.getAs("granularity")!="year"));
		
		JavaPairRDD<String, Integer> ones = lines.mapToPair(s -> new Tuple2<>(
				((String)s.getAs("date")).contains("/")?((String)s.getAs("date")).split("/")[0]:s.getAs("date"), 1));
		
	    //Aggregate the count into a JavaPairRdd
		JavaPairRDD<String, Integer> count = ones.reduceByKey((i1, i2) -> i1 + i2);
		
		//print lines
		count.foreach(item -> {
            System.out.println(item); 
        });
		
		count.saveAsTextFile(outputPath+"/b3.txt");
	}
	
	public static void query4() {
		//sort by count
		JavaRDD<Row> lines = inputDataset
				.filter(i -> (i.getAs("date")!=null))
				.filter(i -> (i.getAs("granularity")!="year"));
		
		JavaPairRDD<String, Integer> ones = lines.mapToPair(s -> new Tuple2<>(
				((String)s.getAs("date")).contains("/")?((String)s.getAs("date")).split("/")[0]:s.getAs("date"), 1));
		
	    //Aggregate the count into a JavaPairRdd
		JavaPairRDD<String, Integer> count = ones.reduceByKey((i1, i2) -> i1 + i2);
		
		
		JavaRDD<Row> sort = count.mapToPair(s -> new Tuple2<>(s._2, s._1))
				.sortByKey(false)
				.map(s -> (Row) RowFactory.create(s._2, s._1))
				.map(new Function<Row, Row>() {
					int i=0, j=1, k=0;
					public Row call(Row s) throws Exception{
						if((int) s.get(1)==j) {
							k++;
							return RowFactory.create(s.get(0), s.get(1), i);
						}
						else {
							i=i+k;
							k=0;
							j=s.getInt(1);
							return RowFactory.create(s.get(0), s.get(1), ++i);
						}
					};
				});
		
		
		//print lines
		sort.foreach(item -> {
            System.out.println(item); 
        });
		
		sort.saveAsTextFile(outputPath+"/b4.txt");
	}
	
	public static void main(String[] args) {
		System.out.print("Enter Query Number: ");  
		Scanner input= new Scanner(System.in);
		int queryNo = input.nextInt();
		if(queryNo==1) {
			//take stdin
			System.out.print("Enter Entity: ");  
			String arg = input.next();
			query1(arg);
		}
		else if(queryNo==2) {
			//take entity.txt
			String file = "/home/parismita/spark/entities.txt";
			JavaRDD<String> scan = sparkSession.read().textFile(file).javaRDD();
			query2(scan);
		}
		else if(queryNo==3) {
			query3();
		}
		else if(queryNo==4) {
			query4();
		}
		input.close();
	}
	
}


