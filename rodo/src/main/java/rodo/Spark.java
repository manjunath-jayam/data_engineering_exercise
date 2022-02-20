package robo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.Strings;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.connector.expressions.Lit;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import com.mongodb.spark.config.WriteConfig;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.api.java.function.*;

public class Spark {
	public static String FeedsDir = Paths.get(System.getProperty("user.dir"), "feeds").toString();
	/////////////////// UDF
	private static UDF1<String, Boolean> isNewUDF = new UDF1<String, Boolean>() {

		@Override
		public Boolean call(String t1) {
			// TODO Auto-generated method stub
			Boolean isNew = false;
			if (t1.equalsIgnoreCase("N") || t1.equalsIgnoreCase("New")) {
				isNew = true;
			}

			return isNew;

		}

	};
	///////////////////// UDF

	///////////////////// dealer_msrp UDF
	private static UDF3<Boolean, Integer, Integer, Integer> msrpUDF = new UDF3<Boolean, Integer, Integer, Integer>() {

		@Override
		public Integer call(Boolean isnew, Integer msrp, Integer lstprice) {
			// TODO Auto-generated method stub
			Integer d = 0;
			if (isnew) {
				d = msrp;
			} else {
				d = lstprice;
			}
			return d;
		}

	};

	//////////////////////// dealer_msrp UDF

	//////////////////// DateFormaterParser UDF
	private static UDF1<String, String> dateFMTParserUDF = new UDF1<String, String>() {

		@Override
		public String call(String stringfmtdate) {
			// TODO Auto-generated method stub
			List<String> formatStrings = Arrays.asList("MM/dd/yyyy", "MM-dd-yyyy");

			for (String formatString : formatStrings) {
				try {
					Date rr = new SimpleDateFormat(formatString).parse(stringfmtdate);

					return new SimpleDateFormat("MM/dd/yyyy").format(rr);
				} catch (ParseException e) {
					System.out.println(e);

				}
			}

			String h = "";
			try {
				h = new SimpleDateFormat("MM/dd/yyyy").format(getDateWithoutTimeUsingFormat());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return h;

		}

	};

	//////////////////// DateFormaterParser UDF

	public static Date getDateWithoutTimeUsingFormat()
			throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(
				"MM/dd/yyyy");
		return formatter.parse(formatter.format(new Date()));
	}

	public static void main(String[] args) throws IOException {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// System.setProperty("hadoop.home.dir", "C:\\bigdata\\spark3");

		// TODO Auto-generated method stub
		System.out.println("Running Spark code");
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Simple Application")
				 
				.getOrCreate();

		// UDF Register
		spark.udf().register("isNewUDF", isNewUDF, DataTypes.BooleanType);
		spark.udf().register("msrpUDF", msrpUDF, DataTypes.IntegerType);
		spark.udf().register("dateFMTParserUDF", dateFMTParserUDF, DataTypes.StringType);

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		File directoryPath = new File(FeedsDir);
		// List of all files and directories
		String ProviderFolders[] = directoryPath.list();

		/* Create an Empty RDD */
		Dataset<Row> TargetDataset = createEmptyDATASET(spark);

		System.out.println("Printing schema of empty Dataset");
		// TargetDataset.printSchema();

		for (int i = 0; i < ProviderFolders.length; i++) {
			//System.out.println();
			//System.out.println("--------------------------------------------------------");
			String currentProvider = ProviderFolders[i];
			//System.out.println("Currently Working in " + currentProvider + " Folder");
			// jsc.textFile(directoryPath+"//"+ProviderFolders[i]);

			String _allDealserFilePaths = getAllDealsersFilesPaths_CSVs(currentProvider);

			// Load a DATASET from Spark
			Dataset<Row> DEALERS_DS = spark.read().format("csv").option("header", "true").load(_allDealserFilePaths);//a.czv,b.csv

			// DEALERS_DS.filter(functions.col("VIN").equalTo("3C6RR7KTXJG164521")).show(false);

			//System.out.println("Trying to map from the ProviderMappings folder, with the file " + currentProvider);
			//System.out.println("Mapping have Source and then Target");
			Map<String, String> providerMapper = getMapFromCSV(
					Paths.get(System.getProperty("user.dir"), "ProviderMappings", currentProvider + ".csv").toString());

			Dataset<Row> DEALERS_MODIFIED_DS = build_DS(DEALERS_DS, DEALERS_DS.columns(), providerMapper);

			// typeCast and add logic for needed Colums
			Dataset<Row> Finished_DS = DEALERS_MODIFIED_DS

					// Typecasted Milage
					.withColumn("mileage", DEALERS_MODIFIED_DS.col("mileage").cast(DataTypes.IntegerType))

					// is_new
					.withColumn("is_new",
							functions.call_udf("isNewUDF", functions.col("is_new")).cast(DataTypes.BooleanType))

					// dealer_year Integer
					.withColumn("dealer_year", functions.col("dealer_year").cast(DataTypes.IntegerType))

					// dealer_msrp integer,
					.withColumn("dealer_msrp", functions.col("dealer_msrp").cast(DataTypes.IntegerType))
					.withColumn("dealer_msrp",
							functions.call_udf("msrpUDF", functions.col("is_new"), functions.col("dealer_msrp"),
									functions.col("List Price")))

					// dealer_invoice integer,
					.withColumn("dealer_invoice", functions.col("dealer_invoice").cast(DataTypes.IntegerType))
					.withColumn("dealer_invoice",
							functions.call_udf("msrpUDF", functions.col("is_new"), functions.col("dealer_invoice"),
									functions.col("List Price")))

					// dealer_inventory_entry_date
					.withColumn("dealer_inventory_entry_date",

							functions.to_date(functions.call_udf("dateFMTParserUDF",
									functions.col("dealer_inventory_entry_date")), "MM/dd/yyyy")

					)

					// dealer_installed_option_codes []
					.withColumn("dealer_installed_option_codes",
							functions.split(functions.col("dealer_installed_option_codes"), "[|,]+"))

					// dealer_installed_option_descriptions []
					.withColumn("dealer_installed_option_descriptions",
							functions.split(functions.col("dealer_installed_option_descriptions"), "[|,]+"))

					// updated_at
					.withColumn("updated_at", functions.current_timestamp())

					// dealer_images []
					.withColumn("dealer_images", functions.split(functions.col("dealer_images"), "[|,]+"))

					// dealer_certified
					.withColumn("dealer_certified",
							functions
									.when(functions.col("dealer_certified").equalTo("Yes")
											.or(functions.col("dealer_certified").equalTo("YES")), true)
									.when(functions.col("dealer_certified").equalTo("No")
											.or(functions.col("dealer_certified").equalTo("NO")), false)
									.otherwise(functions.lit(null)));

			// .printSchema();

			// .show(15,false);

			//
			//System.out.println("--------------------------------------------------------");

			// Drop Invoice and list price
			Finished_DS = Finished_DS.drop("List Price", "Invoice");

			Finished_DS = rearrangeColumOrder(Finished_DS, TargetDataset.columns());
			System.out.println(Finished_DS.columns().length);
			Finished_DS.printSchema();
			Finished_DS.show(false);
			TargetDataset = TargetDataset.union(Finished_DS);

		}

		//System.out.println("^^^^^^^^^^^ TARGET DS ^^^^^^^^^^^^^^^^");

		//System.out.println(TargetDataset.columns().length);
		//System.out.println();
		TargetDataset.printSchema();
		TargetDataset.show(100, false);
		//System.out.println("Total length of entire TARGET DS " + TargetDataset.count());
		// TargetDataset.write().format("json").save("TargetDS.json");
		TargetDataset	 
		.coalesce(1).write().mode(SaveMode.Overwrite).json("data\output\dealers.json");


	}

	private static Dataset<Row> rearrangeColumOrder(Dataset<Row> df1, String[] TargetColums) {

		Column[] f = Arrays.stream(TargetColums).map(f1 -> df1.col(f1))
				.toArray(Column[]::new);

		return df1.select(f);
	}

	public static Dataset<Row> renameDS(Dataset<Row> df) {
		List<String> ColumsLists = Arrays.asList(df.columns());
		for (int i = 0; i < ColumsLists.size(); i++) {
			df = df.withColumnRenamed(ColumsLists.get(i).trim(), "S_" + ColumsLists.get(i).trim());

		}
		return df;
	}

	// List Price
	public static Dataset<Row> checkListPriceExists(Dataset<Row> df) {
		List<String> ColumsLists = Arrays.asList(df.columns());
		if (!ColumsLists.contains("S_List Price")) {
			df = df.withColumn("List Price", functions.lit(0).cast(DataTypes.IntegerType));
		} else {
			df = df.withColumn("List Price", functions.col("S_List Price").cast(DataTypes.IntegerType));
		}

		return df;
	}

	// Invoice
	public static Dataset<Row> checkInvoiceExists(Dataset<Row> df) {
		List<String> ColumsLists = Arrays.asList(df.columns());
		if (!ColumsLists.contains("S_Invoice")) {
			df = df.withColumn("Invoice", functions.lit(0).cast(DataTypes.IntegerType));
		} else {
			df = df.withColumn("Invoice", functions.col("S_Invoice").cast(DataTypes.IntegerType));
		}

		return df;
	}

	private static Dataset<Row> build_DS(Dataset<Row> df, String[] cl, Map<String, String> hsMapCsv) {

		/// Rename DS with '_S'
		df = renameDS(df);
		String[] renamedDSCols = df.columns();

		// Check if List Price exist else replace with 0
		df = checkListPriceExists(df);

		// Check ifInvoice exist else replace with 0
		df = checkInvoiceExists(df);

		List<String> ColumsLists = Arrays.asList(cl);

		for (Map.Entry<String, String> entry : hsMapCsv.entrySet()) {

			String Target_DB_KEY = entry.getKey();
			String SourceDB_KEY = entry.getValue();

			if (ColumsLists.contains(SourceDB_KEY)) {
				System.out.println(SourceDB_KEY + " found in CoulmLIst so Added the Target key --> " + Target_DB_KEY);
				df = df.withColumn(Target_DB_KEY.trim(), df.col("S_" + SourceDB_KEY));
			} else {
				System.out.println(
						SourceDB_KEY + " NOT found in CoulmLIst so Added the Target key as NULL --> " + Target_DB_KEY);
				df = df.withColumn(Target_DB_KEY.trim(), functions.lit(null));
			}

		}

		return df
				.withColumn("hash", functions.hash(functions.monotonically_increasing_id()).cast(DataTypes.StringType))
				.drop(renamedDSCols);

	}

	private static String getAllDealsersFilesPaths_CSVs(String FolderName) {

		String sub_path = Paths.get(FeedsDir, FolderName).toString();
		File sub_directoryPath = new File(sub_path);
		// List of all files and directories
		String DealerShip_CSVs[] = sub_directoryPath.list();

		List<String> All_DealershipPaths = new ArrayList<String>();

		for (int i = 0; i < DealerShip_CSVs.length; i++) {

			All_DealershipPaths.add(Paths.get(sub_path, DealerShip_CSVs[i]).toString());
		}

		return String.join(",", All_DealershipPaths);
	}

	public static Map<String, String> getMapFromCSV(final String filePath) throws IOException {

		Stream<String> lines = Files.lines(Paths.get(filePath));
		Map<String, String> resultMap = lines
				.skip(1)
				.map(line -> line.split(","))
				.collect(Collectors.toMap(line -> line[0].toString().trim(), line -> line[1].toString().trim()));

		return resultMap;
	}

	public static Dataset<Row> createEmptyDATASET(SparkSession spark) {

		StructType schema = DataTypes.createStructType(
				Arrays.asList(
						DataTypes.createStructField("hash", DataTypes.StringType, true),
						DataTypes.createStructField("dealership_id", DataTypes.StringType, false),
						DataTypes.createStructField("vin", DataTypes.StringType, false),
						DataTypes.createStructField("mileage", DataTypes.IntegerType, true),
						DataTypes.createStructField("is_new", DataTypes.BooleanType, true),
						DataTypes.createStructField("stock_number", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_year", DataTypes.IntegerType, true),
						DataTypes.createStructField("dealer_make", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_model", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_trim", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_model_number", DataTypes.StringType, true),

						DataTypes.createStructField("dealer_msrp", DataTypes.IntegerType, true),
						DataTypes.createStructField("dealer_invoice", DataTypes.IntegerType, true),
						DataTypes.createStructField("dealer_body", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_inventory_entry_date", DataTypes.DateType, true),

						DataTypes.createStructField("dealer_exterior_color_description", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_interior_color_description", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_exterior_color_code", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_interior_color_code", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_transmission_name", DataTypes.StringType, true),

						DataTypes.createStructField("dealer_installed_option_codes",
								DataTypes.createArrayType(DataTypes.StringType), true),
						DataTypes.createStructField("dealer_installed_option_descriptions",
								DataTypes.createArrayType(DataTypes.StringType), true),
						DataTypes.createStructField("dealer_additional_specs", DataTypes.StringType, true),

						DataTypes.createStructField("dealer_doors", DataTypes.StringType, true),
						DataTypes.createStructField("dealer_drive_type", DataTypes.StringType, true),

						DataTypes.createStructField("updated_at", DataTypes.TimestampType, false),
						DataTypes.createStructField("dealer_images", DataTypes.createArrayType(DataTypes.StringType),
								true),
						DataTypes.createStructField("dealer_certified", DataTypes.BooleanType, true),
						DataTypes.createStructField("dealer_transmission_type", DataTypes.StringType, true)

				));

		Dataset<Row> d = spark.createDataFrame(spark.emptyDataFrame().toJavaRDD(), schema);
		return d;

	}
}