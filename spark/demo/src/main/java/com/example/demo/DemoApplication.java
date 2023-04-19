package com.example.demo;

import com.example.demo.Model.UserModel;
import org.apache.spark.sql.*;
import org.springframework.boot.autoconfigure.security.SecurityProperties;

import java.util.Arrays;


public class DemoApplication {

    public static void main(String[] args) {
//		SparkSession sparkSession= SparkSession.builder()
//				.appName("just for practice")
//				.master("local")
//				.config("spark.driver.host","localhost")
//				.getOrCreate();
//		Dataset<Row> read= sparkSession.read()
//				.csv("/home/albanero/Downloads/spark/demo/src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
//				read.show(false);

//		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//        Dataset<Row> csv1 = spark.read().csv("/home/albanero/Downloads/spark/demo/src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");
//        csv1.show();



        //////reading in csv
//		SparkSession spark = SparkSession
//		.builder()
//		.appName("Simple Application")
//		.master("local")
//		.config("spark.driver.host", "localhost")
//		.getOrCreate();
//		Dataset<Row> csv1 = spark.read().csv("/home/albanero/Downloads/spark/demo/src/main/resources/csv.csv");
//		csv1.show();








        /////writing in csv
//		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//
//            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
//                    new User("vinayika", "1234567890", "abc", "vin@gmail", "mp","hyd"),
//                    new User("vinayika", "1234567890", "abc", "vin@gmail", "mp","hyd"),
//                    new User("vinayika", "1234567890", "abc", "vin@gmail", "mp","hyd")
//            ), User.class);
//
//            data.show();
//            data.write().format("csv").option("delimiter",",")
//                    .option("header", true).mode(SaveMode.Overwrite)
//               .save("/home/albanero/Downloads/spark/demo/src/main/resources");
//



//////updating in csv file
        SparkSession spark = SparkSession.builder().appName("Simple Application")
                .master("local").config("spark.driver.host", "localhost").getOrCreate();
                Dataset<Row> csv1 = spark.read().option("header",true).csv("/home/albanero/Downloads/spark/demo/src/main/resources/part-00000-4691f1e8-52c7-4d1e-ab22-8eb55e672f43-c000.csv");
                csv1.show();
                csv1 = csv1.withColumn("address", functions.when(csv1.col("address").equalTo("address"),"abc").otherwise(csv1.col("address")));
                csv1.show();
                csv1.write().csv("/home/albanero/Downloads/spark/demo/src/main/resources/abc");




    }

    SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();
          Dataset<Row> csv = spark.read().format("csv")
                    .option("header","true")
                  .option("delimiter","\t")
                    .load("/home/albanero/Downloads/spark/demo/src/main/resources/csv.csv");







    // Describe Schema...
//
//           csv.printSchema();
//
//
//
//    // Selecting Specific Column..
//
//            csv.select("Location").show();
//
////            //writing it into another csv file
//
//           csv.coalesce(1).write().option("header",true).mode(SaveMode.Append).csv("/home/albanero/Downloads/spark/demo/src/main/resources/csv.csv");
//
//
//    //filtering data
//            Dataset<Row> filteredData = csv.filter(new Column("Identifier").as("Id").gt(3));
//            filteredData.show();
//
//
//
//
//
//
//    // Show the first 3 records in the DataFrame
//            csv.show(3);
//
//
//
//
//// Register the DataFrame as a temporary view
//           csv.createOrReplaceTempView("User");
//
//// Group the data by the 'gender' column and calculate the average 'age'
//           Dataset<Row> grouped = csv.groupBy(col("gender")).agg(avg(col("age")));
//
//
//// Show the result
//           grouped.show();

////            List<PersonModel> people = Arrays.asList(
//                    new UserModel("Horvath", 30),
//                   new UserModel("Brad", 25),
//                    new UserModel("Jane", 35),
//                  new UserModel("Jay", 35));
//
//    // Convert the list to a Dataset
//            Dataset<UserModel> peopleDataset = spark.createDataset(user, Encoders.bean(UserModel.class));
//
//            peopleDataset.show();
//
//    //selecting columns
//                csv.show();
//          Dataset<Row> selectedCols = csv.select(col("Department,"));
//            selectedCols.show();
//

    //data from sql
//
//            String url = "jdbc:mysql://localhost:8081/hotel";
//            String driver = "com.mysql.jdbc.Driver";
//            String user = "root";
//            String password = "admin";

//            Dataset<Row> df =  spark.read()
//                .format("jdbc")
//                .option("driver", driver)
//                .option("url", url)
//                .option("user", user)
//                .option("password", password)
//                .option("dbtable", "students")
//                .load();
//
//            System.out.println("start");
////            df.count();
//            df.show(10);
//            System.out.println("end");
//
//            spark.stop();



  //  SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();
//
//            //reading
//            Dataset<Row> data = spark.createDataFrame(Arrays.asList(
//                    new UserModel("1", "vin", "ss", "vin@gmail", "1234"),
//                    new UserModel("2", "vin", "ss", "vin@gmail", "1234"),
//                    new UserModel("3", "vin", "ss", "vin@gmail", "1234"),
//                    new UserModel("1", "vin", "ss", "vin@gmail", "1234"),
//                    new UserModel("2", "vin", "ss", "vin@gmail", "1234"),
//                    new UserModel("3", "vin", "ss", "vin@gmail", "1234")
//            ), UserModel.class);
//            data.show();

    //writing
//            data.write().option("header",true).mode(SaveMode.Append).csv("/home/albanero/Downloads/spark/demo/src/main/resources/csv.csv");


    //appending column

//        data = data.withColumn("password",functions.abs(functions.rand()));
//        data.show();

    //drop column

//        data = data.drop("password");
//        data.show();
//        data.write().format("csv").option("header",true).mode(SaveMode.Overwrite).csv("/home/albanero/Downloads/spark/demo/src/main/resources/csv.csv");

    //changing the data

//        data = data.withColumn("id",functions.when(data.col("id").gt(2),100).otherwise(data.col("id")));
//        data.show();
//        data.write().format("csv").option("header",true).mode(SaveMode.Overwrite).csv("/home/albanero/Downloads/spark/demo/src/main/resources/csv.csv");

    //distinct values

//        data = data.distinct();
//        data.show();

    //union
//        Dataset<Row> temp = spark.createDataFrame(Arrays.asList(
//                new UserModel("1", "vin", "ss", "vin@gmail", "1234")
//        ), UserModel.class);
//
//        data = data.union(temp);
//        data.show();

    //group by
//        data.groupBy("name").count().show();

    //de duplication
//        Dataset<Row> uniqueData = data.dropDuplicates("id");
//        Dataset<Row> removedData = data.except(uniqueData);
//
//        data.show();
//        uniqueData.show();
//        removedData.show();





    //.....join.....
//    Dataset<Row> leftData = spark.createDataFrame(Arrays.asList(
////                    new UserModel("1", "vin", "ss", "vin@gmail", "1234"),
////                    new UserModel("2", "vin", "ss", "vin@gmail", "1234"),
////                    new UserModel("3", "vin", "ss", "vin@gmail", "1234"),
////                    new UserModel("1", "vin", "ss", "vin@gmail", "1234"),
////                    new UserModel("2", "vin", "ss", "vin@gil", "1234"),
//            new UserModel("3", "vin", "ss", "vin@gmail", "1234")
//    ), UserModel.class);
//    Dataset<Row> rightData = spark.createDataFrame(Arrays.asList(
//            new UserModel("1", "123456", "zre", "vin@gmail", "1234"),
//            new UserModel("2", "123457", "fdvd", "vin@gmail", "1234"),
//            new UserModel("3", "1221435", "fwf", "vin@gmail", "1234"),
//            new UserModel("1", "454356", "fdsf", "vin@gmail", "1234"),
//            new UserModel("2", "23435546", "fwf", "vin@gmail", "1234"),
//            new UserModel("3", "3453546", "fdss", "vin@gmail", "1234")
//    ), UserModel.class);


    // Inner join..
//    Dataset<Row> result = leftData.join(rightData, leftData.col("fname").equalTo(rightData.col("fname")),"inner");

    // Left Join...
//    Dataset<Row> result = leftData.join(rightData, leftData.col("fname").equalTo(rightData.col("fname")),"left");

    // Right Join...
//    Dataset<Row> result = leftData.join(rightData, leftData.col("fname").equalTo(rightData.col("fname")),"right");

//        result.show();

    //union
//        leftData.union(rightData).show();

///===========================================================================================================================

}
//        catch (Exception e){
//                e.printStackTrace();
//                }
//                }
//                }
//
//}
