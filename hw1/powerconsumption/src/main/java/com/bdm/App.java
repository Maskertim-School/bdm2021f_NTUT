package com.bdm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.Level;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * The big data mining homework 1
 * Implementation of Spark
 */
public class App 
{
    public static final String DELIMITER = ";";
    private static Logger logger = Logger.getLogger(App.class.getName());
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        org.apache.log4j.Logger.getLogger("bdm").setLevel(org.apache.log4j.Level.ERROR);

        // Log the stdout of main program
        try{
            FileHandler fileHandler = new FileHandler("/tmp/App.log");
	        fileHandler.setLevel(Level.INFO); //Log的層級
	        logger.addHandler(fileHandler);
        }catch(Exception e){
            e.printStackTrace();
        }
        
        // Create Spark Context
        SparkConf conf = new SparkConf().setAppName("power_consumption");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load Datasets
        JavaRDD<String> power = sparkContext.textFile("/tmp/household_power_consumption.txt");
        String firstline = power.first();

        /* Transformations */
        // clean the dataset and retrive the expected column of data
        JavaRDD<Double> global_active_power = power
                    .filter(line->!line.equals(firstline))
                    .filter(x-> !x.contains("?"))
                    .map(x->Double.valueOf(x.split(DELIMITER)[2]));
        global_active_power.setName("global_active_power");

        JavaRDD<Double> global_reactive_power = power
                    .filter(line->!line.equals(firstline))
                    .filter(x-> !x.contains("?"))
                    .map(x->Double.valueOf(x.split(DELIMITER)[3]));
        global_reactive_power.setName("global_reactive_power");

        JavaRDD<Double> voltage = power
                    .filter(line->!line.equals(firstline))
                    .filter(x-> !x.contains("?"))
                    .map(x->Double.valueOf(x.split(DELIMITER)[4]));
        voltage.setName("voltage");
        
        JavaRDD<Double> global_intensity = power
                    .filter(line->!line.equals(firstline))
                    .filter(x-> !x.contains("?"))
                    .map(x->Double.valueOf(x.split(DELIMITER)[5]));
        global_intensity.setName("global_intensity");


        /* Calculate max, min, count, mean, stadard deviation, and min-max normaliation */
        // max value
        ArrayList<JavaRDD<Double>> all_global_rdds = new ArrayList<JavaRDD<Double>>();
        all_global_rdds.add(global_active_power);
        all_global_rdds.add(global_reactive_power);
        all_global_rdds.add(voltage);
        all_global_rdds.add(global_intensity);

        for(JavaRDD<Double> rdd : all_global_rdds){
            // max value
            double max = calculateMax(rdd);
            logger.info(rdd.name()+" max:"+max);
            // min value
            double min = calculateMin(rdd);
            logger.info(rdd.name()+" min:"+min);
            // row of data (i.e., count)
            long count = calculateCount(rdd);
            logger.info(rdd.name()+" count:"+count);
            // mean of data
            double mean = calculateMean(rdd);
            logger.info(rdd.name()+" mean:"+mean);
            // standard deviation of data
            double variation = rdd.map(x-> Math.pow((x-mean), 2))
                                .aggregate(0.0, (x, y)-> x+y, (x,y)-> x+y);
            double std = calculateSTD(rdd, variation);
            logger.info(rdd.name()+" std:"+std);
            // Z-Score (min-max normalization)
            JavaRDD<Double> norm = calculateNorm(rdd, mean, std);
            // RDD action 
            norm.collect();
            // write file
            try{
                norm.coalesce(1).saveAsTextFile("file:////tmp/"+rdd.name());
            }catch(Exception e){
                System.out.println("the file is existed, please remove the old file.");
            }
        }

        // if finish, just close the context
        sparkContext.close();
        
    }

    // calculate max per rdd
    public static double calculateMax(JavaRDD<Double> rdd){
        return rdd.max(Comparator.naturalOrder());
    }

    // calculate min per rdd
    public static double calculateMin(JavaRDD<Double> rdd){
        return rdd.min(Comparator.naturalOrder());
    }

    // calculate count per rdd
    public static long calculateCount(JavaRDD<Double> rdd){
        return rdd.count();
    }

    // calculate mean per rdd
    public static double calculateMean(JavaRDD<Double> rdd){
        return rdd.aggregate(0.0, (x, y)-> x+y, (x,y)-> x+y)/(double)rdd.count();
    }

    // calculate standard deviation per rdd
    public static double calculateSTD(JavaRDD<Double> rdd, double variation){
        return Math.sqrt(variation/(double)rdd.count());
    }

    // calculate Z-Score (min-max normalization)
    public static JavaRDD<Double> calculateNorm(JavaRDD<Double> rdd, double mean, double std){
        return rdd.map(x -> (x - mean) / std);
    }
}
