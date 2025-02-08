# DataFrames Case : Without names just ID

from pyspark.sql import SparkSession                                                                                                                
from pyspark.sql import Row                                                                                                                         
from pyspark.sql.functions import desc, avg, count                                                                                                                      
def parseInput(line):                                                                                                                               
    fields = line.split()                                                                                                                                              
    return Row(movieID=int(fields[1]), rating=float(fields[2]))                                                                                                                    

if __name__ == "__main__":                                                                                                                                              

    #Create a SparkSession                                                                                                                          
    spark = SparkSession.builder.appName("PopularBadMovies").getOrCreate()                                                                                              
    sc = spark.sparkContext                                                                                                                                                    
    #Read ratings data                                                                                                                              
    lines = sc.textFile("hdfs:///user/maria_dev/movies_data_cmd/u.data")                                                                                                    

    #Get Data for RDD                                                                                                                               
    ratings = lines.map(parseInput)                                                                                                                 

    #Get Data for DataFrame                                                                                                                         
    ratingsDf = spark.createDataFrame(ratings)
                                                                                                 
    #Compute average per movieID                                                                                                                    
    averageRatings = ratingsDf.groupBy("movieID").agg(avg("rating").alias("avg_rating"))                                                                                                                                                                                 
    countsRatings = ratingsDf.groupBy("movieID").agg(count("rating").alias("count_rating"))                                                            
    averagesAndCounts = countsRatings.join(averageRatings, "movieID")
                                                                           
    badTen = averagesAndCounts.orderBy("avg_rating", desc("count_rating"))                                                                          
    badTen.show(10)