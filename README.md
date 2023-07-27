# Apache-Spark

This repo documents my understanding of Apache Spark and how it comes to rescue with big data where Pandas has limitations. The theories and my notes from the Udemy course <a href="https://www.udemy.com/course/spark-and-python-for-big-data-with-pyspark/?utm_source=adwords&utm_medium=udemyads&utm_campaign=Webindex_Catchall_la.EN_cc.CA&utm_term=_._ag_119831896715_._ad_533102824920_._kw__._de_c_._dm__._pl__._ti_dsa-392284169515_._li_9001494_._pd__._&matchtype=&gclid=Cj0KCQjwiIOmBhDjARIsAP6YhSUQcjYp_yVRKuBsf8-vQMvg9cYbeRWiB5dJchKx6zZMSHmkROT5zqMaAnzIEALw_wcB">Spark and Python for Big Data with PySpark</a> are detailed in the following. My solutions to all coding assignments are incorporated in the Jupyter Notebook files attached to this repo.

Here is the summary of my notes  The structure of my notes is as follows:

1. [Introduction](#1)
    1. [What is a Database?](#2)
    2. [Relational DBMS](#3)
    3. [Non-relational databases](#4)
    4. [Course structure](#5)

   
<a name="1"></a>
### Introduction 

course: 
Spark and Python for Big Data by Jose Portilla on udemy

https://spark.apache.org/docs/latest/


# Apache Spark vs. Pandas 

source: https://www.youtube.com/watch?v=XrpSRCwISdk

Apache Spark is an open-source distributed computing system designed for processing large datasets. It runs on distributed compute like YARN, Mesos, Standalone cluster.

PySpark is the Python API for Apache Spark.

Apache Spark has two main abstractions: 
+ RDD - distributed collection of objects
+ Dataframe - distributed dataset of tabular data.
  + Integrated SQL
  + ML Algorithms 

Gain and lose of using PySpark over Pandas:

Gain:
+ Ability to work with big data
+ Native SQL support 
+ Decent documentation

Lose:
+ Amazing documentation
+ easy plotting
+ indices

Pandas:
  + df = pd.read_csv (path_to_csv_file) 
  + df
  + df.head (10)
  + df.columns 
  + df.dtypes
  + df.columns = ['a', 'b'] 
  + df.rename(columns = {'old': 'new'}) 
  + df.drop ('mpg', axis = 1) 
  + df[df.mpg < 20] 
  + df['gpm'] = 1 / df.mpg
  + df.fillna(0)
  + df.groupby(['cyl', 'gear']).agg({'mpg': 'mean', 'disp': 'min'}) 
  + import numpy as np
    + df['logdisp'] = np.log(df.disp) 
  + left.merge(right, on = 'key')
  + left.merge(right, left_an = 'a', right_on = 'b') 
  + pd.pivot_table(df, values = 'D', index = ['A', 'B'], columns = [ 'C'] , aggfunc = np.sum)
  + df.describe()
  + df.hist()
  + no SQL support, there are third party libraries which support SQL like pandasql (open-source) and yhat (commercial) though 


  
PySpark:
  + df = spark.read.csv(path_to_csv_file, header=True, inferSchema=True)
  + df.show()
  + df.show (10)
  + df.columns 
  + df.dtypes
  + df.toDF('a', 'b') because Spark dataframes are immutable we cannot just make assignments and instead we have to actually craete a new dataframe with those names
  + df.withColumnRenamed('old', 'new') 
  + df.drop ('mpg') we don't have an axis concept here unlike in Pandas and we don't have index and so the only thing we can do is to drop columns 
  + df[df.mpg < 20] 
  + df.withColumn('gpm', 1 / df.mpg) again because of immutability we cannot make just assignment. Devision by zero in Pandas gives infinity but in Spark gives Null 
  + df.fillna(0) much less options compared to Pandas 
  + df.groupby(['cyl', 'gear']).agg({'mpg': 'mean', 'disp': 'min'}) 
  + import pyspark.sql.functions as F --> this keeps compute in the JVM and not running any python in the executor meaning it is faster 
    + df.withColumn ('logdisp', F.log(df.disp))
  + left.join(right, on = 'key')
  + left.join(right, left.a == right.b) 
  + df.groupBy("A", "B ").pivot("C").sum("D")
  + df.describe().show() (only count, mean, stddev, min, max, NO Quartiles) to get quartiles we need more code using built-in function called percentile_approx 
  + df.sample(False, 0.1).toPandas().hist()
  + great SQL support 
    + df.createOrReplaceTempView('foo')
    + df2 = spark.sql('select * from foo')
  
  PySpark best practices:
  + make sure to use pyspark.sql.functions and other built-in functions
  + use the same version of python and packages on cluster as driver 
  + learn about SSH port forwarding 
  + MLlib for ML at scale, which is equivalent to scikit-learn but in PySpark  
  + don't iterate through rows 
  + do df.limit(5).toPandas() NOT df.toPandas().head() 
     

# Course Notes

sources: Spark and Python for Big Data class by Jose Portilla on udemy

1. Spark runs programs up to 100x faster than Hadoop MapReduce in memory 

2. What is big datta?
Data that can fit on a local computer, in the scale of 0-32 GB depending on RAM, is not big data. 

3. But what can we do if we have a larger set of data?

    + **Try using a SQL database to move storage onto hard drive instead of RAM**
    
    + **Or use a distributed system, that distributes the data to multiple machines/computer. This is where Spark comes to play.**

If you're using spark, you're at a point where it no longer makes sense to fit all your data on RAM and it no longer makes sense to fit all your data into a single machine.

4. Local vs. distributed system: 

a local system is probably what you're used to. It's just a single machine, a single computer.It all shares the same ram, the same hard drive. A local process will use the computation resources of a single machine. 

In a distributed system, you have one main computer, some sort of master node, and you also have data and calculations distributed onto the other computers. A distributed process has access to the computational resources across a number of machines connected through a network. 

5. Scaling

+ After a certain point, it's much easier to scale out to many lower CPU machines in a distributed system, than try to scale up to a single machine with a high CPU
+ distributed machines also have the advantage of easily scaling. All you have to do is just add more machines versus a single computer.

No matter how nice it is, there's going to be a limit on how much ram or how much storage you can add to a single machine. So in distributed machines, you can just keep adding systems to the network and just get more power

6. Fault-tolerance

Distributed machines also include fault tolerance, which is really important when you're talking about large data sets. If one machine fails, the whole network can still go on, which you can't do on a local machine if your local machine crashes due to some error in the calculation you just lost all your calculation, all your data and fault tolerance is a fundamental idea where you're going to be replicating your data across multiple machines. So even if one goes down, your calculations and your data still persists and goes on.

7. Distributed architecture of Haddop

    + Hadoop is a way to distribute very large files across multiple machines.
    + It uses the Hadoop Distributed File System (HDFS)
    + HDFS allows a user to work with large data sets
    + HDFS also duplicates blocks of data for fault tolerance
    + It also then uses MapReduce
    + MapReduce allows computations on that data

So we kind of have two fundamental ideas here:

+ Hadoop is distributed file system, which is the way that we get a really large data set distributed across multiple machines.

+ And then we have the idea of MapReduce, which allows computations across the distributed data set.

8. Distributed storage - HDFS
    + HDFS will use blocks of data, with a size of 128 MB by default
    + Each of these blocks is replicated 3 times
    + The blocks are distributed in a way to support fault tolerance
    + Smaller blocks provide more parallelization during processing
    + Multiple copies of a block prevent loss of data due to a failure of a node


9. MapReduce

    + MapReduce is a way of splitting a **computation task** to a distributed set of files (such as HDFS)
    + It consists of a Job Tracker and multiple Task Trackers

10. What we covered can be thought of in two distinct parts:
    + Using HDFS to distribute large data sets
    + Using MapReduce to distribute a computational task to a distributed data set

Spark improves on the concepts of using distribution

11. Spark is a flexible alternative to MapReduce NOT to Hadoop

So don't think of spark in the concepts of Hadoop versus Spark, but instead  MapReduce versus Spark.

12. Spark vs MapReduce

    + MapReduce requires files to be stored in HDFS, Spark does not! Spark can use data stored in a variety of formats
    
            Cassandra

            AWS S3

            HDFS

            And more

    + Spark also can perform operations up to 100x faster than MapReduce
 
13. So how does spark achieve this speed?

    + MapReduce writes most data to disk after each map and reduce operation
    + Spark keeps most of the data in memory after each transformation
    + Spark can spill over to disk if the memory is filled

14. Spark RRD 

At the core of Spark is the idea of a Resilient Distributed Dataset (RDD)
Resilient Distributed Dataset (RDD) has 4 main features:

+ Distributed Collection of Data
+ Fault-tolerant
+ Parallel operation - partioned
+ Ability to use many data sources

RDDs are **immutable, lazily evaluated, and cacheable**

There are two types of Spark operations:

+ Transformations: Transformations are basically a recipe to follow.
+ Actions: Actions actually perform what the recipe says to do and returns something back.

15. Transformations vs actions behavior 

The behavior of transformations versus actions also carries over to the syntax when coding. A lot of times when you write a method call off of a data frame which we're going to be working with with pyspark, you won't see anything as a result until you call an action something like show. And this makes sense because if you're working with a really large data set, you don't want to constantly be calculating all the transformations. Maybe a transformation can be something like take the average or take the count of a particular data, or show me where column X is greater than the number two, etc. like that. But you don't want to actually calculate that every time until you're sure you want to perform it because it's such a huge data set. It's quite a task to calculate everything every time you type something. So that's why everything is separated between transformations and then those calls to action.

16. Spark DataFrames are also now the standard way of using Spark’s Machine Learning Capabilities.

17. why Linux? 

Realistically Spark won't be running on a single machine. That's basically the whole point of spark. Your data is so large that it no longer fits on a single machine and you're going to need to run it on a cluster on a service like Google Cloud or Amazon Web Services. **And these cluster services will pretty much always be on a Linux based system.** They're not running Mac OS or Windows.

Employers and the real world is really focused on Linux when it comes to spark, which makes sense because if you're running it on a cluster, it's going to be on a Linux based system.

18. findspark library

I need to install find Spark library to not be needing to change directory every time and instead, being able to import Spark from any directory so I don't have to worry about changing directory to that spark home directory.

**pip3 install findspark **

then 

    import findspark
    findspark.init('/home/danial/spark-3.3.2-bin-hadoop3')
    import pyspark 

19. Intro to Spark DF 

Spark in its early days began with something known as the **RDD syntax**, which was a little ugly and a bit tricky to learn. Fortunately, now, Spark 2.0 and higher has shifted towards a **data frame syntax**, which is much cleaner and easier to work with. And this **data frame syntax looks really similar across all the APIs**, which is nice. Meaning if you've already done a course in something like Scala and Spark Learning Python and Spark data frames is really easy. A lot of that stuff looks extremely similar.


20. How to define Schema

Often if you're not dealing with data that's really nice, or maybe from a particular source, you need to actually clarify what the schema is. So in order to do certain operations, the schema has to be correct. It has to know what columns are strings, what columns are integers, etc..

        from pyspark.sql.types import StructField,StringType,IntegerType,StructType
        
        data_schema = [StructField("age", IntegerType(), True),StructField("name", StringType(), True)]

        final_struc = StructType(fields=data_schema)

        df = spark.read.json('people.json', schema=final_struc)
        
21. How to grab data from a spark DataFrame
**df['age']** gives me back the column object but if I actually want to get a data frame with that singular column so that I can see the results I use the select method:

**df.select('age').show()**

So the main differences between these two methods is the fact that one of them, the first one, is returning back a column, while the second one is returning a data frame that contains a single column, so we have a lot more methods and attributes we can call off of that.

You get a lot more flexibility with a data frame of a single column versus just a column So a lot of times we use select instead of just grabbing that column object.

df.head(2) gives me back a list of row object.

And the reason there are so many specialized objects, such as a column object or a row object, is because of **Spark's ability to read from a distributed data source and then map that out to distributed computing.**

to select multiple columns:

**df.select(['age', 'name'])**

22. Adding new column using **withColumn method**

withColumn method basically returns a new dataframe by adding in a column or replacing an existing column. This is not an in place operation and we would have to save this to a new dataframe.

23. Renaming a column using withColumnRenamed method

**df.withColumnRenamed('old_col_name', 'new_col_name')**

24. Using pure SQL to directly deal and interact with the spark data frame

First I need to register the DataFrame as a SQL temporary view then I can pass in direct SQL queries:

**df.crerateOrReplaceTempView('people')**

**results = spark.sql("SELECT * FROM people WHERE age=30")**

**results.show()**

What's really awesome is if you already have a lot of SQL knowledge, you can leverage that with spark SQL and you can do complicated operations really quickly in case you happen to forget some of the more basic spark data frame operations.

25. How to filter data when you grabed it

A large part of working with data frames is the ability to quickly filter out data based on conditions. Spark data frames are built on top of that Spark SQL platform, which means, as we previously discussed, if you already know SQL, you could quickly and easily grab that data using SQL commands. However, we're really going to be using the data frame methods as our focus for the course. but here is what it looks like using SQL:

**df.filter("Close < 500").select(["Open", "Close"]).show()**

but the above operation using normal python comparison operations is like:

**df.filter(df["Close"] < 500).select(["Open", "Close"]).show()**

The key things to keep in mind here is that I'm using dot filter and then passing in the column, some comparison operator and then the value.

**filtering based on multiple conditions**:

**df.filter((df["Close"] < 500) & ~(df["Open"] > 200)).show()**

26. collect method

And when you're working in the real world with data, a lot of times you're going to want to collect stuff so you can actually work with that variable later on. Often in this course, we'll just using we'll just be using **show** so you can actually see stuff, but we don't really need to collect it. But **in real life you'll probably be collecting more often than showing.**

**result = df.filter(df["Low"] == 197.16).collect()** 

**row = result[0]**

**row.asDict()**

27. Groupby and Aggregate Operations 

        import findspark
        findspark.init("home/danial/spark-3.3.2-bin-hadoop3")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("aggs").getOrCreate()
        df = spark.read.csv(path, inferSchema=True, heade=True)

        df.groupBy("Company").mean().show()

    We can import functions from pyspark.sql.functions, then what I can do with these functions is combine them with a **select call**:

        from pyspark.sql.functions import countDistinct, avg, stddev
        df.select(avg("Sales")).show()
        df.select(avg("Sales").alias("Average Sales")).show()

        from pyspark.sql.functions import format_number

        df.select(stddev("Sales").alias("std")).select(format_number("std", 2).alias("std")).show() 
        
        df.orderBy("Sales").show() # in ascending order 
        df.orderBy(df["Sales"].desc()).show() # in descending order 
        
28. Missing values 

        import findspark
        findspark.init("home/danial/spark-3.3.2-bin-hadoop3")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("miss").getOrCreate()
        df = spark.read.csv(path, inferSchema=True, heade=True)

        df.na.drop().show()
        df.na.drop(how='any').show() # how is defaulted to any 
        df.na.drop(how='all').show()

        df.na.drop(thresh=2).show()
        df.na.drop(subset=['Sales']).show()

       the best practice is to make it clear which column you want to fill null in (spark is smart enought to figure it out on her/his own though):
       
        df.na.fill('No Name', subset=['Name']).show() 

        from pyspark.sql.functions import mean 
        mean_val = df.select(mean(df['Sales'])).collect()
        mean_sales = mean_val[0][0]
        df.na.fill(mean_sales, ['Sales']).show()
            
29. Dates and Timestamps

Basically whenever I want to use any pyspark.sql.functions, first I need to do **df.select** then I just call the function on whatever column I want so I have to pass in the actual column meaning I have to use **bracket notation** like:

    from pyspark.sql.functions import dayofmonth, year, format_number 
    df.select(dayofmonth(df["Date"])).show()
    
    
    newdf  = df.withColumn("Year", year(df["Date"]))
    result = newdf.groupBy("Year").mean().select(["Year", "avg(Close)"]) 
    result.select(["Year", format_number("avg(Close)", 2).alias("Average Closing Price")]).show()


30. Intro to ML with MLlib 

So one of the main quirks when dealing with the MLlib is that you need to format your data. So eventually just has one or two columns. And if you're using a supervised learning algorithm, the two columns are going to be features and labels, for unsupervised it's just a features column.

Basically what that means is if you have a data set with a ton of feature columns, you eventually need to **condense those all down to just a singular column** where each entry in that singular column, so the rows, is actually just an array consisting of all those old entries.

So overall, this requires a little more data processing work than some other machine learning libraries. But the big upside and the whole reason for all this data processing work is that **that exact same syntax will work with distributed data**. So if you have a huge data set, you don't need to learn a new syntax for it. So that's no small feat for what's actually going on under the hood with Python and spark. It just requires you to put in a little more work with data processing using different **vector indexers**.

31. Linear regression using MLlib

        import findspark
        findspark.init("home/danial/spark-3.3.2-bin-hadoop3")
        
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("lrm").getOrCreate()
        
        
        from pyspark.ml.regression import LinearRegression 
        path = 'path_to_data'
        data = spark.read.format('libsvm').load(path)
        
        train_set, test_set = data.randomSplit([0.7, 0.3]) 
        
        lr = LinearRegression(featuresCol='features', labelCol='label', predictionCol='predictions')
        lrModel = lr.fit(train_set)
        
        lrModel.coefficients
        lrModel.intercept
        
        summary = lrModel.summary
        summary.r2
        summary.rootMeanSquaredError 
        
        test_results = lrModel.evaluate(test_set)
        
        test_results.residuals.show()
        test_results.rootMeanSquaredError
        
        # pretending to have unlabelled data for model deployment 
        
        unlabelled_data = test_set.select('features')
        predictions = lrModel.transform(unlabelled_data)
        predictions.show()

32. Data transformation to prepare features for MLlib

Usually what we end up doing is combining the various feature columns in a realistic data set into a single features column using the data transformations:

    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler


    assembler = VectorAssembler(inputCols=numerical_features, outputCol='features')
    output = assembler.transform(df)
    output.printSchema()
    final_data = output.select('features', 'Yearly Amount Spent')
    
    train_set, test_set = final_data.randomSplit([0.7, 0.3]) # I need to transform all the data before splitting it into training/testing.

    lr = LinearRegression(labelCol='Yearly Amount Spent')
    lr_model = lr.fit(train_set)
    test_results = lr_model.evaluate(test_set)

    test_results.residuals.show()
    test_results.rootMeanSquaredError
    test_results.r2
    final_data.describe().show()
    
    unlabelled_data = test_set.select('features') # let's pretend we have unlabelled data on which I would like to deploy my model
    predictions = lr_model.transform(unlabelled_data)
    predictions.show()
    
33. How to convert categorical columns into numerical usint **StringIndexer**

In Apache Spark, StringIndexer is a **feature transformer that converts a categorical column of strings into a column of numerical indices.** It assigns a unique numerical value to each distinct string in the column, based on the frequency of occurrence. The most frequent string is assigned an index of 0, the second most frequent string is assigned an index of 1, and so on.

The StringIndexer takes the following arguments:

inputCol: The name of the input column of string type to be indexed.
outputCol: The name of the output column of numerical indices.

example:

    from pyspark.ml.feature import StringIndexer
    indexer = StringIndexer(inputCol='Cruise_line', outputCol='Cruise_line_Index')
    indexed = indexer.fit(data).transform(data)

    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler

    num_col = ['Age',
             'Tonnage',
             'passengers',
             'length',
             'cabins',
             'passenger_density',
             'Cruise_line_Index']
    assembler = VectorAssembler(inputCols=num_col, outputCol='features')
    output = assembler.transform(indexed)

    final_data = output.select('features', 'crew')
    train_set, test_set = final_data.randomSplit([0.7, 0.3])
    lr = LinearRegression(labelCol='crew')
    lr_model = lr.fit(train_set)
    test_results = lr_model.evaluate(test_set)
    test_results.residuals.show()
    test_results.r2
    test_results.rootMeanSquaredError
    test_results.meanAbsoluteErrortest_results.meanSquaredError

    from pyspark.sql.functions import corr
    data.select(corr('crew','passengers')).show()
    data.select(corr('crew','cabins')).show()

34. **StringIndexer of spark vs. LabelEncoder and OneHotEncoder of scikit-learn**

StringIndexer is similar to LabelEncoder in scikit-learn, in that it maps categorical values to numerical labels. However, unlike one-hot encoding, StringIndexer does not create new columns for each distinct value. Instead, it creates a **single column of numerical labels.**

In contrast, one-hot encoding creates a binary vector for each distinct value in the categorical column, where each element in the vector represents the presence or absence of that value in the original column. This can create a sparse matrix with many zero elements, which can be useful in some machine learning algorithms.

So, if you want to use one-hot encoding in Spark, you can use the **OneHotEncoder transformer**. However, you should note that unlike scikit-learn's LabelEncoder, which assigns numerical labels based on **the alphabetical order of the distinct values**, StringIndexer in Spark assigns numerical labels based on **the frequency of occurrence of each distinct value in the column**. This can be useful in some cases where you want to assign **higher importance to more frequent values**, but you should be aware of this difference if you are using Spark's StringIndexer instead of scikit-learn's LabelEncoder

35. Classification -- Evaluators 

**Evaluators are very important part of our pipeline when working with Machine Learning**:

https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.BinaryClassificationEvaluator.html#pyspark.ml.evaluation.BinaryClassificationEvaluator.metricName

https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.MulticlassClassificationEvaluator.html


Evaluators behave similar to machine learning algorithm objects, but are designed to take an evaluation data frames. That is, the data frames that are produced when we run things like model.evaluate on some test data set that produces an evaluation data frame that has the prediction column and this evaluator object is going to be able to take in that object and we can call metrics off of it. Here is an example:

    from pyspark.ml.classification import LogisticRegression
    train_set, test_set = data.randomSplit([0.7, 0.3])
    lg = LogisticRegression()
    lg_model = lg.fit(train_set)

    predictions_and_labels = lg_model.evaluate(test_set)
    predictions_and_labels.predictions.show()

    from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                        MulticlassClassificationEvaluator)
    my_eval = BinaryClassificationEvaluator()

    my_final_roc = my_eval.evaluate(predictions_and_labels.predictions) # areaUnderROC
    my_final_roc


36. Evaluators in Spark 

Spark evaluators are not only for classification. Spark evaluators are used for evaluating the performance of various machine learning models, including classification, regression, and clustering models.

In classification, evaluators such as BinaryClassificationEvaluator and MulticlassClassificationEvaluator are used to evaluate the performance of binary and multi-class classification models, respectively.

In regression, evaluators such as RegressionEvaluator are used to evaluate the performance of regression models.

In clustering, evaluators such as ClusteringEvaluator are used to evaluate the performance of clustering models.

Apart from machine learning models, Spark evaluators can also be used for evaluating the performance of Spark SQL queries, Streaming queries, and GraphX programs.

Overall, Spark evaluators are a versatile tool that can be used for evaluating the performance of a wide range of Spark-based applications, including machine learning models, SQL queries, and streaming applications.

37. Pipeline in Spark

In Apache Spark, a pipeline is a sequence of **stages** that are executed in a **specific order** to perform a data processing task. A pipeline typically consists of data preparation, feature engineering, model training, and evaluation stages.

Spark's pipeline API is designed to simplify the process of building machine learning pipelines by providing a high-level API for constructing and executing pipelines. The pipeline API is built on top of Spark's DataFrame API, which provides a powerful and flexible way to manipulate structured and semi-structured data.

To build a pipeline in Spark, you typically follow these steps:

+ Define the stages of the pipeline: Each stage represents a specific data processing task, such as data cleaning, feature extraction, or model training.

+ Construct the pipeline: Use the Pipeline constructor to create a new pipeline object, passing in the stages as a list.

+ Fit the pipeline: Use the fit method of the pipeline object to fit the pipeline to the training data.

+ Apply the pipeline: Use the transform method of the pipeline object to apply the pipeline to the test data.

Here is an example:

    my_final_data.printSchema()

    root
     |-- Survived: integer (nullable = true)
     |-- Pclass: integer (nullable = true)
     |-- Sex: string (nullable = true)
     |-- Age: double (nullable = true)
     |-- SibSp: integer (nullable = true)
     |-- Parch: integer (nullable = true)
     |-- Fare: double (nullable = true)
     |-- Embarked: string (nullable = true)

    from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    gender_indexer = StringIndexer(inputCol='Sex', outputCol='SexIndex')
    gender_encoder = OneHotEncoder(inputCol='SexIndex', outputCol='SexVec')

    embark_indexer = StringIndexer(inputCol='Embarked', outputCol='EmbarkedIndex')
    embark_encoder = OneHotEncoder(inputCol='EmbarkedIndex', outputCol='EmbarkedVec')

    assembler = VectorAssembler(inputCols=['Pclass', 'SexVec', 'Age', 'SibSp',
                                           'Parch', 'Fare', 'EmbarkedVec'], outputCol='features')

    log_reg_titanic = LogisticRegression(featuresCol='features', labelCol='Survived')

    pipeline = Pipeline(stages =[gender_indexer, embark_indexer,
                                 gender_encoder, embark_encoder,
                                 assembler, log_reg_titanic])

    train_data, test_data = my_final_data.randomSplit([0.7, 0.3])

    fit_model = pipeline.fit(train_data)
    results = fit_model.transform(test_data)

    my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='Survived')
    results.select('Survived', 'prediction').show()

    AUC =  my_eval.evaluate(results)
    AUC
    
38. An example of logistic regression

Here is the code: 

    from pyspark.sql import SparkSession
    
    from pyspark.ml.feature import VectorAssembler
    
    from pyspark.ml.classification import LogisticRegression
    
    from pyspark.ml.evaluation import BinaryClassificationEvaluator

    df.columns
    ['Names',
     'Age',
     'Total_Purchase',
     'Account_Manager',
     'Years',
     'Num_Sites',
     'Onboard_date',
     'Location',
     'Company',
     'Churn']

    assembler = VectorAssembler(inputCols=['Age',
                                         'Total_Purchase',
                                         'Account_Manager',
                                         'Years',
                                         'Num_Sites'], outputCol='features')
    output = assembler.transform(df)
    final_data = output.select(['features', 'Churn'])
    train_set, test_set = final_data.randomSplit([0.7, 0.3])
    churn_model = LogisticRegression(featuresCol='features', labelCol='Churn')
    fit_churn_model = churn_model.fit(train_set)
    training_sum = fit_churn_model.summary
    training_sum.predictions.describe().show()
    pred_and_labels = fit_churn_model.evaluate(test_set)
    pred_and_labels.predictions.show()
    my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='Churn')
    AUC = my_eval.evaluate(pred_and_labels.predictions)
    AUC

predict on new data

    final_churn_model = churn_model.fit(final_data)

    new_data = spark.read.csv(path_new_data, header=True, inferSchema=True)
    new_data.printSchema()
    root
     |-- Names: string (nullable = true)
     |-- Age: double (nullable = true)
     |-- Total_Purchase: double (nullable = true)
     |-- Account_Manager: integer (nullable = true)
     |-- Years: double (nullable = true)
     |-- Num_Sites: double (nullable = true)
     |-- Onboard_date: timestamp (nullable = true)
     |-- Location: string (nullable = true)
     |-- Company: string (nullable = true)

    test_new_data = assembler.transform(new_data)
    test_new_data.printSchema()

    root
     |-- Names: string (nullable = true)
     |-- Age: double (nullable = true)
     |-- Total_Purchase: double (nullable = true)
     |-- Account_Manager: integer (nullable = true)
     |-- Years: double (nullable = true)
     |-- Num_Sites: double (nullable = true)
     |-- Onboard_date: timestamp (nullable = true)
     |-- Location: string (nullable = true)
     |-- Company: string (nullable = true)
     |-- features: vector (nullable = true)

    final_results = final_churn_model.transform(test_new_data)
    final_results.select('Company', 'prediction').show()

39. Decision Trees and RF

Here is an example using the data from the documentation: 

        import findspark
        findspark.init('/home/danial/spark-3.3.2-bin-hadoop3')
        from pyspark.sql import SparkSession
        from pyspark.ml import Pipeline
        from pyspark.ml.classification import (RandomForestClassifier,
                                               GBTClassifier,
                                               DecisionTreeClassifier)
        spark = SparkSession.builder.appName('trees').getOrCreate()
        path = '/home/danial/Desktop/myspark/Apache-Spark/Python-and-Spark-for-Big-Data-master/Spark_for_Machine_Learning/Tree_Methods/'
        data = spark.read.format('libsvm').load(path + 'sample_libsvm_data.txt')

        data.printSchema()
        root
         |-- label: double (nullable = true)
         |-- features: vector (nullable = true)

        train_data, test_data = data.randomSplit([0.7, 0.3])
        train_data.describe().show()
        +-------+------------------+
        |summary|             label|
        +-------+------------------+
        |  count|                77|
        |   mean|0.5454545454545454|
        | stddev|0.5011947448335864|
        |    min|               0.0|
        |    max|               1.0|
        +-------+------------------+

        test_data.describe().show()
        +-------+-------------------+
        |summary|              label|
        +-------+-------------------+
        |  count|                 23|
        |   mean| 0.6521739130434783|
        | stddev|0.48698475355767396|
        |    min|                0.0|
        |    max|                1.0|
        +-------+-------------------+

        dtc = DecisionTreeClassifier()
        rfc = RandomForestClassifier(numTrees=100)
        gbt = GBTClassifier()

        dtc_model = dtc.fit(train_data)
        rfc_model = rfc.fit(train_data)
        gbt_model = gbt.fit(train_data)

        dtc_preds = dtc_model.transform(test_data)
        rfc_preds = rfc_model.transform(test_data)
        gbt_preds = gbt_model.transform(test_data)

        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        
        acc_eval = MulticlassClassificationEvaluator(metricName='accuracy')
        
        print ('DTC Accuracy:')
        acc_eval.evaluate(dtc_preds)
        DTC Accuracy:
        1.0
        
        print ('RFC Accuracy:')
        acc_eval.evaluate(rfc_preds)
        RFC Accuracy:
        1.0
        
        print ('GBT Accuracy:')
        acc_eval.evaluate(gbt_preds)
        GBT Accuracy:
        1.0

        rfc_model.featureImportances
        
40. Here is another example (goal is to build a model to predict whether or not a university is Private)

        data.printSchema()
        root
         |-- School: string (nullable = true)
         |-- Private: string (nullable = true)
         |-- Apps: integer (nullable = true)
         |-- Accept: integer (nullable = true)
         |-- Enroll: integer (nullable = true)
         |-- Top10perc: integer (nullable = true)
         |-- Top25perc: integer (nullable = true)
         |-- F_Undergrad: integer (nullable = true)
         |-- P_Undergrad: integer (nullable = true)
         |-- Outstate: integer (nullable = true)
         |-- Room_Board: integer (nullable = true)
         |-- Books: integer (nullable = true)
         |-- Personal: integer (nullable = true)
         |-- PhD: integer (nullable = true)
         |-- Terminal: integer (nullable = true)
         |-- S_F_Ratio: double (nullable = true)
         |-- perc_alumni: integer (nullable = true)
         |-- Expend: integer (nullable = true)
         |-- Grad_Rate: integer (nullable = true)

        data.head(1)
        [Row(School='Abilene Christian University', Private='Yes', Apps=1660, Accept=1232, Enroll=721, Top10perc=23, Top25perc=52, F_Undergrad=2885, P_Undergrad=537, Outstate=7440, Room_Board=3300, Books=450, Personal=2200, PhD=70, Terminal=78, S_F_Ratio=18.1, perc_alumni=12, Expend=7041, Grad_Rate=60)]

        data.count()
        777
        from pyspark.ml.feature import VectorAssembler
        data.columns
        ['School',
         'Private',
         'Apps',
         'Accept',
         'Enroll',
         'Top10perc',
         'Top25perc',
         'F_Undergrad',
         'P_Undergrad',
         'Outstate',
         'Room_Board',
         'Books',
         'Personal',
         'PhD',
         'Terminal',
         'S_F_Ratio',
         'perc_alumni',
         'Expend',
         'Grad_Rate']

        assembler = VectorAssembler(inputCols=['Apps',
                                             'Accept',
                                             'Enroll',
                                             'Top10perc',
                                             'Top25perc',
                                             'F_Undergrad',
                                             'P_Undergrad',
                                             'Outstate',
                                             'Room_Board',
                                             'Books',
                                             'Personal',
                                             'PhD',
                                             'Terminal',
                                             'S_F_Ratio',
                                             'perc_alumni',
                                             'Expend',
                                             'Grad_Rate'], outputCol='features')
        output = assembler.transform(data)
        from pyspark.ml.feature import StringIndexer
        indexer = StringIndexer(inputCol='Private', outputCol='PrivateIndex')
        indexed = indexer.fit(output).transform(output)
        indexed.printSchema()

        root
         |-- School: string (nullable = true)
         |-- Private: string (nullable = true)
         |-- Apps: integer (nullable = true)
         |-- Accept: integer (nullable = true)
         |-- Enroll: integer (nullable = true)
         |-- Top10perc: integer (nullable = true)
         |-- Top25perc: integer (nullable = true)
         |-- F_Undergrad: integer (nullable = true)
         |-- P_Undergrad: integer (nullable = true)
         |-- Outstate: integer (nullable = true)
         |-- Room_Board: integer (nullable = true)
         |-- Books: integer (nullable = true)
         |-- Personal: integer (nullable = true)
         |-- PhD: integer (nullable = true)
         |-- Terminal: integer (nullable = true)
         |-- S_F_Ratio: double (nullable = true)
         |-- perc_alumni: integer (nullable = true)
         |-- Expend: integer (nullable = true)
         |-- Grad_Rate: integer (nullable = true)
         |-- features: vector (nullable = true)
         |-- PrivateIndex: double (nullable = false)

        final_data = indexed.select(['PrivateIndex', 'features'])
        final_data.show()

        +------------+--------------------+
        |PrivateIndex|            features|
        +------------+--------------------+
        |         0.0|[1660.0,1232.0,72...|
        |         0.0|[2186.0,1924.0,51...|
        |         0.0|[1428.0,1097.0,33...|
        |         0.0|[417.0,349.0,137....|
        |         0.0|[193.0,146.0,55.0...|
        |         0.0|[587.0,479.0,158....|
        |         0.0|[353.0,340.0,103....|
        |         0.0|[1899.0,1720.0,48...|
        |         0.0|[1038.0,839.0,227...|
        |         0.0|[582.0,498.0,172....|
        |         0.0|[1732.0,1425.0,47...|
        |         0.0|[2652.0,1900.0,48...|
        |         0.0|[1179.0,780.0,290...|
        |         0.0|[1267.0,1080.0,38...|
        |         0.0|[494.0,313.0,157....|
        |         0.0|[1420.0,1093.0,22...|
        |         0.0|[4302.0,992.0,418...|
        |         0.0|[1216.0,908.0,423...|
        |         0.0|[1130.0,704.0,322...|
        |         1.0|[3540.0,2001.0,10...|
        +------------+--------------------+
        only showing top 20 rows

        train_set, test_set = final_data.randomSplit([0.7, 0.3])
        
        from pyspark.ml.classification import (DecisionTreeClassifier, 
                                               RandomForestClassifier, 
                                               GBTClassifier)
                                               
        dtc = DecisionTreeClassifier(labelCol='PrivateIndex')
        rfc = RandomForestClassifier(labelCol='PrivateIndex')
        gbt = GBTClassifier(labelCol='PrivateIndex')
        
        dtc_model = dtc.fit(train_set)
        rfc_model = rfc.fit(train_set)
        gbt_model = gbt.fit(train_set)
        
        dtc_preds = dtc_model.transform(test_set)
        rfc_preds = rfc_model.transform(test_set)
        gbt_preds = gbt_model.transform(test_set)

        from pyspark.ml.evaluation import BinaryClassificationEvaluator
 
        bin_eval = BinaryClassificationEvaluator(labelCol='PrivateIndex')
        
        print (f'DTC AUC = {bin_eval.evaluate(dtc_preds)}')
        DTC AUC = 0.9175424767910318
        
        print (f'RFC AUC = {bin_eval.evaluate(rfc_preds)}')
        RFC AUC = 0.9647486424943077
        
        print (f'GBT AUC = {bin_eval.evaluate(gbt_preds)}')
        GBT AUC = 0.9401383779996496

        rfc_new = RandomForestClassifier(labelCol='PrivateIndex', numTrees=150)
        rfc_new_model = rfc_new.fit(train_set)
        rfc_new_model_preds = rfc_new_model.transform(test_set)

        print ('RFC (num of trees = 150) AUC')
        bin_eval.evaluate(rfc_new_model_preds)
        RFC (num of trees = 150) AUC
        0.9655806621124543
        
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        
        acc_eval = MulticlassClassificationEvaluator(labelCol='PrivateIndex', metricName='accuracy')
        
        dtc_acc = acc_eval.evaluate(dtc_preds)
        print (f'DTC accuracy = {dtc_acc}')
        DTC accuracy = 0.899581589958159
        
        rfc_acc = acc_eval.evaluate(rfc_preds)
        print (f'RFC accuracy = {rfc_acc}')
        RFC accuracy = 0.9121338912133892
        
        gbt_acc = acc_eval.evaluate(gbt_preds)
        print (f'GBT accuracy = {gbt_acc}')
        GBT accuracy = 0.9079497907949791

41. Using ML for a different purpose - to get the feature importance

        spark = SparkSession.builder.appName('consulting').getOrCreate()
        data = spark.read.csv(path, header=True, inferSchema=True)
        data.head(1)
        [Row(A=4, B=2, C=12.0, D=3, Spoiled=1.0)]

        data.printSchema()
        root
         |-- A: integer (nullable = true)
         |-- B: integer (nullable = true)
         |-- C: double (nullable = true)
         |-- D: integer (nullable = true)
         |-- Spoiled: double (nullable = true)

        data.select('Spoiled').distinct().show()
        +-------+
        |Spoiled|
        +-------+
        |    0.0|
        |    1.0|
        +-------+

        from pyspark.ml.feature import VectorAssembler
        data.columns
        ['A', 'B', 'C', 'D', 'Spoiled']
        assembler = VectorAssembler(inputCols=['A', 'B', 'C', 'D'], outputCol='features')
        output = assembler.transform(data)
        output.printSchema()

        root
         |-- A: integer (nullable = true)
         |-- B: integer (nullable = true)
         |-- C: double (nullable = true)
         |-- D: integer (nullable = true)
         |-- Spoiled: double (nullable = true)
         |-- features: vector (nullable = true)

        final_data = output.select(['features', 'Spoiled', ])

        final_data.show()
        +-------------------+-------+
        |           features|Spoiled|
        +-------------------+-------+
        | [4.0,2.0,12.0,3.0]|    1.0|
        | [5.0,6.0,12.0,7.0]|    1.0|
        | [6.0,2.0,13.0,6.0]|    1.0|
        | [4.0,2.0,12.0,1.0]|    1.0|
        | [4.0,2.0,12.0,3.0]|    1.0|
        |[10.0,3.0,13.0,9.0]|    1.0|
        | [8.0,5.0,14.0,5.0]|    1.0|
        | [5.0,8.0,12.0,8.0]|    1.0|
        | [6.0,5.0,12.0,9.0]|    1.0|
        | [3.0,3.0,12.0,1.0]|    1.0|
        | [9.0,8.0,11.0,3.0]|    1.0|
        |[1.0,10.0,12.0,3.0]|    1.0|
        |[1.0,5.0,13.0,10.0]|    1.0|
        |[2.0,10.0,12.0,6.0]|    1.0|
        |[1.0,10.0,11.0,4.0]|    1.0|
        | [5.0,3.0,12.0,2.0]|    1.0|
        | [4.0,9.0,11.0,8.0]|    1.0|
        | [5.0,1.0,11.0,1.0]|    1.0|
        |[4.0,9.0,12.0,10.0]|    1.0|
        | [5.0,8.0,10.0,9.0]|    1.0|
        +-------------------+-------+
        only showing top 20 rows

        from pyspark.ml.classification import RandomForestClassifier

        rfc = RandomForestClassifier(featuresCol='features', labelCol='Spoiled')
        rfc_model = rfc.fit(final_data)
        
        rfc_model.featureImportances
        SparseVector(4, {0: 0.036, 1: 0.0285, 2: 0.9095, 3: 0.026})
        
        data.head(1)
        [Row(A=4, B=2, C=12.0, D=3, Spoiled=1.0)]
        
        final_data.head(1)
        [Row(features=DenseVector([4.0, 2.0, 12.0, 3.0]), Spoiled=1.0)]
 
So chemical C is the cause of early spoiling!

42. Unsupervised learning 

Often we try to create groups from data instead of trying to predict classes or continuous values. This sort of problem is known as clustering. You can think of it almost as an attempt to create labels. You input some unlabeled data, and the unsupervised learning algorithm returns back possible clusters of the data.

This means you have data that only contains features and you want to see if there are patterns in the data that would allow you to create groupings or clusters. This is a key distinction from our previous supervised learning tasks where we had historical labeled data.

By the nature of this problem, it can be difficult to evaluate the groups or clusters for correctness. A large part of being able to interpret the clusters assigned comes down to **domain knowledge.**

A lot of clustering problems have no 100% correct approach or answer, and that's just the nature of unsupervised learning in general, since you never had those labels to check against.

42. K-means Clustering 

The K means algorithm does the following steps.

+ You as the user have to choose a number of clusters K So you have to choose that **before you actually initiate the model.** That means you have to use **domain knowledge to choose a reasonable K value.**

+ Then it's going to randomly assign each point to a cluster until clusters stop changing, it's going to repeat the following for each cluster.

    + It's going to compute the cluster centroid by taking the mean vector of points in the cluster.

    + Then it will assign each data point to the cluster for which the centroid is the closest.
    
    
Choosing a K value can be a pretty difficult decision or maybe a really easy decision. It really depends on the domain knowledge and what the data looks like.

43. Elbow technique

There is no easy answer for choosing a “best” K value. One mathematical method is the elbow method. First of all, we compute the sum of squared error (SSE) for some values of k (for example 2, 4, 6, 8, etc.). The SSE is defined as the sum of the squared distance between each member of the cluster and its centroid. 

If you plot SSE vs. k, you will see that the error decreases as k gets larger; **this is because when the number of clusters increases, they should be smaller, so distortion is also smaller.**

The idea of the elbow method is to choose the k at which the SSE decreases abruptly. But don’t take this as a strict rule when choosing a K value! A lot of time it depends more on the context of the exact situation (domain knowledge)

side note: Pyspark by itself doesn’t support a plotting mechanism, but you could use collect() and then plot the results with matplotlib or other visualization libraries.

44. Here is an example of kmeans clustering coding 


        import findspark
        findspark.init("/home/danial/spark-3.3.2-bin-hadoop3")
        
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName('clustering').getOrCreate()
        
        path = '/home/danial/Desktop/myspark/Apache-Spark/Python-and-Spark-for-Big-Data master/Spark_for_Machine_Learning/Clustering/sample_kmeans_data.txt'
        
        data = spark.read.format('libsvm').load(path)


        data.printSchema()
        root
         |-- label: double (nullable = true)
         |-- features: vector (nullable = true)

        data.show()
        +-----+--------------------+
        |label|            features|
        +-----+--------------------+
        |  0.0|           (3,[],[])|
        |  1.0|(3,[0,1,2],[0.1,0...|
        |  2.0|(3,[0,1,2],[0.2,0...|
        |  3.0|(3,[0,1,2],[9.0,9...|
        |  4.0|(3,[0,1,2],[9.1,9...|
        |  5.0|(3,[0,1,2],[9.2,9...|
        +-----+--------------------+

        since it is unsupervised we don't need label

        final_data = data.select('features')
        final_data.show()
        +--------------------+
        |            features|
        +--------------------+
        |           (3,[],[])|
        |(3,[0,1,2],[0.1,0...|
        |(3,[0,1,2],[0.2,0...|
        |(3,[0,1,2],[9.0,9...|
        |(3,[0,1,2],[9.1,9...|
        |(3,[0,1,2],[9.2,9...|
        +--------------------+


        from pyspark.ml.clustering import KMeans
        
        kmeans = KMeans().setK(2).setSeed(1)
        model = kmeans.fit(final_data)

        **Within Set Sum of Squared Errors (WSSSE)**

        wssse = model.summary.trainingCost
        wssse
        0.11999999999994547
        
        centers = model.clusterCenters()
        centers
        [array([9.1, 9.1, 9.1]), array([0.1, 0.1, 0.1])]
        
        results = model.transform(final_data)
        results.show()
        
        +--------------------+----------+
        |            features|prediction|
        +--------------------+----------+
        |           (3,[],[])|         1|
        |(3,[0,1,2],[0.1,0...|         1|
        |(3,[0,1,2],[0.2,0...|         1|
        |(3,[0,1,2],[9.0,9...|         0|
        |(3,[0,1,2],[9.1,9...|         0|
        |(3,[0,1,2],[9.2,9...|         0|
        +--------------------+----------+

        kmeans = KMeans().setK(3).setSeed(1)
        model = kmeans.fit(final_data)

        wssse = model.summary.trainingCost
        wssse
        0.07499999999994544
        
        centers = model.clusterCenters()
        centers
        [array([9.1, 9.1, 9.1]), array([0.05, 0.05, 0.05]), array([0.2, 0.2, 0.2])]
        
        results = model.transform(final_data)
        results.show()
        
        +--------------------+----------+
        |            features|prediction|
        +--------------------+----------+
        |           (3,[],[])|         1|
        |(3,[0,1,2],[0.1,0...|         1|
        |(3,[0,1,2],[0.2,0...|         2|
        |(3,[0,1,2],[9.0,9...|         0|
        |(3,[0,1,2],[9.1,9...|         0|
        |(3,[0,1,2],[9.2,9...|         0|
        +--------------------+----------+


45. Feature scaling and curse of dimensionality 

So for a certain machine learning algorithms, sometimes it's a good idea to actually scale your data. And this is due to something called **curse of dimensionality**. Basically what happens is you have a drop in model performance with highly dimensional data. So we try to scale features using pyspark.

If you're dealing with a data set that has many, many features to it or many columns of data which is highly dimensional, what you need to do is actually to scale those features.

**curse of dimensionality**:

The curse of dimensionality is a term used in machine learning and other fields to describe the difficulties that arise when analyzing and modeling high-dimensional data. In high-dimensional data, the number of features or dimensions is large, and the amount of data available to estimate the relationships between these features is limited.

One major issue that arises in high-dimensional data is sparsity, which refers to the fact that as the number of dimensions increases, the number of data points required to capture the relationships between them grows exponentially. This can lead to overfitting, where a model becomes too complex and captures noise or irrelevant features in the data.

Another issue with high-dimensional data is that the distance between points in the feature space becomes less meaningful. In other words, the "nearest neighbors" of a data point in high-dimensional space may not actually be very similar to it in terms of the underlying relationships between the features.

To address the curse of dimensionality, dimensionality reduction techniques such as PCA, t-SNE, and others can be used to extract meaningful low-dimensional representations of the data. Additionally, regularization techniques and feature selection methods can be used to prevent overfitting and identify the most important features for modeling.

46. Example of coding KMeans clustering with feature scaling using **StandardScaler**

        import findspark
        findspark.init("/home/danial/spark-3.3.2-bin-hadoop3/")
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder.appName('cluster').getOrCreate()
        
        path = '/home/danial/Desktop/myspark/Apache-Spark/Python-and-Spark-for-Big-Data-master/Spark_for_Machine_Learning/Clustering/seeds_dataset.csv'
        
        data = spark.read.csv(path, header=True, inferSchema=True)
        data.printSchema()
        
        root
         |-- area: double (nullable = true)
         |-- perimeter: double (nullable = true)
         |-- compactness: double (nullable = true)
         |-- length_of_kernel: double (nullable = true)
         |-- width_of_kernel: double (nullable = true)
         |-- asymmetry_coefficient: double (nullable = true)
         |-- length_of_groove: double (nullable = true)

        data.head(1)
        
        [Row(area=15.26, perimeter=14.84, compactness=0.871, length_of_kernel=5.763, width_of_kernel=3.312, asymmetry_coefficient=2.221, length_of_groove=5.22)]


        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import VectorAssembler
        
        data.columns
        ['area',
         'perimeter',
         'compactness',
         'length_of_kernel',
         'width_of_kernel',
         'asymmetry_coefficient',
         'length_of_groove']
         
        assembler = VectorAssembler(inputCols=data.columns, outputCol='features')
        
        final_data = assembler.transform(data)
        
        final_data.printSchema()
        
        root
         |-- area: double (nullable = true)
         |-- perimeter: double (nullable = true)
         |-- compactness: double (nullable = true)
         |-- length_of_kernel: double (nullable = true)
         |-- width_of_kernel: double (nullable = true)
         |-- asymmetry_coefficient: double (nullable = true)
         |-- length_of_groove: double (nullable = true)
         |-- features: vector (nullable = true)

**Since a lot of machine learning algorithm object don't mind having a bunch of extra columns (they don't read them) they won't do anything with them, they just look for features column and (in case of supervised also they look for label column) so I don't need to perform the following one line of code**: 

my_final_data = final_data.select('features')
        
        from pyspark.ml.feature import StandardScaler
        
        scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures')
        scaler_model = scaler.fit(final_data)
        final_data = scaler_model.transform(final_data)
        final_data.printSchema()
        
        root
         |-- area: double (nullable = true)
         |-- perimeter: double (nullable = true)
         |-- compactness: double (nullable = true)
         |-- length_of_kernel: double (nullable = true)
         |-- width_of_kernel: double (nullable = true)
         |-- asymmetry_coefficient: double (nullable = true)
         |-- length_of_groove: double (nullable = true)
         |-- features: vector (nullable = true)
         |-- scaledFeatures: vector (nullable = true)

        final_data.head(1)
        
        [Row(area=15.26, perimeter=14.84, compactness=0.871, length_of_kernel=5.763, width_of_kernel=3.312, asymmetry_coefficient=2.221, length_of_groove=5.22, features=DenseVector([15.26, 14.84, 0.871, 5.763, 3.312, 2.221, 5.22]), scaledFeatures=DenseVector([5.2445, 11.3633, 36.8608, 13.0072, 8.7685, 1.4772, 10.621]))]
        kmeans = KMeans(featuresCol='scaledFeatures', k=3)

So you can see that there is a change between the features and the scaled features, but it's not a huge change. This becomes more important when you have not just high dimensions of data, meaning many features, but you also have orders of magnitude varying a lot between your actual data. So maybe you have one column that's in units of thousands of miles or something, and then you have another column that's in units of millimeters. Those kind of things can also be improved upon using some sort of scaling technique.

        model = kmeans.fit(final_data)
        
        print (f" wssse is equal to {model.summary.trainingCost}")
         wssse is equal to 429.07559671507244
         
        centers = model.clusterCenters()
        centers
        [array([ 4.87257659, 10.88120146, 37.27692543, 12.3410157 ,  8.55443412,
                 1.81649011, 10.32998598]),
         array([ 6.31670546, 12.37109759, 37.39491396, 13.91155062,  9.748067  ,
                 2.39849968, 12.2661748 ]),
         array([ 4.06105916, 10.13979506, 35.80536984, 11.82133095,  7.50395937,
                 3.27184732, 10.42126018])]
                 
        model.transform(final_data).select('prediction').show()
        
        +----------+
        |prediction|
        +----------+
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         1|
        |         1|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         0|
        |         2|
        +----------+
        only showing top 20 rows

47. Content-Based vs. Collaborative filtering for recommender systerms 

The two most common types of recommender systems are Content-Based and Collaborative Filtering (CF).

+ **Collaborative filtering** produces recommendations based on the knowledge of users’ attitude towards items, that is it uses the **"wisdom of the crowd"** to recommend items.

+ **Content-based recommender** systems focus on the attributes of the items and give you recommendations based on the similarity between them.

some points:

+ In general, **Collaborative filtering (CF) is more commonly used than content-based systems** because it usually gives better results and is relatively easy to understand (from an overall implementation perspective). 

+ The algorithm has the ability to do feature learning on its own, which means that it can start to learn for itself what features to use.

+ These techniques aim to fill in the missing entries of a user-item association matrix. 

+ spark.ml currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries.

+ spark.ml uses the **alternating least squares (ALS)** algorithm to learn these latent factors.

+ Your data needs to be in a specific format to work with Spark’s ALS Recommendation Algorithm!

+ ALS is basically a Matrix Factorization approach to implement a recommendation algorithm you decompose your large user/item matrix into lower dimensional user factors and item factors.

+ The hard part is getting your data into the specific format and getting enough of it.

48. Here is a code for building a recommender system:

        import findspark
        findspark.init("/home/danial/spark-3.3.2-bin-hadoop3/")

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName('recommender').getOrCreate()

        path = '/home/danial/Desktop/myspark/Apache-Spark/Python-and-Spark-for-Big-Data-master/Spark_for_Machine_Learning/Recommender_Systems/movielens_ratings.csv'

        data = spark.read.csv(path, header=True, inferSchema=True)
        data.show()

        +-------+------+------+
        |movieId|rating|userId|
        +-------+------+------+
        |      2|   3.0|     0|
        |      3|   1.0|     0|
        |      5|   2.0|     0|
        |      9|   4.0|     0|
        |     11|   1.0|     0|
        |     12|   2.0|     0|
        |     15|   1.0|     0|
        |     17|   1.0|     0|
        |     19|   1.0|     0|
        |     21|   1.0|     0|
        |     23|   1.0|     0|
        |     26|   3.0|     0|
        |     27|   1.0|     0|
        |     28|   1.0|     0|
        |     29|   1.0|     0|
        |     30|   1.0|     0|
        |     31|   1.0|     0|
        |     34|   1.0|     0|
        |     37|   1.0|     0|
        |     41|   2.0|     0|
        +-------+------+------+
        only showing top 20 rows

        data.describe().show()

        +-------+------------------+------------------+------------------+
        |summary|           movieId|            rating|            userId|
        +-------+------------------+------------------+------------------+
        |  count|              1501|              1501|              1501|
        |   mean| 49.40572951365756|1.7741505662891406|14.383744170552964|
        | stddev|28.937034065088994| 1.187276166124803| 8.591040424293272|
        |    min|                 0|               1.0|                 0|
        |    max|                99|               5.0|                29|
        +-------+------------------+------------------+------------------+

        from pyspark.ml.recommendation import ALS
        from pyspark.ml.evaluation import RegressionEvaluator
        
        training, testing = data.randomSplit([0.8, 0.2])

As mentioned, a big part of building a recommendation system that is intended to work with Python and spark is that you actually get your data into a specific format, which is attained through the following line of code: 

        als = ALS(maxIter= 5, regParam=0.01, itemCol= 'movieId', userCol='userId', ratingCol='rating')
        model = als.fit(training)

        predictions = model.transform(testing)
        predictions.show()
        
        +-------+------+------+-----------+
        |movieId|rating|userId| prediction|
        +-------+------+------+-----------+
        |      1|   1.0|    28|  0.9663938|
        |      5|   2.0|    26| 0.02526009|
        |      4|   1.0|    12|  0.8127196|
        |      3|   2.0|    22|  1.6282926|
        |      3|   1.0|     1|  1.3922048|
        |      4|   2.0|    13|   1.782724|
        |      6|   1.0|    20| 0.24720451|
        |      4|   1.0|     5|  1.3330386|
        |      2|   1.0|    19|  1.5031521|
        |      4|   1.0|    19|  1.8913373|
        |      1|   4.0|    15|  1.3782326|
        |      6|   1.0|    15|  1.0345336|
        |      2|   1.0|    17|  1.9824493|
        |      0|   1.0|    23|  2.1761677|
        |      4|   1.0|    23| 0.94977033|
        |      2|   4.0|    10|  3.7171347|
        |      4|   1.0|    24|0.024004161|
        |      2|   4.0|    21|  2.5022135|
        |      0|   1.0|    11|  1.6437361|
        |      5|   1.0|    14|  3.3678727|
        +-------+------+------+-----------+
        only showing top 20 rows
        
        evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')
        rmse = evaluator.evaluate(predictions)

        print (f"the RMSE is equal to {rmse}")
        the RMSE is equal to 1.9813298445825118
        
        data.describe().show()
        +-------+------------------+------------------+------------------+
        |summary|           movieId|            rating|            userId|
        +-------+------------------+------------------+------------------+
        |  count|              1501|              1501|              1501|
        |   mean| 49.40572951365756|1.7741505662891406|14.383744170552964|
        | stddev|28.937034065088994| 1.187276166124803| 8.591040424293272|
        |    min|                 0|               1.0|                 0|
        |    max|                99|               5.0|                29|
        +-------+------------------+------------------+------------------+

SO the RMSE of 1.98 is too high given the fact that the rating values are in the range of 1 to 5! Which is because small dataset that we used!

        single_user = testing.filter(testing['userId']==11).select(['movieId', 'userId'])
        single_user.show()

        +-------+------+
        |movieId|userId|
        +-------+------+
        |      0|    11|
        |     13|    11|
        |     25|    11|
        |     38|    11|
        |     43|    11|
        |     50|    11|
        |     59|    11|
        |     66|    11|
        |     72|    11|
        |     75|    11|
        |     76|    11|
        |     88|    11|
        +-------+------+

        recommendations = model.transform(single_user)
        recommendations.orderBy('prediction', ascending = False).show()
        +-------+------+------------+
        |movieId|userId|  prediction|
        +-------+------+------------+
        |     76|    11|   5.5854764|
        |     25|    11|    5.083869|
        |     66|    11|    3.925778|
        |     50|    11|   3.5083916|
        |      0|    11|   1.6437361|
        |     13|    11|   1.5627245|
        |     75|    11|   1.1251544|
        |     88|    11|  0.47486284|
        |     59|    11|-0.043731105|
        |     38|    11| -0.14139383|
        |     72|    11| -0.66860205|
        |     43|    11|  -1.7488561|
        +-------+------+------------+


side note: Keep in mind, it's actually really hard to know conclusively how well a recommender system did, especially for a certain topics that **subjectivity is involved**. For example, not everyone that loves Star Wars is going to love Star Trek, even if they're both science fiction. Certain users or certain people just aren't going to like the same things, especially when subjectivity is involved with creative items like books or movies, etc.


**Cold Start:**

There's also something called a cold start problem, and the cold start problem is what do you do if users that are new to your platform and haven't seen any movies whatsoever? Well, there's different ways of trying to solve that. You could give them a quick survey on what movies have they watched? Can you quickly rate them for us? You can also just say, Hey, are you similar to user X, Y or Z, which is typical profile, etc. So those are more domain knowledge issues than actual algorithmic issues. But keep that in mind Cold Start is definitely a problem with recommendation systems in general.

**49. Spark Streaming with Python**

Documented in the coding jupyter notebook file. 



