// MAGIC %md # Saving & loading Machine Learning (ML) models
// MAGIC *via the application of handwritten digit recognition*
// MAGIC 
// MAGIC This notebook demonstrates saving and loading ML models, including single models and complex Pipelines.  This is written for Apache Spark 2.0.
// MAGIC 
// MAGIC **Notebook contents**
// MAGIC * Basic Python-Scala workflow demonstrating model saving and loading between languages
// MAGIC   * Load the data.
// MAGIC   * Fit a Random Forest to the data using Python, and save the model.
// MAGIC   * Load the Random Forest model using Scala, and apply it to a new dataset.
// MAGIC * Saving complex Machine Learning Pipelines
// MAGIC   * Scala: Create a more complex Pipeline to fit the data.  Save the Pipeline, both before fitting (as a "recipe") and after fitting (as a fitted complex model).
// MAGIC   * Python: Demonstrate a similar workflow, showing how to save results from model selection.
// MAGIC * Note on saving and loading R models

// COMMAND ----------

// MAGIC %md #### Set up
// MAGIC We first create a temp folder which will be used for saving and loading models.

// COMMAND ----------

// WARNING: This notebook will save and load using the following basePath, so make sure the directory is usable.
val basePath = "/tmp/mllib-persistence-example"
dbutils.fs.rm(basePath, recurse=true)
dbutils.fs.mkdirs(basePath)

// COMMAND ----------

// MAGIC %md ## Basic Python-Scala workflow
// MAGIC 
// MAGIC Goal: Demonstrate model saving and loading between languages

// COMMAND ----------

// MAGIC %md ### Python: the data scientist
// MAGIC 
// MAGIC In this section, the data scientist loads training data, fits a Random Forest, and saves the model for others to use.

// COMMAND ----------

// MAGIC %md #### Load MNIST training and test datasets
// MAGIC 
// MAGIC Our datasets are vectors of pixels representing images of handwritten digits.  For example:
// MAGIC 
// MAGIC ![Image of a digit](http://training.databricks.com/databricks_guide/digit.png)
// MAGIC ![Image of all 10 digits](http://training.databricks.com/databricks_guide/MNIST-small.png)

// COMMAND ----------

// MAGIC %md These datasets are stored in the popular LibSVM dataset format.  We will load them using the Spark SQL data source for LibSVM files.

// COMMAND ----------

// MAGIC %py
// MAGIC # We explicitly set numFeatures = 784 since the images are 28x28 = 784 pixels (features).
// MAGIC training = sqlContext.read.format("libsvm").option("numFeatures", "784").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt")
// MAGIC 
// MAGIC # Cache data for multiple uses.
// MAGIC training.cache()
// MAGIC 
// MAGIC print "We have %d training images." % training.count()

// COMMAND ----------

// MAGIC %md Show a sample of our data.  Each image has the true label (the `label` column) and a vector of `features` which represent pixel intensities.  Note the vectors are stored in sparse format.

// COMMAND ----------

// MAGIC %py
// MAGIC display(training)

// COMMAND ----------

// MAGIC %md #### Train a Random Forest Classifier
// MAGIC 
// MAGIC We train a random forest for classifying images of digits.  Given a new image of a digit, this classifier will be able to predict the actual digit 0 - 9 in the image.

// COMMAND ----------

// MAGIC %py
// MAGIC from pyspark.ml.classification import RandomForestClassifier
// MAGIC # Run the Random Forest algorithm on our data
// MAGIC rf = RandomForestClassifier(numTrees=20)
// MAGIC model = rf.fit(training)

// COMMAND ----------

// MAGIC %md #### Save the Random Forest model

// COMMAND ----------

// MAGIC %py
// MAGIC basePath = "/tmp/mllib-persistence-example"
// MAGIC model.save(basePath + "/model")
// MAGIC 
// MAGIC # You may also specify "overwrite" just as when saving Datasets and DataFrames:
// MAGIC # model.write().overwrite().save(basePath + "/model")

// COMMAND ----------

// MAGIC %md ### Scala: The data engineer
// MAGIC 
// MAGIC A data engineer now loads the model from the data scientist into a Scala workflow and tests it on new data.

// COMMAND ----------

import org.apache.spark.ml.classification.RandomForestClassificationModel
val model = RandomForestClassificationModel.load(basePath + "/model")

val test = sqlContext.read.format("libsvm").option("numFeatures", "784").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-test.txt")

val predictions = model.transform(test)
display(predictions.select("label", "prediction"))

// COMMAND ----------

// MAGIC %md As you can see, many labels and predictions match, but some do not.  With more effort, we could tune the model to make improved predictions.  However, we will not explore accuracy and tuning deeply since the goal of this notebook is to demonstrate model persistence.

// COMMAND ----------

// MAGIC %md We next print some model info just to show that these models are indeed the same.

// COMMAND ----------

// MAGIC %scala
// MAGIC println(s"Model has most important feature: ${model.featureImportances.argmax}")

// COMMAND ----------

// MAGIC %py
// MAGIC import numpy
// MAGIC print "Model has most important feature: %d" % numpy.argmax(model.featureImportances.toArray())

// COMMAND ----------

// MAGIC %md ## Saving complex Machine Learning Pipelines
// MAGIC 
// MAGIC We can also save and load entire ML Pipelines, including feature extraction and transformations, model fitting, and hyperparameter tuning.  Here, we will demonstrate this with a Pipeline consisting of:
// MAGIC * Feature extraction: Binarizer to convert images to black and white (just for the sake of this demo)
// MAGIC * Model fitting: Random Forest Classifier
// MAGIC * Tuning: Cross-Validation

// COMMAND ----------

// MAGIC %md ### Scala

// COMMAND ----------

// MAGIC %md First, we will define our Pipeline.

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}

// Define our basic Pipeline: [feature transformation] -> [random forest]
val binarizer = new Binarizer().setInputCol("features").setOutputCol("binaryFeatures").setThreshold(0.1)
val rf = new RandomForestClassifier().setNumTrees(20).setFeaturesCol("binaryFeatures")
val evaluator = new MulticlassClassificationEvaluator()
val pipeline = new Pipeline().setStages(Array(binarizer, rf))

// Define the grid of hyperparameters to test for model tuning.
val paramGrid = new ParamGridBuilder().addGrid(rf.maxDepth, Array(3, 5, 8)).build()

// Define our model tuning workflow using Cross Validation.
val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)

// COMMAND ----------

// MAGIC %md Before we fit this Pipeline, we will show that we can save entire workflows (before fitting).  This workflow could be loaded later to run on another dataset, on another Spark cluster, etc.

// COMMAND ----------

cv.save(basePath + "/cv")
val sameCV = CrossValidator.load(basePath + "/cv")

// COMMAND ----------

// MAGIC %md Comparing `cv` and `sameCV`, we can see they are the same.

// COMMAND ----------

cv.getEstimatorParamMaps

// COMMAND ----------

sameCV.getEstimatorParamMaps

// COMMAND ----------

// MAGIC %md Let's fit our entire Pipeline.  This can take a minute or longer since it trains (3 folds of cross-validation) x (3 random forests) x (20 trees per forest) = 180 trees.

// COMMAND ----------

val training = sqlContext.read.format("libsvm").option("numFeatures", "784").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt").cache()
val cvModel = cv.fit(training)

// COMMAND ----------

// MAGIC %md As before, we can make predictions using our Pipeline (`cvModel`).  This will pass test examples through feature transformations and make predictions using the best model chosen during cross-validation.

// COMMAND ----------

val predictions = cvModel.transform(test)
display(predictions.select("label", "prediction"))

// COMMAND ----------

// MAGIC %md We can now save our fitted Pipeline.  This saves both the feature extraction stage and the random forest model chosen by model tuning.

// COMMAND ----------

cvModel.save(basePath + "/cvModel")

// COMMAND ----------

val sameCVModel = CrossValidatorModel.load(basePath + "/cvModel")

// COMMAND ----------

// MAGIC %md Once again, our original and re-loaded models are the same!

// COMMAND ----------

cvModel.avgMetrics

// COMMAND ----------

sameCVModel.avgMetrics

// COMMAND ----------

// MAGIC %md ### Python: Saving results for Python CrossValidator and TrainValidationSplit
// MAGIC The one missing item in Spark 2.0 is Python tuning.  Python does not yet support saving and loading CrossValidator and TrainValidationSplit, which are used to tune model hyperparameters.  However, it is still possible to save the results from CrossValidator and TrainValidationSplit from Python.  For example, let's use Cross-Validation to tune a Random Forest.

// COMMAND ----------

// MAGIC %py
// MAGIC from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
// MAGIC from pyspark.ml.evaluation import MulticlassClassificationEvaluator
// MAGIC from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
// MAGIC 
// MAGIC # Define our workflow: CrossValidator[RandomForest]
// MAGIC rf = RandomForestClassifier()
// MAGIC evaluator = MulticlassClassificationEvaluator()
// MAGIC paramGrid = ParamGridBuilder().addGrid(rf.maxDepth, [3, 5, 8]).build()
// MAGIC cv = CrossValidator(estimator=rf, evaluator=evaluator, estimatorParamMaps=paramGrid)
// MAGIC 
// MAGIC # Run Cross-Validation to fit the model
// MAGIC cvModel = cv.fit(training)
// MAGIC 
// MAGIC # Get the best Random Forest model
// MAGIC rfModel = cvModel.bestModel
// MAGIC 
// MAGIC # Save & load the Random Forest model
// MAGIC rfPath = "/tmp/mllib-persistence-example/rfModel"
// MAGIC rfModel.save(rfPath)
// MAGIC sameRFModel = RandomForestClassificationModel.load(rfPath)

// COMMAND ----------

// MAGIC %md ## Note on saving and loading R models
// MAGIC 
// MAGIC Creating and saving models in R is straightforward.  However, loading those models in Scala, Java, or Python requires a hack currently.  The reason is that R models save extra metadata and therefore use a somewhat different format than the other languages.  In future releases, we plan to smooth out this process to make transitioning models between R and other languages seamless.
// MAGIC 
// MAGIC The hack goes as follows.
// MAGIC 
// MAGIC **R: create and save model**
// MAGIC 
// MAGIC `
// MAGIC model <- naiveBayes(...)
// MAGIC write.ml(model, "myModelPath")
// MAGIC `
// MAGIC 
// MAGIC This saves the model as a Pipeline which includes the features transformations from the R formula.
// MAGIC 
// MAGIC **Scala: load model**
// MAGIC 
// MAGIC `
// MAGIC import org.apache.spark.ml.PipelineModel
// MAGIC val model = PipelineModel.load("myModelPath/pipeline")
// MAGIC `
// MAGIC 
// MAGIC Note that we loaded from the "pipeline" folder.  This loads a PipelineModel which handles feature transformations.
// MAGIC 
// MAGIC In the future, we plan to make this process seamless and avoid this hack.

// COMMAND ----------

// MAGIC %md #### Clean up temp directory
// MAGIC 
// MAGIC The below line is to clean up the temp directory in Databricks.

// COMMAND ----------

dbutils.fs.rm(basePath, recurse=true)