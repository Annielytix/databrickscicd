# MAGIC %md
# MAGIC 
# MAGIC Motiva Predictive Maintenance ML Pipeline

# COMMAND ----------

# File location and type
#file_location = "/FileStore/tables/Truck_Liftings__Component_Product____Mar_2018-a6681.csv"
#file_locationApr = "/FileStore/tables/Truck_Liftings__Component_Product____Apr_2018-d43ce.csv"
file_locationMay = "/FileStore/tables/Truck_Liftings__Component_Product____May_2018-1c2a0.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df3 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_locationMay)

display(df3)
#display(df2)

# COMMAND ----------

# Create a view or table

temp_table_nameMay = "Truck_Liftings_Component_May"


df3.createOrReplaceTempView(temp_table_nameMay)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC t
# MAGIC /* select * from `Truck_Liftings_Component_Apr` */

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_nameMay = "Truck_Liftings_Component_May"

df3.write.format("parquet").saveAsTable(permanent_table_nameMay)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Truck_Liftings_Component_May tlc full outer join Truck_Liftings_Component_Apr tlcapr on tlc.cust_no = tlcapr.cust_no and tlc.term_id = tlcapr.term_id  order by tlc.load_start

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC val parquetDF = spark.read.parquet("Truck_Liftings_Component_May")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * into Truck_Liftings_Component_new tlc from Truck_Liftings_Component_Apr tlcapr full outer join Truck_Liftings_Component_May tlcmay on tlcapr.cust_no = tlcmay.cust_no 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Truck_Liftings_Component ptm
# MAGIC USING Truck_Liftings_Component_Apr pta
# MAGIC ON ptm.term_id = pta.term_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     ptm.data = pta.data
# MAGIC WHEN NOT
# MAGIC 
# MAGIC MATCHED
# MAGIC   THEN INSERT (ptm.term_id, ptm.data) VALUES (pta.term_id,pta.data)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Truck_Liftings_Component_Final
# MAGIC COMMENT 'This table is created with existing data'
# MAGIC     AS SELECT * FROM Truck_Liftings_Component

# COMMAND ----------

CREATE TABLE Truck_Liftings_Component_Jun
  USING csv
  OPTIONS (path " /FileStore/tables/Truck_Liftings__Component_Product____Jun_2018-6e47a.csv", header "true")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC insert into Truck_Liftings_Component (term_id, doc_no, trans_ref_no, load_start, load_stop, cust_no, acct_no, cancel_rebill, prod_rec_no, comp_rec_no, prod_id, temperature, grav_density, gross_volume, net_volume) select term_id, doc_no, trans_ref_no, load_start, load_stop, cust_no, acct_no, cancel_rebill, prod_rec_no, comp_rec_no, prod_id, temperature, grav_density, gross_volume, net_volume from permanent_table_nameApr

# COMMAND ----------

# MAGIC %sh apt-get --yes install pandoc

# COMMAND ----------

# MAGIC 
# MAGIC %r
# MAGIC 
# MAGIC install.packages("htmlwidgets")

# COMMAND ----------

# MAGIC %r
# MAGIC databricksURL <- "https://eastus2.cloud.databricks.com"

# COMMAND ----------

# MAGIC %r
# MAGIC library(htmlwidgetsp)
# MAGIC db_html_print <- function(x, ..., view = interactive()) {
# MAGIC   fileName <- paste(tempfile(), ".html", sep="")
# MAGIC   htmlwidgets::saveWidget(x, file = fileName)
# MAGIC   
# MAGIC   randomFileName = paste0(floor(runif(1, 0, 10^12)), ".html")
# MAGIC   baseDir <- "/dbfs/FileStore/rwidgets/"
# MAGIC   dir.create(baseDir)
# MAGIC   internalFile = paste0(baseDir, randomFileName)
# MAGIC   externalFile = paste0(databricksURL, "/files/rwidgets/", randomFileName)
# MAGIC   system(paste("cp", fileName, internalFile))
# MAGIC   displayHTML(externalFile)
# MAGIC }
# MAGIC R.utils::reassignInPackage("print.htmlwidget", pkgName = "htmlwidgets", value = db_html_print)

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

# COMMAND ----------

# MAGIC %sh
# MAGIC python3 get-pip.py

# COMMAND ----------

# MAGIC %sh
# MAGIC pip3 install plotly

# COMMAND ----------



# COMMAND ----------

import plotly
from plotly.offline import plot
from plotly.graph_objs import *
import numpy as np

x = np.random.randn(2000)
y = np.random.randn(2000)

# Instead of simply calling plot(...), store your plot as a variable and pass it to displayHTML().
# Make sure to specify output_type='div' as a keyword argument.
# (Note that if you call displayHTML() multiple times in the same cell, only the last will take effect.)

p = plot(
  [
    Histogram2dContour(x=x, y=y, contours=Contours(coloring='heatmap')),
    Scatter(x=x, y=y, mode='markers', marker=Marker(color='white', size=3, opacity=0.3))
  ],
  output_type='div'
)

displayHTML(p)