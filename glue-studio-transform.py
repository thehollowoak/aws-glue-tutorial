def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import split, col, current_date
    
    # Convert to PySpark Data Frame 
    sourcedata = dfc.select(list(dfc.keys())[0]).toDF()
    
    # Do Transformations
    split_col = split(sourcedata["quarter"], " ")
    sourcedata = sourcedata.withColumn("quarter", split_col.getItem(0))
    sourcedata = sourcedata.withColumn("profit", col("revenue")*col("gross margin"))
    sourcedata = sourcedata.withColumn("current date", current_date())
    
    # Convert back to Glue Dynamic Frame
    dyf = DynamicFrame.fromDF(sourcedata, glueContext, "datasource")
    return DynamicFrameCollection({"CustomTransform": dyf}, glueContext)
