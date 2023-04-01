def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date""");
        print("Validate the spark session by printing"+str(opDF.collect()))
    except NameError as exp:
        print("NameError in method get_curr_date() Please check the stack trace - "+ str(exp))
        raise
    except Exception as e:
        print("NameError in method get_curr_date() Please check the stack trace - " + str(e))
        raise
    else:
        print("Spark Object is Validated")


