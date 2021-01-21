from pyspark.sql.functions import col


def filter_spark_data_frame(
    dataframe,
    column_name='age',
    value=20,
):
    return dataframe.where(col(column_name) > value)



def do_word_counts(lines):
    counts = (lines.flatMap(lambda x: x.split())
                  .map(lambda x: (x, 1))
                  .reduceByKey(lambda x, y: x+y)
             ) 
    results = {word: count for word, count in counts.collect()}
    return results
