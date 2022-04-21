import argparse

import pyspark.sql.functions as funcs
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession

from utils import extract_values_from_vector_udf, max_index_udf, second_max_index_udf


def main():
    parser = argparse.ArgumentParser(
        description='Process csv files with names of institutes to solve institute disambiguation in your data!'
    )
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file to process.'
    )
    parser.add_argument(
        '--output',
        dest='output',
        default='result',
        help='Output folder to write results to.'
    )
    known_args, _ = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(known_args.input, schema='uni string')

    # Preprocessing
    df = df.withColumn('processed', funcs.lower(df.uni))  # make text lowercase
    df = df.withColumn('processed', funcs.regexp_replace(df.processed, '\W', ' '))  # remove special character
    df = df.withColumn('processed', funcs.regexp_replace(df.processed, '[0-9]+', ' '))  # remove numbers
    df = df.withColumn('processed', funcs.regexp_replace(df.processed, '\s+[a-zA-Z]\s+', ''))  # remove single character
    df = df.withColumn('processed', funcs.regexp_replace(df.processed, '\s+', ' '))  # remove multi space

    # TF-IDF model
    domain_words = ['university', 'institute', 'department', 'center', 'sciences', 'science', 'unit']
    pipeline = Pipeline(stages=[
        Tokenizer(inputCol='processed', outputCol='token'),
        StopWordsRemover(inputCol='token', outputCol='no_stop_words'),
        StopWordsRemover(inputCol='no_stop_words', outputCol='no_stop_words_token', stopWords=domain_words),
        HashingTF(inputCol='no_stop_words_token', outputCol='tf'),
        IDF(inputCol='tf', outputCol='idf')
    ])
    model = pipeline.fit(df)
    tfidf = model.transform(df)

    # Calculating of Bigrams with the most valuable words
    tfidf = tfidf.withColumn('key', extract_values_from_vector_udf(tfidf['idf']))
    tfidf = tfidf.filter(funcs.size(tfidf['key']) > 0)
    tfidf = tfidf.withColumn('min_ind', max_index_udf(tfidf['key']))
    tfidf = tfidf.withColumn('second_min_ind', second_max_index_udf(tfidf['key']))
    tfidf = tfidf.withColumn('min_word', funcs.concat_ws(
        ' ', tfidf['no_stop_words_token'][tfidf['min_ind']], tfidf['no_stop_words_token'][tfidf['second_min_ind']]
    ))
    # MD5 hash of Bigrams acts as ID of the cluster
    tfidf = tfidf.withColumn('result_id', funcs.md5(tfidf['min_word']))

    # Saving of the result
    tfidf.select('uni', 'result_id').write.csv(known_args.output)


if __name__ == '__main__':
    main()
