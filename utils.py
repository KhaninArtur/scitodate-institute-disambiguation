import pyspark.sql.functions as funcs

from pyspark.sql.types import IntegerType, ArrayType, DoubleType


def extract_values_from_vector(vector):
    return vector.values.tolist()


def extract_values_from_vector_udf(col):
    return funcs.udf(extract_values_from_vector, ArrayType(DoubleType()))(col)


max_index_udf = funcs.udf(lambda x: x.index(max(x)), IntegerType())


def second_max_index_udf(col):
    def second_max_index(array):
        arr = array.copy()
        arr.remove(max(arr))
        return array.index(max(arr)) if len(arr) > 0 else 0
    return funcs.udf(second_max_index, IntegerType())(col)
