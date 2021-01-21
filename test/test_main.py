import pytest
from src.main import *
import pandas as pd

class TestClass:

    pytestmark = pytest.mark.usefixtures("spark")

    def test_do_word_counts(self, spark):
        print(spark)
        test_input = [
            ' hello spark ',
            ' hello again spark spark'
        ]

        input_rdd = spark.sparkContext.parallelize(test_input, 1)
        results = do_word_counts(input_rdd)
        expected_results = {'hello':2, 'spark':3, 'again':1}  
        assert results == expected_results

    def test_filter_spark_data_frame(self, spark):
        input = spark.createDataFrame(
            [('charly', 16),
            ('fabien', 15),
            ('sam', 21),
            ('sam', 25),
            ('nick', 19),
            ('nick', 40)],
            ['name', 'age'],
        )
        expected_output = spark.createDataFrame(
            [('sam', 25),
            ('sam', 21),
            ('nick', 40)],
            ['name', 'age'],
        )
        real_output = filter_spark_data_frame(input)
        real_output = self.get_sorted_data_frame(
            real_output.toPandas(),
            ['age', 'name'],
        )
        expected_output = self.get_sorted_data_frame(
            expected_output.toPandas(),
            ['age', 'name'],
        )
        pd.testing.assert_frame_equal(expected_output, real_output, check_like=True)


    def get_sorted_data_frame(self, data_frame, columns_list):
        return data_frame.sort_values(columns_list).reset_index(drop=True)