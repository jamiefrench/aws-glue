from awsglue import DynamicFrame
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def transform(df: DataFrame, column_name: str, case: str) -> DataFrame:
    valid_cases = ["uppercase", "lowercase"]
    if case not in valid_cases:
        raise ValueError(
            f"Provided value '{case}' for parameter 'case' is invalid. Valid options are: {', '.join(valid_cases)}."  # pylint: disable=C0301
        )

    if not column_name in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")

    if df.schema[column_name].dataType.simpleString() != "string":
        raise AnalysisException(f"Column '{column_name}' is not of StringType.")

    if case == "uppercase":
        df = df.withColumn(column_name, F.upper(F.col(column_name)))
    else:
        df = df.withColumn(column_name, F.lower(F.col(column_name)))

    return df


def case_transform(self, column_name: str, case: str) -> DynamicFrame:
    df: DataFrame = self.toDF()
    df = transform(df, column_name, case)
    return DynamicFrame.fromDF(df, self.glue_ctx, "case_transform")


DynamicFrame.case_transform = case_transform