"""Week 4 Task 3 fare regressor and evaluation shared by Task 3 and Task 4."""

from __future__ import annotations

import math

from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REG_PARAM = 0.1
ELASTIC_NET_PARAM = 0.0
MAX_ITER = 50
SOLVER = "l-bfgs"
# Task 2 already robustly scales continuous values.
STANDARDIZATION = False


def build_regressor() -> LinearRegression:
    """Return the unfitted regularized linear regression used by every workflow."""
    return LinearRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        regParam=REG_PARAM,
        elasticNetParam=ELASTIC_NET_PARAM,
        maxIter=MAX_ITER,
        solver=SOLVER,
        standardization=STANDARDIZATION,
        loss="squaredError",
    )


def hyperparameters() -> dict:
    """Return the regressor configuration in the manifest format."""
    return {
        "reg_param": REG_PARAM,
        "elastic_net_param": ELASTIC_NET_PARAM,
        "max_iter": MAX_ITER,
        "solver": SOLVER,
        "standardization": STANDARDIZATION,
    }


def evaluate_split(model, split_df: DataFrame, baseline_prediction: float):
    """Compute model and constant-baseline MAE, RMSE, and R2 in one aggregation."""
    scored = model.transform(split_df).select("label", "prediction")
    row = scored.agg(
        F.count("*").alias("n"),
        F.sum(F.abs(F.col("label") - F.col("prediction"))).alias("model_abs"),
        F.sum(F.pow(F.col("label") - F.col("prediction"), 2)).alias("model_sq"),
        F.sum(F.abs(F.col("label") - F.lit(baseline_prediction))).alias("baseline_abs"),
        F.sum(F.pow(F.col("label") - F.lit(baseline_prediction), 2)).alias("baseline_sq"),
        F.sum("label").alias("label_sum"),
        F.sum(F.pow(F.col("label"), 2)).alias("label_sq_sum"),
    ).first()

    n = row["n"]
    total_variance = row["label_sq_sum"] - (row["label_sum"] ** 2 / n)

    def metrics_from(abs_error, squared_error):
        return {
            "mae": abs_error / n,
            "rmse": math.sqrt(squared_error / n),
            "r2": 1.0 - squared_error / total_variance,
        }

    return (
        metrics_from(row["model_abs"], row["model_sq"]),
        metrics_from(row["baseline_abs"], row["baseline_sq"]),
    )
