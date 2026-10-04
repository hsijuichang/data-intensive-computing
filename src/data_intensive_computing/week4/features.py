"""Week 4 Task 2 fare feature pipeline: the single source of truth for features.

``build_feature_pipeline()`` returns an *unfitted* Spark ML ``Pipeline``. Fit it on
the Train split only, then apply the fitted ``PipelineModel`` unchanged to
Validation and Test so that medians, category mappings, and scaling statistics
never use future data.
"""

from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    RobustScaler,
    SQLTransformer,
    StringIndexer,
    VectorAssembler,
)

# Columns the Task 1-style training dataset must provide.
REQUIRED_INPUT_COLUMNS = frozenset(
    {
        "trip_id",
        "pickup_datetime",
        "split",
        "label",
        "pickup_zone",
        "dropoff_zone",
        "pickup_borough",
        "dropoff_borough",
        "trip_distance",
        "passenger_count",
        "vendor_id",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_month_of_year",
        "is_weekend",
        "pickup_hour_sin",
        "pickup_hour_cos",
        "pickup_dow_sin",
        "pickup_dow_cos",
        "weather_temperature_c",
        "weather_relative_humidity_pct",
        "weather_precipitation_mm",
        "weather_wind_speed_kmh",
        "weather_pressure_hpa",
        "weather_condition_code",
        "air_quality_pm25_mean",
        "air_quality_missing",
    }
)

# Columns written to the feature Delta table.
FEATURE_DATASET_COLUMNS = ["trip_id", "pickup_datetime", "split", "label", "features"]

IMPUTER_INPUT_COLS = [
    "passenger_count",
    "weather_relative_humidity_pct",
    "weather_precipitation_mm",
    "air_quality_pm25_mean",
]
IMPUTER_OUTPUT_COLS = [f"{column}_imputed" for column in IMPUTER_INPUT_COLS]

# Only continuous values are robustly scaled.
CONTINUOUS_NUMERIC_COLS = [
    "trip_distance",
    "passenger_count_imputed",
    "pickup_month_of_year",
    "weather_temperature_c",
    "weather_relative_humidity_pct_imputed",
    "weather_precipitation_mm_imputed",
    "weather_wind_speed_kmh",
    "weather_pressure_hpa",
    "air_quality_pm25_mean_imputed",
]

BINARY_COLS = [
    "is_weekend",
    "air_quality_missing",
]

CYCLICAL_COLS = [
    "pickup_hour_sin",
    "pickup_hour_cos",
    "pickup_dow_sin",
    "pickup_dow_cos",
]

CATEGORICAL_INPUT_COLS = [
    "pickup_zone_cat",
    "dropoff_zone_cat",
    "pickup_borough_cat",
    "dropoff_borough_cat",
    "vendor_id_cat",
    "rate_code_id_cat",
    "store_and_fwd_flag_cat",
    "weather_condition_code_cat",
]
CATEGORICAL_INDEX_COLS = [f"{column}_index" for column in CATEGORICAL_INPUT_COLS]
CATEGORICAL_VECTOR_COLS = [f"{column}_vector" for column in CATEGORICAL_INPUT_COLS]

_PREPARE_CATEGORIES_SQL = """
    SELECT
        *,
        COALESCE(pickup_zone, 'Unknown') AS pickup_zone_cat,
        COALESCE(dropoff_zone, 'Unknown') AS dropoff_zone_cat,
        COALESCE(pickup_borough, 'Unknown') AS pickup_borough_cat,
        COALESCE(dropoff_borough, 'Unknown') AS dropoff_borough_cat,
        COALESCE(CAST(vendor_id AS STRING), 'Unknown') AS vendor_id_cat,
        COALESCE(CAST(rate_code_id AS STRING), 'Unknown') AS rate_code_id_cat,
        COALESCE(store_and_fwd_flag, 'Unknown') AS store_and_fwd_flag_cat,
        COALESCE(
            CAST(weather_condition_code AS STRING),
            'Unknown'
        ) AS weather_condition_code_cat
    FROM __THIS__
"""


def build_feature_pipeline() -> Pipeline:
    """Return the unfitted seven-stage fare feature pipeline."""
    prepare_categories = SQLTransformer(statement=_PREPARE_CATEGORIES_SQL)

    numeric_imputer = Imputer(
        strategy="median",
        inputCols=IMPUTER_INPUT_COLS,
        outputCols=IMPUTER_OUTPUT_COLS,
    )

    # handleInvalid="keep" gives unseen Validation/Test categories a fallback bucket.
    category_indexer = StringIndexer(
        inputCols=CATEGORICAL_INPUT_COLS,
        outputCols=CATEGORICAL_INDEX_COLS,
        handleInvalid="keep",
        stringOrderType="alphabetAsc",
    )

    # dropLast=True avoids a redundant dummy column for linear regression.
    category_encoder = OneHotEncoder(
        inputCols=CATEGORICAL_INDEX_COLS,
        outputCols=CATEGORICAL_VECTOR_COLS,
        handleInvalid="keep",
        dropLast=True,
    )

    continuous_assembler = VectorAssembler(
        inputCols=CONTINUOUS_NUMERIC_COLS,
        outputCol="continuous_features_raw",
        handleInvalid="keep",
    )

    # Interquartile-range scaling is robust to extreme distances and fares.
    continuous_scaler = RobustScaler(
        inputCol="continuous_features_raw",
        outputCol="continuous_features_scaled",
        lower=0.25,
        upper=0.75,
        withCentering=False,
        withScaling=True,
    )

    final_assembler = VectorAssembler(
        inputCols=[
            "continuous_features_scaled",
            *BINARY_COLS,
            *CYCLICAL_COLS,
            *CATEGORICAL_VECTOR_COLS,
        ],
        outputCol="features",
        handleInvalid="keep",
    )

    return Pipeline(
        stages=[
            prepare_categories,
            numeric_imputer,
            category_indexer,
            category_encoder,
            continuous_assembler,
            continuous_scaler,
            final_assembler,
        ]
    )
