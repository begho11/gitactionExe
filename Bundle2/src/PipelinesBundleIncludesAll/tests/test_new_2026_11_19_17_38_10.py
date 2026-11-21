"""Unit tests for the Bundle2_etl pipeline.

The pipeline loads samples.nyctaxi.trips into sample_trips_Bundle2 and
aggregates fares per pickup zip into sample_zones_Bundle2. These tests seed
the external source with controlled rows, run the real pipeline through the
TestPipeline API, and assert on the output tables.
"""

from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()

# External source table read by the pipeline (kept fully qualified).
SOURCE_TABLE = "samples.nyctaxi.trips"

# Pipeline datasets publish to the pipeline's default catalog/schema.
TRIPS_TABLE = "workspace.loko57347.sample_trips_bundle2"
ZONES_TABLE = "workspace.loko57347.sample_zones_bundle2"


def seed_source(test_spark, rows):
    """Replace the source table with the given (pickup_zip, fare_amount) rows."""
    test_spark.sql(
        "CREATE OR REPLACE TABLE samples.nyctaxi.trips "
        "(pickup_zip INT, fare_amount DOUBLE)"
    )
    values = ", ".join(f"({zip_code}, {fare})" for zip_code, fare in rows)
    test_spark.sql(f"INSERT INTO samples.nyctaxi.trips VALUES {values}")


def test_trips_dataset_loads_all_source_rows(test_spark):
    # sample_trips_Bundle2 is a pass-through of the source: every seeded row
    # must appear in the output table.
    seed_source(test_spark, [(10001, 10.5), (10002, 5.0), (10003, 7.25)])

    status = test_pipeline.run(test_spark)
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    trips = test_spark.sql(f"SELECT * FROM {TRIPS_TABLE}")
    assert trips.count() == 3
    fares = {row.fare_amount for row in trips.collect()}
    assert fares == {10.5, 5.0, 7.25}


def test_zones_dataset_sums_fares_per_pickup_zip(test_spark):
    # sample_zones_Bundle2 must group by pickup_zip and total the fares,
    # merging rows that share a zip into a single aggregated row.
    seed_source(test_spark, [(10001, 10.5), (10001, 20.0), (10002, 5.0)])

    status = test_pipeline.run(test_spark)
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    zones = test_spark.sql(f"SELECT * FROM {ZONES_TABLE}")
    totals = {row.pickup_zip: row.total_fare for row in zones.collect()}
    assert totals == {10001: 30.5, 10002: 5.0}
