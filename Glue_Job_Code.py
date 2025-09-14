import sys
import re
import concurrent.futures
from typing import List, Dict, Any
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality


class GroupFilter:
    """Represents a filter group for routing data."""

    def __init__(self, name: str, filters):
        self.name = name
        self.filters = filters


class GlueETLJob:
    """Main ETL job class for processing movie data with data quality checks."""

    def __init__(self):
        """Initialize the Glue ETL job with resolved arguments."""
        self.args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.args['JOB_NAME'], self.args)

    @staticmethod
    def apply_group_filter(source_dyf, group: GroupFilter):
        """Apply filter to a dynamic frame."""
        return Filter.apply(frame=source_dyf, f=group.filters)

    @staticmethod
    def threaded_route(glue_ctx, source_dyf, group_filters: List[GroupFilter]) -> DynamicFrameCollection:
        """
        Route data to different paths using threaded filtering.

        Args:
            glue_ctx: Glue context
            source_dyf: Source dynamic frame
            group_filters: List of GroupFilter objects

        Returns:
            DynamicFrameCollection with filtered results
        """
        dynamic_frames = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_filter = {
                executor.submit(GlueETLJob.apply_group_filter, source_dyf, gf): gf
                for gf in group_filters
            }

            for future in concurrent.futures.as_completed(future_to_filter):
                gf = future_to_filter[future]
                try:
                    result = future.result()
                    dynamic_frames[gf.name] = result
                except Exception as e:
                    print(f'Filter {gf.name} generated an exception: {e}')
                    # Consider logging to CloudWatch or raising if critical

        return DynamicFrameCollection(dynamic_frames, glue_ctx)

    def load_source_data(self):
        """Load source data from S3 catalog."""
        print("Loading source data from S3...")
        return self.glue_context.create_dynamic_frame.from_catalog(
            database="movies-db",
            table_name="movies_rawinput_data",
            transformation_ctx="MoviesFromS3"
        )

    def evaluate_data_quality(self, source_frame):
        """Evaluate data quality using DQ rules."""
        print("Evaluating data quality...")

        dq_ruleset = """
            Rules = [
                ColumnValues "imdb_rating" between 8.5 and 10.3,
                IsComplete "imdb_rating",
                ColumnType "imdb_rating" = "Double",
                ColumnType "meta_score" = "Long",
                ColumnType "no_of_votes" = "Long"
            ]
        """

        return EvaluateDataQuality().process_rows(
            frame=source_frame,
            ruleset=dq_ruleset,
            publishing_options={
                "dataQualityEvaluationContext": "MoviesDataQualityCheck",
                "enableDataQualityCloudWatchMetrics": True,
                "enableDataQualityResultsPublishing": True
            },
            additional_options={
                "observations.scope": "ALL",
                "performanceTuning.caching": "CACHE_NOTHING"
            }
        )

    def route_records(self, row_level_outcomes):
        """Route records based on data quality outcomes."""
        print("Routing records based on DQ outcomes...")

        group_filters = [
            GroupFilter(
                name="failed_records",
                filters=lambda row: bool(re.match("Failed", row["DataQualityEvaluationResult"]))
            ),
            GroupFilter(
                name="passed_records",
                filters=lambda row: not bool(re.match("Failed", row["DataQualityEvaluationResult"]))
            )
        ]

        return self.threaded_route(
            self.glue_context,
            source_dyf=row_level_outcomes,
            group_filters=group_filters
        )

    def transform_passed_records(self, passed_records):
        """Apply schema mapping to passed records."""
        print("Transforming passed records...")

        column_mappings = [
            ("poster_link", "string", "poster_link", "string"),
            ("series_title", "string", "series_title", "string"),
            ("released_year", "string", "released_year", "string"),
            ("certificate", "string", "certificate", "string"),
            ("runtime", "string", "runtime", "string"),
            ("genre", "string", "genre", "string"),
            ("imdb_rating", "double", "imdb_rating", "double"),
            ("overview", "string", "overview", "string"),
            ("meta_score", "long", "meta_score", "long"),
            ("director", "string", "director", "string"),
            ("star1", "string", "star1", "string"),
            ("star2", "string", "star2", "string"),
            ("star3", "string", "star3", "string"),
            ("star4", "string", "star4", "string"),
            ("no_of_votes", "long", "no_of_votes", "long"),
            ("gross", "string", "gross", "string")
        ]

        return ApplyMapping.apply(
            frame=passed_records,
            mappings=column_mappings,
            transformation_ctx="SchemaTransformation"
        )

    def write_rule_outcomes(self, rule_outcomes):
        """Write rule outcomes to S3."""
        print("Writing rule outcomes to S3...")
        self.glue_context.write_dynamic_frame.from_options(
            frame=rule_outcomes,
            connection_type="s3",
            format="json",
            connection_options={
                "path": "s3://movies-data-yb/rule_outcome_from_etl/",
                "partitionKeys": []
            },
            transformation_ctx="RuleOutcomesS3"
        )

    def write_failed_records(self, failed_records):
        """Write failed records to S3."""
        print("Writing failed records to S3...")
        self.glue_context.write_dynamic_frame.from_options(
            frame=failed_records,
            connection_type="s3",
            format="json",
            connection_options={
                "path": "s3://movies-data-yb/bad_records/",
                "partitionKeys": []
            },
            transformation_ctx="FailedRecordsS3"
        )

    def write_to_redshift(self, transformed_data):
        """Write transformed data to Redshift."""
        print("Writing to Redshift...")
        self.glue_context.write_dynamic_frame.from_catalog(
            frame=transformed_data,
            database="movies-db",
            table_name="dev_movies_imdb_movies_rating",
            redshift_tmp_dir="s3://redshift-temp-yb",
            additional_options={
                "aws_iam_role": "arn:aws:iam::010526265053:role/service-role/AmazonRedshift-CommandsAccessRole-20240806T210859"
            },
            transformation_ctx="RedshiftTarget"
        )

    def run(self):
        """Execute the complete ETL pipeline."""
        try:
            # Extract
            source_data = self.load_source_data()

            # Data Quality Check
            dq_results = self.evaluate_data_quality(source_data)

            # Split results
            rule_outcomes = SelectFromCollection.apply(
                dfc=dq_results,
                key="ruleOutcomes",
                transformation_ctx="RuleOutcomes"
            )

            row_level_outcomes = SelectFromCollection.apply(
                dfc=dq_results,
                key="rowLevelOutcomes",
                transformation_ctx="RowLevelOutcomes"
            )

            # Route records
            routed_data = self.route_records(row_level_outcomes)

            failed_records = SelectFromCollection.apply(
                dfc=routed_data,
                key="failed_records",
                transformation_ctx="FailedRecords"
            )

            passed_records = SelectFromCollection.apply(
                dfc=routed_data,
                key="passed_records",
                transformation_ctx="PassedRecords"
            )

            # Transform
            transformed_data = self.transform_passed_records(passed_records)

            # Load
            self.write_rule_outcomes(rule_outcomes)
            self.write_failed_records(failed_records)
            self.write_to_redshift(transformed_data)

            print("ETL job completed successfully!")

        except Exception as e:
            print(f"ETL job failed with error: {e}")
            raise
        finally:
            self.job.commit()


if __name__ == "__main__":
    # Run the ETL job
    etl_job = GlueETLJob()
    etl_job.run()