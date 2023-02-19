import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


AmazonS3_node1676723967170 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://lrpostgradudeflights/airports/airports.parquet"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1676723967170",
)


S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://lrpostgradudeflights/cities/cities.parquet"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)


AmazonS3_node1676723915520 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://lrpostgradudeflights/departures/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1676723915520",
)


ChangeSchemaApplyMapping_node1676723993080 = ApplyMapping.apply(
    frame=AmazonS3_node1676723967170,
    mappings=[
        ("GMT", "string", "GMT_Airport", "string"),
        ("airportId", "string", "airportId", "string"),
        ("codeIataAirport", "string", "codeIataAirport", "string"),
        ("codeIataCity", "string", "codeIataCity_Airport", "string"),
        ("codeIcaoAirport", "string", "codeIcaoAirport", "string"),
        ("codeIso2Country", "string", "codeIso2Country_Airport", "string"),
        ("geonameId", "string", "geonameId_Airport", "string"),
        ("latitudeAirport", "double", "latitudeAirport", "double"),
        ("longitudeAirport", "double", "longitudeAirport", "double"),
        ("nameAirport", "string", "nameAirport", "string"),
        ("nameCountry", "string", "nameCountry_Airport", "string"),
        ("phone", "string", "phone_Airport", "string"),
        ("timezone", "string", "timezone_Airport", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1676723993080",
)


ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("GMT", "string", "GMT_City", "string"),
        ("cityId", "string", "cityId", "string"),
        ("codeIataCity", "string", "codeIataCity", "string"),
        ("codeIso2Country", "string", "codeIso2Country_City", "string"),
        ("geonameId", "bigint", "geonameId_City", "long"),
        ("latitudeCity", "double", "latitudeCity", "double"),
        ("longitudeCity", "double", "longitudeCity", "double"),
        ("nameCity", "string", "nameCity", "string"),
        ("timezone", "string", "timezone_City", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)


ChangeSchemaApplyMapping_node1676723990600 = ApplyMapping.apply(
    frame=AmazonS3_node1676723915520,
    mappings=[
        ("flight_type", "string", "flight_type", "string"),
        ("status", "string", "status", "string"),
        ("departure_iataCode", "string", "departure_iataCode", "string"),
        ("departure_icaoCode", "string", "departure_icaoCode", "string"),
        ("departure_terminal", "string", "departure_terminal", "string"),
        ("departure_gate", "string", "departure_gate", "string"),
        ("departure_delay", "double", "departure_delay", "double"),
        (
            "departure_scheduledTime",
            "timestamp",
            "departure_scheduledTime",
            "timestamp",
        ),
        (
            "departure_estimatedTime",
            "timestamp",
            "departure_estimatedTime",
            "timestamp",
        ),
        ("departure_actualTime", "timestamp", "departure_actualTime", "timestamp"),
        (
            "departure_estimatedRunway",
            "timestamp",
            "departure_estimatedRunway",
            "timestamp",
        ),
        ("departure_actualRunway", "timestamp", "departure_actualRunway", "timestamp"),
        ("arrival_iataCode", "string", "arrival_iataCode", "string"),
        ("arrival_icaoCode", "string", "arrival_icaoCode", "string"),
        ("arrival_terminal", "string", "arrival_terminal", "string"),
        ("arrival_baggage", "string", "arrival_baggage", "string"),
        ("arrival_gate", "string", "arrival_gate", "string"),
        ("arrival_delay", "double", "arrival_delay", "double"),
        ("arrival_scheduledTime", "timestamp", "arrival_scheduledTime", "timestamp"),
        ("arrival_estimatedTime", "timestamp", "arrival_estimatedTime", "timestamp"),
        ("arrival_actualTime", "timestamp", "arrival_actualTime", "timestamp"),
        (
            "arrival_estimatedRunawy",
            "timestamp",
            "arrival_estimatedRunawy",
            "timestamp",
        ),
        ("arrival_actualRunway", "timestamp", "arrival_actualRunway", "timestamp"),
        ("airline_name", "string", "airline_name", "string"),
        ("airline_iataCode", "string", "airline_iataCode", "string"),
        ("airline_icaoCode", "string", "airline_icaoCode", "string"),
        ("flight_number", "string", "flight_number", "string"),
        ("flight_iataNumber", "string", "flight_iataNumber", "string"),
        ("flight_icaoNumber", "string", "flight_icaoNumber", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1676723990600",
)


ChangeSchemaApplyMapping_node1676723990600DF = (
    ChangeSchemaApplyMapping_node1676723990600.toDF()
)
ChangeSchemaApplyMapping_node1676723993080DF = (
    ChangeSchemaApplyMapping_node1676723993080.toDF()
)
Join_node1676724056508 = DynamicFrame.fromDF(
    ChangeSchemaApplyMapping_node1676723990600DF.join(
        ChangeSchemaApplyMapping_node1676723993080DF,
        (
            ChangeSchemaApplyMapping_node1676723990600DF["arrival_iataCode"]
            == ChangeSchemaApplyMapping_node1676723993080DF["codeIataAirport"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1676724056508",
)


Join_node1676724056508DF = Join_node1676724056508.toDF()
ApplyMapping_node2DF = ApplyMapping_node2.toDF()
Join_node1676724144410 = DynamicFrame.fromDF(
    Join_node1676724056508DF.join(
        ApplyMapping_node2DF,
        (
            Join_node1676724056508DF["codeIataCity_Airport"]
            == ApplyMapping_node2DF["codeIataCity"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1676724144410",
)


AmazonS3_node1676724193415 = glueContext.getSink(
    path="s3://lrpostgradudeflights/full_departures/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1676724193415",
)
AmazonS3_node1676724193415.setCatalogInfo(
    catalogDatabase="departures_full", catalogTableName="departures_full"
)
AmazonS3_node1676724193415.setFormat("glueparquet")
AmazonS3_node1676724193415.writeFrame(Join_node1676724144410)
job.commit()
