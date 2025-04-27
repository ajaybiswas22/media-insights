import yaml
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType, LongType
)

class DDLGenerator:
    TYPE_MAPPING = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType()
    }

    def __init__(self, yaml_content):
        if isinstance(yaml_content, str):
            self.data = yaml.safe_load(yaml_content)
        else:
            self.data = yaml_content

    def _parse_schema(self, schema_dict):
        fields = []
        for field_name, field_info in schema_dict.items():
            data_type_str = field_info["type"]
            data_type = self.TYPE_MAPPING.get(data_type_str.lower())
            if not data_type:
                raise ValueError(f"Unsupported data type: {data_type_str} for field {field_name}")
            fields.append(StructField(field_name, data_type, True))
        return StructType(fields)

    def get_table_info(self, table_name):
        table_info = self.data.get(table_name)
        if not table_info:
            raise ValueError(f"Table '{table_name}' not found in YAML.")

        schema_dict = table_info.get("schema")
        struct_type = self._parse_schema(schema_dict)
        cluster_by_cols = table_info.get("clusterBy", [])
        partition_by_cols = table_info.get("partitionBy", [])
        description = table_info.get("description", "")
        location = table_info.get("location", "")

        return {
            "schema": struct_type,
            "cluster_by": cluster_by_cols,
            "partition_by": partition_by_cols,
            "description": description,
            "location": location
        }
    
    def generate_create_table_sql(self, table_name, database="default"):
        info = self.get_table_info(table_name)
        fields_sql = ",\n\t".join(
            [f"{field.name} {field.dataType.simpleString()}" for field in info["schema"].fields]
        )

        cluster_clause = ""
        if info["cluster_by"]:
            cluster_cols = ", ".join(info["cluster_by"])
            cluster_clause = f"\nCLUSTER BY ({cluster_cols})"

        partition_clause = ""
        if info["partition_by"]:
            cluster_cols = ", ".join(info["partition_by"])
            partition_clause = f"\nPARTITION BY ({cluster_cols})"

        location_clause = ""
        if info["location"]:
            location_clause = f"\nLOCATION '{info['location']}'"

        # Adding optional TBLPROPERTIES to encourage Liquid Clustering
        return f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name} (
        {fields_sql}
        )
        USING DELTA
        {partition_clause}{cluster_clause}{location_clause};
        """.strip()
    
    def generate_all_create_table_sql(self, database="default"):
        queries = []
        for table_name in self.data.keys():
            query = self.generate_create_table_sql(table_name, database=database)
            queries.append(query)
        return queries




