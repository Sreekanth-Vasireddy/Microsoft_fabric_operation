import json
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

def optimize_and_vacuum_single_table(table_path):
    try:
        print(f"Optimizing table: {table_path}")
        
        # Load the Delta table
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Optimize the table by compacting small files
        delta_table.optimize().executeCompaction()
        
        # Get the operation metrics after optimization
        history_after = delta_table.history().select("operationMetrics").collect()
        
        # Perform vacuum to clean up old files
        delta_table.vacuum()
        
        # Collect optimization statistics
        stats = {
            "table": table_path,
            "metrics_after": history_after[0]
        }
        
        print(f"Table {table_path} optimized and vacuumed successfully.")
        return stats
    
    except AnalysisException as e:
        print(f"Error optimizing table {table_path}: {e}")
        return {"table": table_path, "error": str(e)}

def optimize_and_vacuum_lakehouse_schema(lakehouse, schema):
    lakehouse_schema = f"{lakehouse}/{schema}"
    spark.catalog.setCurrentDatabase(schema)  
    tables = spark.catalog.listTables()
    results = []
    for table in tables:
        optimisation_table = f"{lakehouse_schema}/{table.name}"
        table_path = optimisation_table
        stats = optimize_and_vacuum_single_table(table_path)
        results.append(stats)
    
    # Serialize the output messages to JSON and send back to the pipeline
    output_json = json.dumps({"messages": results})
    notebook.exit(output_json)

# Example usage
lakehouse = "/path/to/lakehouse"
schema = "schema_name"
optimize_and_vacuum_lakehouse_schema(lakehouse, schema)
