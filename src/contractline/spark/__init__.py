"""Block implementations for the Spark engine family (ADR-0010).

Shared by the local Spark + Delta adapter and the Databricks adapter. Nothing
here may import an adapter: blocks receive DataFrames and settings, and return
DataFrames.
"""
