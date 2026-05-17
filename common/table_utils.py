def get_source_table(source_name: str, table_name: str) -> str:
    """
    Construct the fully qualified source table name.
    
    Args:
        source_name: Name of the source schema (e.g., 'bakehouse', 'wanderbricks')
        table_name: Name of the table in the source schema
    
    Returns:
        str: Fully qualified source table name (e.g., 'samples.bakehouse.sales_customers')
    """
    return f"samples.{source_name}.{table_name}"


def get_target_table(catalog: str, zone: str, source_name: str, table_name: str) -> str:
    """
    Construct the fully qualified target table name.
    
    Args:
        catalog: Target catalog name (e.g., 'workspace')
        zone: Target zone/schema name (e.g., 'bronze_raw')
        source_name: Name of the source (e.g., 'bakehouse', 'wanderbricks')
        table_name: Name of the table
    
    Returns:
        str: Fully qualified target table name (e.g., 'workspace.bronze_raw.bakehouse_sales_customers')
    """
    return f"{catalog}.{zone}.{source_name}_{table_name}"
