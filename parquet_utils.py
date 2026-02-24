"""
Utility functions for reading partitioned parquet files
"""
from pathlib import Path
import dask.dataframe as dd


def read_parquet_safe(path: Path) -> dd.DataFrame:
    """
    Safely read parquet files from a directory.
    Handles both single files and partitioned directories.
    
    Args:
        path: Path to parquet file or directory
        
    Returns:
        Dask DataFrame
        
    Raises:
        FileNotFoundError: If path doesn't exist
        ValueError: If no valid parquet files found
    """
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    
    if path.is_dir():
        # Check if there are any parquet files
        parquet_files = list(path.glob("*.parquet"))
        if not parquet_files:
            raise ValueError(f"No parquet files found in {path}")
        
        # Try to read partitioned parquet files
        pattern = str(path / "*.parquet")
        try:
            return dd.read_parquet(pattern)
        except Exception as e:
            # If reading fails, try to read only valid files
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error reading all partitions, trying individual files: {e}")
            
            # Read each file individually and concatenate
            dfs = []
            for pf in parquet_files:
                try:
                    df = dd.read_parquet(str(pf))
                    dfs.append(df)
                except Exception as file_error:
                    logger.warning(f"Skipping corrupted file {pf}: {file_error}")
            
            if not dfs:
                raise ValueError(f"No valid parquet files could be read from {path}")
            
            return dd.concat(dfs, ignore_index=True)
    else:
        # Single file
        return dd.read_parquet(str(path))
