import re
import pandas as pd
import base64
from typing import Optional

def validate_geo_id(geo_id: str) -> bool:
    """
    Validate GEO ID format
    
    Args:
        geo_id (str): GEO ID to validate
        
    Returns:
        bool: True if valid GEO ID format
    """
    if not geo_id:
        return False
    
    # Common GEO ID patterns
    patterns = [
        r'^GDS\d+$',  # GDS (GEO DataSet)
        r'^GSE\d+$',  # GSE (GEO Series)
        r'^GPL\d+$',  # GPL (GEO Platform)
        r'^GSM\d+$'   # GSM (GEO Sample)
    ]
    
    geo_id = geo_id.strip().upper()
    
    for pattern in patterns:
        if re.match(pattern, geo_id):
            return True
    
    return False

def create_download_link(df: pd.DataFrame, filename: str, link_text: str) -> str:
    """
    Create a download link for a DataFrame
    
    Args:
        df (pd.DataFrame): DataFrame to download
        filename (str): Name for the downloaded file
        link_text (str): Text to display for the link
        
    Returns:
        str: HTML download link
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{link_text}</a>'
    return href

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human readable format
    
    Args:
        size_bytes (int): Size in bytes
        
    Returns:
        str: Formatted size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe file operations
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Remove or replace unsafe characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove multiple underscores
    filename = re.sub(r'_+', '_', filename)
    
    # Remove leading/trailing underscores and dots
    filename = filename.strip('_.')
    
    # Ensure filename is not empty
    if not filename:
        filename = "file"
    
    return filename

def detect_delimiter(sample_text: str) -> str:
    """
    Detect the delimiter used in a text file
    
    Args:
        sample_text (str): Sample text from the file
        
    Returns:
        str: Detected delimiter
    """
    # Common delimiters to check
    delimiters = ['\t', ',', ';', '|']
    
    lines = sample_text.split('\n')[:5]  # Check first 5 lines
    
    delimiter_counts = {}
    for delimiter in delimiters:
        count = sum(line.count(delimiter) for line in lines)
        delimiter_counts[delimiter] = count
    
    # Return the delimiter with the highest count
    if delimiter_counts:
        return max(delimiter_counts, key=delimiter_counts.get)
    
    return '\t'  # Default to tab

def is_numeric_column(series: pd.Series) -> bool:
    """
    Check if a pandas Series contains mostly numeric data
    
    Args:
        series (pd.Series): Series to check
        
    Returns:
        bool: True if mostly numeric
    """
    if series.empty:
        return False
    
    # Try converting to numeric
    numeric_series = pd.to_numeric(series, errors='coerce')
    
    # Calculate percentage of non-null numeric values
    non_null_count = numeric_series.notna().sum()
    total_count = len(series)
    
    # Consider it numeric if >80% of values can be converted
    return (non_null_count / total_count) > 0.8 if total_count > 0 else False

def clean_column_name(column_name: str) -> str:
    """
    Clean and standardize column names
    
    Args:
        column_name (str): Original column name
        
    Returns:
        str: Cleaned column name
    """
    if not column_name:
        return "unnamed_column"
    
    # Convert to string and strip whitespace
    name = str(column_name).strip()
    
    # Replace spaces and special characters with underscores
    name = re.sub(r'[^\w]', '_', name)
    
    # Remove multiple underscores
    name = re.sub(r'_+', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    # Convert to lowercase
    name = name.lower()
    
    # Ensure it's not empty
    if not name:
        name = "unnamed_column"
    
    return name

def get_memory_usage(df: pd.DataFrame) -> dict:
    """
    Get memory usage information for a DataFrame
    
    Args:
        df (pd.DataFrame): DataFrame to analyze
        
    Returns:
        dict: Memory usage information
    """
    memory_usage = df.memory_usage(deep=True)
    total_memory = memory_usage.sum()
    
    return {
        'total_memory_bytes': total_memory,
        'total_memory_mb': total_memory / (1024 * 1024),
        'memory_per_column': memory_usage.to_dict(),
        'shape': df.shape,
        'dtypes': df.dtypes.to_dict()
    }
