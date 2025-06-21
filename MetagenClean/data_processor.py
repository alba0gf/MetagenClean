import pandas as pd
import numpy as np
import GEOparse
import io
import re
from typing import Dict, List, Tuple, Optional, Any

class DataProcessor:
    """
    Class for processing and cleaning GEO data
    """
    
    def __init__(self):
        """Initialize the data processor"""
        self.missing_indicators = [
            '', 'null', 'NULL', 'na', 'NA', 'n/a', 'N/A', 'none', 'None', 'NONE',
            'missing', 'Missing', 'MISSING', '-', '--', '---', 'undefined',
            'Undefined', 'UNDEFINED', 'unknown', 'Unknown', 'UNKNOWN'
        ]
    
    def extract_geo_data(self, geo_obj) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Extract metadata and expression data from GEO object
        
        Args:
            geo_obj: GEOparse GDS or GSE object
            
        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: metadata and expression data
        """
        metadata_list = []
        expression_data = None
        
        try:
            # Check if it's a GDS (DataSet) or GSE (Series) object
            if hasattr(geo_obj, 'subsets'):
                # This is a GDS object
                return self._extract_gds_data(geo_obj)
            elif hasattr(geo_obj, 'gsms'):
                # This is a GSE object
                return self._extract_gse_data(geo_obj)
            else:
                raise Exception("Unknown GEO object type")
                
        except Exception as e:
            raise Exception(f"Error extracting GEO data: {e}")
    
    def _extract_gds_data(self, gds) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Extract data from GDS (GEO DataSet) object
        
        Args:
            gds: GEOparse GDS object
            
        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: metadata and expression data
        """
        metadata_list = []
        
        # Extract subset information (samples)
        for subset_name, subset in gds.subsets.items():
            sample_data = {
                'sample_id': subset_name,
                'title': subset.metadata.get('description', [''])[0],
                'organism': gds.metadata.get('sample_organism', [''])[0],
                'source_name': subset.metadata.get('type', [''])[0],
                'characteristics': subset.metadata.get('description', [''])[0],
                'dataset_id': gds.name,
                'platform_id': gds.metadata.get('platform', [''])[0],
                'submission_date': gds.metadata.get('date', [''])[0],
                'description': ' | '.join(gds.metadata.get('summary', [])),
                'contact_name': ' | '.join(gds.metadata.get('contributor', [])),
                'dataset_type': gds.metadata.get('type', [''])[0],
                'value_type': gds.metadata.get('value_type', [''])[0],
                'reference_series': gds.metadata.get('reference_series', [''])[0],
                'pubmed_id': ' | '.join(gds.metadata.get('pubmed_id', []))
            }
            
            # Add additional metadata
            for key, value in subset.metadata.items():
                if key not in sample_data:
                    if isinstance(value, list):
                        sample_data[key] = ' | '.join(value) if value else ''
                    else:
                        sample_data[key] = str(value) if value else ''
            
            metadata_list.append(sample_data)
        
        metadata_df = pd.DataFrame(metadata_list)
        
        # Extract expression data from GDS table
        expression_data = None
        try:
            if hasattr(gds, 'table') and gds.table is not None:
                expression_data = gds.table.copy()
                # Set ID_REF as index if it exists
                if 'ID_REF' in expression_data.columns:
                    expression_data = expression_data.set_index('ID_REF')
        except Exception as e:
            print(f"Warning: Could not extract expression data from GDS: {e}")
        
        return metadata_df, expression_data
    
    def _extract_gse_data(self, gse) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Extract data from GSE (GEO Series) object
        
        Args:
            gse: GEOparse GSE object
            
        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: metadata and expression data
        """
        metadata_list = []
        
        # Extract sample metadata
        for gsm_name, gsm in gse.gsms.items():
            sample_data = {
                'sample_id': gsm_name,
                'title': gsm.metadata.get('title', [''])[0],
                'organism': gsm.metadata.get('organism_ch1', [''])[0],
                'source_name': gsm.metadata.get('source_name_ch1', [''])[0],
                'characteristics': ' | '.join(gsm.metadata.get('characteristics_ch1', [])),
                'treatment': gsm.metadata.get('treatment_protocol_ch1', [''])[0],
                'growth_protocol': gsm.metadata.get('growth_protocol_ch1', [''])[0],
                'extract_protocol': gsm.metadata.get('extract_protocol_ch1', [''])[0],
                'label_protocol': gsm.metadata.get('label_protocol_ch1', [''])[0],
                'hyb_protocol': gsm.metadata.get('hyb_protocol', [''])[0],
                'scan_protocol': gsm.metadata.get('scan_protocol', [''])[0],
                'description': gsm.metadata.get('description', [''])[0],
                'data_processing': ' | '.join(gsm.metadata.get('data_processing', [])),
                'platform_id': gsm.metadata.get('platform_id', [''])[0],
                'contact_name': gsm.metadata.get('contact_name', [''])[0],
                'contact_email': gsm.metadata.get('contact_email', [''])[0],
                'contact_institute': gsm.metadata.get('contact_institute', [''])[0],
                'submission_date': gsm.metadata.get('submission_date', [''])[0],
                'last_update_date': gsm.metadata.get('last_update_date', [''])[0]
            }
            
            # Add any additional characteristics
            for key, value in gsm.metadata.items():
                if key not in sample_data and isinstance(value, list):
                    sample_data[key] = ' | '.join(value) if value else ''
                elif key not in sample_data:
                    sample_data[key] = str(value) if value else ''
            
            metadata_list.append(sample_data)
        
        metadata_df = pd.DataFrame(metadata_list)
        
        # Extract expression data if available
        expression_data = None
        try:
            if hasattr(gse, 'pivot_samples'):
                expression_data = gse.pivot_samples('VALUE')
            elif len(gse.gsms) > 0:
                # Try to extract expression data from individual samples
                sample_tables = []
                for gsm_name, gsm in gse.gsms.items():
                    if hasattr(gsm, 'table') and gsm.table is not None:
                        sample_table = gsm.table.copy()
                        if 'VALUE' in sample_table.columns:
                            sample_table = sample_table.set_index('ID_REF')['VALUE']
                            sample_table.name = gsm_name
                            sample_tables.append(sample_table)
                
                if sample_tables:
                    expression_data = pd.concat(sample_tables, axis=1)
                    
        except Exception as e:
            print(f"Warning: Could not extract expression data: {e}")
            expression_data = None
        
        return metadata_df, expression_data
    
    def parse_geo_file(self, file_content: bytes, filename: str) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Parse GEO file content
        
        Args:
            file_content (bytes): File content
            filename (str): Original filename
            
        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: metadata and expression data
        """
        try:
            # Convert bytes to string
            content = file_content.decode('utf-8')
            
            # Create a temporary file-like object
            temp_file = io.StringIO(content)
            
            # Parse the content based on file type
            if filename.endswith('.soft'):
                return self._parse_soft_file(content)
            elif filename.endswith('.txt'):
                return self._parse_txt_file(content)
            else:
                raise ValueError("Unsupported file format. Please use .soft or .txt files.")
                
        except Exception as e:
            raise Exception(f"Error parsing file {filename}: {e}")
    
    def _parse_soft_file(self, content: str) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Parse SOFT file format
        
        Args:
            content (str): File content
            
        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: metadata and expression data
        """
        lines = content.split('\n')
        metadata_list = []
        current_sample = {}
        in_sample_section = False
        expression_data = None
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('^SAMPLE'):
                if current_sample:
                    metadata_list.append(current_sample)
                current_sample = {'sample_id': line.split('=')[1].strip()}
                in_sample_section = True
                
            elif line.startswith('!Sample_'):
                if in_sample_section:
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        key = parts[0].replace('!Sample_', '').lower()
                        value = parts[1].strip()
                        current_sample[key] = value
            
            elif line.startswith('!sample_table_begin'):
                # Skip expression data for now in SOFT files
                break
        
        if current_sample:
            metadata_list.append(current_sample)
        
        metadata_df = pd.DataFrame(metadata_list) if metadata_list else pd.DataFrame()
        
        return metadata_df, expression_data
    
    def _parse_txt_file(self, content: str) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Parse TXT file format (assuming tab-delimited)
        
        Args:
            content (str): File content
            
        Returns:
            Tuple[pd.DataFrame, Optional[pd.DataFrame]]: metadata and expression data
        """
        try:
            # Try to read as tab-delimited file
            df = pd.read_csv(io.StringIO(content), sep='\t', low_memory=False)
            
            # Determine if this is metadata or expression data
            if 'sample_id' in df.columns.str.lower() or any('gsm' in col.lower() for col in df.columns):
                # Likely metadata
                return df, None
            else:
                # Likely expression data, create minimal metadata
                sample_columns = [col for col in df.columns if col != df.columns[0]]
                metadata_list = [{'sample_id': col, 'source': 'uploaded_file'} for col in sample_columns]
                metadata_df = pd.DataFrame(metadata_list)
                
                # Set first column as index for expression data
                expression_data = df.set_index(df.columns[0])
                
                return metadata_df, expression_data
                
        except Exception as e:
            raise Exception(f"Error parsing TXT file: {e}")
    
    def clean_metadata(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """
        Clean metadata DataFrame
        
        Args:
            metadata (pd.DataFrame): Raw metadata
            
        Returns:
            pd.DataFrame: Cleaned metadata
        """
        df = metadata.copy()
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Handle missing values
        df = self._handle_missing_values(df)
        
        # Clean text fields
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            df[col] = df[col].apply(self._clean_text_field)
        
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Remove duplicate rows
        df = df.drop_duplicates()
        
        return df
    
    def clean_expression_data(self, expression_data: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        Clean expression data DataFrame
        
        Args:
            expression_data (Optional[pd.DataFrame]): Raw expression data
            
        Returns:
            Optional[pd.DataFrame]: Cleaned expression data
        """
        if expression_data is None:
            return None
        
        df = expression_data.copy()
        
        # Convert to numeric where possible
        numeric_columns = []
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                numeric_columns.append(col)
            except:
                pass
        
        # Remove rows with all NaN values
        df = df.dropna(how='all')
        
        # Remove columns with all NaN values
        df = df.dropna(axis=1, how='all')
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in DataFrame
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized missing values
        """
        # Replace common missing value indicators with NaN
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(self.missing_indicators, np.nan)
        
        return df
    
    def _clean_text_field(self, text: Any) -> str:
        """
        Clean individual text field
        
        Args:
            text: Input text
            
        Returns:
            str: Cleaned text
        """
        if pd.isna(text):
            return text
        
        text = str(text).strip()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        return text
    
    def generate_missing_data_report(self, metadata: pd.DataFrame, 
                                   expression_data: Optional[pd.DataFrame]) -> Dict:
        """
        Generate comprehensive missing data report
        
        Args:
            metadata (pd.DataFrame): Metadata DataFrame
            expression_data (Optional[pd.DataFrame]): Expression data DataFrame
            
        Returns:
            Dict: Missing data report
        """
        report = {
            'metadata_missing': {},
            'expression_missing': {},
            'total_missing': 0,
            'completion_rate': 0,
            'missing_by_column': {}
        }
        
        # Analyze metadata missing values
        if not metadata.empty:
            metadata_missing = metadata.isnull().sum()
            metadata_total = len(metadata) * len(metadata.columns)
            metadata_missing_total = metadata.isnull().sum().sum()
            
            report['metadata_missing'] = {
                'total_cells': metadata_total,
                'missing_cells': metadata_missing_total,
                'missing_percentage': (metadata_missing_total / metadata_total) * 100 if metadata_total > 0 else 0,
                'by_column': metadata_missing.to_dict()
            }
            
            report['missing_by_column'].update(metadata_missing.to_dict())
            report['total_missing'] += metadata_missing_total
        
        # Analyze expression data missing values
        if expression_data is not None and not expression_data.empty:
            expression_missing = expression_data.isnull().sum()
            expression_total = len(expression_data) * len(expression_data.columns)
            expression_missing_total = expression_data.isnull().sum().sum()
            
            report['expression_missing'] = {
                'total_cells': expression_total,
                'missing_cells': expression_missing_total,
                'missing_percentage': (expression_missing_total / expression_total) * 100 if expression_total > 0 else 0,
                'by_column': expression_missing.head(20).to_dict()  # Limit to first 20 columns
            }
            
            report['total_missing'] += expression_missing_total
        
        # Calculate overall completion rate
        total_cells = report['metadata_missing'].get('total_cells', 0) + report['expression_missing'].get('total_cells', 0)
        if total_cells > 0:
            report['completion_rate'] = ((total_cells - report['total_missing']) / total_cells) * 100
        
        return report
    
    def generate_quality_recommendations(self, missing_report: Dict) -> List[str]:
        """
        Generate data quality recommendations based on missing data report
        
        Args:
            missing_report (Dict): Missing data report
            
        Returns:
            List[str]: List of recommendations
        """
        recommendations = []
        
        completion_rate = missing_report.get('completion_rate', 0)
        
        if completion_rate >= 95:
            recommendations.append("✅ Excellent data quality! Missing data is minimal.")
        elif completion_rate >= 85:
            recommendations.append("✅ Good data quality. Some missing values present but manageable.")
        elif completion_rate >= 70:
            recommendations.append("⚠️ Moderate data quality. Consider imputation strategies for missing values.")
        else:
            recommendations.append("❌ Poor data quality. Significant missing data may impact analysis.")
        
        # Specific recommendations based on missing data patterns
        missing_by_column = missing_report.get('missing_by_column', {})
        
        high_missing_columns = [col for col, count in missing_by_column.items() if count > 0]
        if high_missing_columns:
            recommendations.append(f"📊 Columns with missing data: {', '.join(high_missing_columns[:5])}")
            
            if len(high_missing_columns) > 5:
                recommendations.append(f"📊 And {len(high_missing_columns) - 5} more columns with missing data.")
        
        # Organism-specific recommendations
        if any('organism' in col.lower() for col in missing_by_column.keys()):
            recommendations.append("🧬 Organism information has missing values. Consider manual curation for better analysis.")
        
        return recommendations
