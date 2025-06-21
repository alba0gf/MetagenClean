import pandas as pd
import re
from typing import Dict, List, Optional

class OrganismNormalizer:
    """
    Class for normalizing organism names in GEO metadata
    """
    
    def __init__(self):
        """Initialize the organism normalizer with common mappings"""
        self.organism_mappings = {
            # Human variants
            'human': 'Homo sapiens',
            'h. sapiens': 'Homo sapiens',
            'homo sapiens': 'Homo sapiens',
            'h.sapiens': 'Homo sapiens',
            'homo_sapiens': 'Homo sapiens',
            'hsa': 'Homo sapiens',
            'hsapiens': 'Homo sapiens',
            
            # Mouse variants
            'mouse': 'Mus musculus',
            'm. musculus': 'Mus musculus',
            'mus musculus': 'Mus musculus',
            'm.musculus': 'Mus musculus',
            'mus_musculus': 'Mus musculus',
            'mmu': 'Mus musculus',
            'mmusculus': 'Mus musculus',
            
            # Rat variants
            'rat': 'Rattus norvegicus',
            'r. norvegicus': 'Rattus norvegicus',
            'rattus norvegicus': 'Rattus norvegicus',
            'r.norvegicus': 'Rattus norvegicus',
            'rattus_norvegicus': 'Rattus norvegicus',
            'rno': 'Rattus norvegicus',
            'rnorvegicus': 'Rattus norvegicus',
            
            # Zebrafish variants
            'zebrafish': 'Danio rerio',
            'd. rerio': 'Danio rerio',
            'danio rerio': 'Danio rerio',
            'd.rerio': 'Danio rerio',
            'danio_rerio': 'Danio rerio',
            'dre': 'Danio rerio',
            'drerio': 'Danio rerio',
            
            # Fruit fly variants
            'fruit fly': 'Drosophila melanogaster',
            'fruitfly': 'Drosophila melanogaster',
            'd. melanogaster': 'Drosophila melanogaster',
            'drosophila melanogaster': 'Drosophila melanogaster',
            'd.melanogaster': 'Drosophila melanogaster',
            'drosophila_melanogaster': 'Drosophila melanogaster',
            'dme': 'Drosophila melanogaster',
            'dmelanogaster': 'Drosophila melanogaster',
            
            # C. elegans variants
            'c. elegans': 'Caenorhabditis elegans',
            'caenorhabditis elegans': 'Caenorhabditis elegans',
            'c.elegans': 'Caenorhabditis elegans',
            'caenorhabditis_elegans': 'Caenorhabditis elegans',
            'cel': 'Caenorhabditis elegans',
            'celegans': 'Caenorhabditis elegans',
            'worm': 'Caenorhabditis elegans',
            
            # Yeast variants
            'yeast': 'Saccharomyces cerevisiae',
            's. cerevisiae': 'Saccharomyces cerevisiae',
            'saccharomyces cerevisiae': 'Saccharomyces cerevisiae',
            's.cerevisiae': 'Saccharomyces cerevisiae',
            'saccharomyces_cerevisiae': 'Saccharomyces cerevisiae',
            'sce': 'Saccharomyces cerevisiae',
            'scerevisiae': 'Saccharomyces cerevisiae',
            
            # Arabidopsis variants
            'arabidopsis': 'Arabidopsis thaliana',
            'a. thaliana': 'Arabidopsis thaliana',
            'arabidopsis thaliana': 'Arabidopsis thaliana',
            'a.thaliana': 'Arabidopsis thaliana',
            'arabidopsis_thaliana': 'Arabidopsis thaliana',
            'ath': 'Arabidopsis thaliana',
            'athaliana': 'Arabidopsis thaliana',
        }
        
        # Common organism patterns for more flexible matching
        self.organism_patterns = [
            (r'\b(homo\s+sapiens|h\.\s*sapiens|human)\b', 'Homo sapiens'),
            (r'\b(mus\s+musculus|m\.\s*musculus|mouse)\b', 'Mus musculus'),
            (r'\b(rattus\s+norvegicus|r\.\s*norvegicus|rat)\b', 'Rattus norvegicus'),
            (r'\b(danio\s+rerio|d\.\s*rerio|zebrafish)\b', 'Danio rerio'),
            (r'\b(drosophila\s+melanogaster|d\.\s*melanogaster|fruit\s*fly)\b', 'Drosophila melanogaster'),
            (r'\b(caenorhabditis\s+elegans|c\.\s*elegans|worm)\b', 'Caenorhabditis elegans'),
            (r'\b(saccharomyces\s+cerevisiae|s\.\s*cerevisiae|yeast)\b', 'Saccharomyces cerevisiae'),
            (r'\b(arabidopsis\s+thaliana|a\.\s*thaliana|arabidopsis)\b', 'Arabidopsis thaliana'),
        ]
    
    def normalize_organism_name(self, organism: str) -> str:
        """
        Normalize a single organism name
        
        Args:
            organism (str): Raw organism name
            
        Returns:
            str: Normalized organism name
        """
        if pd.isna(organism) or organism == '':
            return organism
        
        # Convert to string and clean
        organism_clean = str(organism).strip().lower()
        
        # Remove extra whitespace and special characters
        organism_clean = re.sub(r'\s+', ' ', organism_clean)
        organism_clean = re.sub(r'[^\w\s\.]', ' ', organism_clean)
        organism_clean = organism_clean.strip()
        
        # Direct mapping lookup
        if organism_clean in self.organism_mappings:
            return self.organism_mappings[organism_clean]
        
        # Pattern-based matching
        for pattern, normalized_name in self.organism_patterns:
            if re.search(pattern, organism_clean, re.IGNORECASE):
                return normalized_name
        
        # If no match found, return original with proper capitalization
        return self._capitalize_organism_name(organism)
    
    def _capitalize_organism_name(self, organism: str) -> str:
        """
        Apply proper capitalization to organism names
        
        Args:
            organism (str): Organism name
            
        Returns:
            str: Properly capitalized organism name
        """
        if pd.isna(organism) or organism == '':
            return organism
        
        # Split into words
        words = str(organism).strip().split()
        
        # Capitalize first letter of each word, except for certain particles
        capitalized_words = []
        for i, word in enumerate(words):
            if i == 0 or word.lower() not in ['sp.', 'var.', 'subsp.', 'cf.']:
                capitalized_words.append(word.capitalize())
            else:
                capitalized_words.append(word.lower())
        
        return ' '.join(capitalized_words)
    
    def normalize_metadata(self, metadata: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize organism names in metadata DataFrame
        
        Args:
            metadata (pd.DataFrame): Metadata DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with normalized organism names
        """
        df = metadata.copy()
        
        # Find organism-related columns
        organism_columns = self._find_organism_columns(df)
        
        # Normalize each organism column
        for col in organism_columns:
            if col in df.columns:
                df[col] = df[col].apply(self.normalize_organism_name)
        
        return df
    
    def _find_organism_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Find columns that likely contain organism information
        
        Args:
            df (pd.DataFrame): Metadata DataFrame
            
        Returns:
            List[str]: List of column names that likely contain organism data
        """
        organism_keywords = [
            'organism', 'species', 'taxonomy', 'taxon', 'organism_ch1',
            'source_name_ch1', 'characteristics_ch1', 'organism_part'
        ]
        
        found_columns = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in organism_keywords):
                found_columns.append(col)
        
        return found_columns
    
    def get_organism_statistics(self, metadata: pd.DataFrame) -> Dict:
        """
        Get statistics about organisms in the metadata
        
        Args:
            metadata (pd.DataFrame): Metadata DataFrame
            
        Returns:
            Dict: Statistics about organisms
        """
        organism_columns = self._find_organism_columns(metadata)
        
        if not organism_columns:
            return {'error': 'No organism columns found'}
        
        stats = {}
        
        for col in organism_columns:
            if col in metadata.columns:
                value_counts = metadata[col].value_counts()
                stats[col] = {
                    'unique_count': len(value_counts),
                    'most_common': value_counts.head(10).to_dict(),
                    'missing_count': metadata[col].isna().sum()
                }
        
        return stats
    
    def add_custom_mapping(self, from_name: str, to_name: str):
        """
        Add a custom organism name mapping
        
        Args:
            from_name (str): Source organism name
            to_name (str): Target normalized organism name
        """
        self.organism_mappings[from_name.lower().strip()] = to_name
    
    def get_supported_organisms(self) -> List[str]:
        """
        Get list of supported normalized organism names
        
        Returns:
            List[str]: List of normalized organism names
        """
        return list(set(self.organism_mappings.values()))
