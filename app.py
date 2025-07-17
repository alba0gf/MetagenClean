import subprocess
import importlib.util

# Check if GEOparse is available, otherwise install it
if importlib.util.find_spec("GEOparse") is None:
    subprocess.run(["pip", "install", "GEOparse"], check=True)
import streamlit as st
import pandas as pd
import numpy as np
import io
import GEOparse
from organism_normalizer import OrganismNormalizer
from data_processor import DataProcessor
from utils import validate_geo_id, create_download_link

# Configure page
st.set_page_config(
    page_title="MetagenClean - GEO Data Processor",
    page_icon="🧬",
    layout="wide"
)

# Initialize session state
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'metadata' not in st.session_state:
    st.session_state.metadata = None
if 'expression_data' not in st.session_state:
    st.session_state.expression_data = None

# Initialize processors
@st.cache_resource
def get_organism_normalizer():
    return OrganismNormalizer()

@st.cache_resource
def get_data_processor():
    return DataProcessor()

organism_normalizer = get_organism_normalizer()
data_processor = get_data_processor()

# Main app
def main():
    st.title("🧬 MetagenClean")
    st.markdown("### GEO Metadata and Expression Data Cleaning Tool")
    st.markdown("Process and clean Gene Expression Omnibus `(GEO)` datasets with organism normalization and missing data detection.")
    
    # Sidebar for input options
    st.sidebar.header("Data Input Options")
    input_method = st.sidebar.radio(
        "Choose input method:",
        ["GEO ID", "Upload File"]
    )
    
    # Main content area
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Input Configuration")
        
        if input_method == "GEO ID":
            geo_id = st.text_input(
                "Enter GEO ID (e.g., GDS507, GSE1234):",
                placeholder="GDS507"
            )
            
            if st.button("Process GEO Dataset", type="primary"):
                if geo_id:
                    if validate_geo_id(geo_id):
                        process_geo_id(geo_id)
                    else:
                        st.error("Invalid GEO ID format. Please use format like GDS507 or GSE1234")
                else:
                    st.error("Please enter a GEO ID")
                    
        else:  # Upload File
            uploaded_file = st.file_uploader(
                "Upload GEO file (.soft or .txt):",
                type=['soft', 'txt'],
                help="Upload a .soft or .txt file downloaded from GEO"
            )
            
            if uploaded_file is not None:
                if st.button("Process Uploaded File", type="primary"):
                    process_uploaded_file(uploaded_file)
    
    with col2:
        st.subheader("Processing Status")
        status_container = st.container()
        
        # Display processing results
        if st.session_state.processed_data is not None:
            display_results()

def process_geo_id(geo_id):
    """Process GEO dataset by ID"""
    try:
        with st.spinner(f"Fetching GEO dataset {geo_id}..."):
            # Download and parse GEO data
            gse = GEOparse.get_GEO(geo=geo_id, destdir="./temp/")
            
            # Extract metadata and expression data
            metadata, expression_data = data_processor.extract_geo_data(gse)
            
            # Process the data
            process_extracted_data(metadata, expression_data)
            
    except Exception as e:
        st.error(f"Error processing GEO ID {geo_id}: {str(e)}")
        st.error("Please check if the GEO ID exists and is publicly available.")

def process_uploaded_file(uploaded_file):
    """Process uploaded GEO file"""
    try:
        with st.spinner("Processing uploaded file..."):
            # Save uploaded file temporarily
            file_content = uploaded_file.getvalue()
            
            # Parse the file content
            metadata, expression_data = data_processor.parse_geo_file(file_content, uploaded_file.name)
            
            # Process the data
            process_extracted_data(metadata, expression_data)
            
    except Exception as e:
        st.error(f"Error processing uploaded file: {str(e)}")
        st.error("Please ensure the file is a valid GEO .soft or .txt file.")

def process_extracted_data(metadata, expression_data):
    """Process extracted metadata and expression data"""
    try:
        with st.spinner("Cleaning and normalizing data..."):
            # Clean metadata
            cleaned_metadata = data_processor.clean_metadata(metadata)
            
            # Normalize organism names
            cleaned_metadata = organism_normalizer.normalize_metadata(cleaned_metadata)
            
            # Clean expression data
            cleaned_expression = data_processor.clean_expression_data(expression_data)
            
            # Detect missing data
            missing_report = data_processor.generate_missing_data_report(
                cleaned_metadata, cleaned_expression
            )
            
            # Store in session state
            st.session_state.metadata = cleaned_metadata
            st.session_state.expression_data = cleaned_expression
            st.session_state.processed_data = {
                'metadata': cleaned_metadata,
                'expression_data': cleaned_expression,
                'missing_report': missing_report
            }
            
            st.success("Data processing completed successfully!")
            
    except Exception as e:
        st.error(f"Error during data processing: {str(e)}")

def display_results():
    """Display processing results"""
    if st.session_state.processed_data is None:
        return
    
    data = st.session_state.processed_data
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Summary", "🗂️ Metadata", "📈 Expression Data", "⚠️ Missing Data"])
    
    with tab1:
        st.subheader("Dataset Summary")
        
        # Display summary statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Metadata Records", len(data['metadata']))
        
        with col2:
            if data['expression_data'] is not None:
                st.metric("Expression Features", data['expression_data'].shape[1] if len(data['expression_data'].shape) > 1 else len(data['expression_data']))
        
        with col3:
            missing_count = data['missing_report']['total_missing']
            st.metric("Missing Values", missing_count)
        
        # Organism distribution
        if 'organism' in data['metadata'].columns:
            st.subheader("Organism Distribution")
            organism_counts = data['metadata']['organism'].value_counts()
            st.bar_chart(organism_counts)
            
            
    with tab2:
        st.subheader("Cleaned Metadata")
        
        # Add search functionality
        search_term = st.text_input("Search metadata:", placeholder="Enter search term...")
        
        # Display metadata with filtering
        display_df = data['metadata'].copy()
        if search_term:
            import re  # Asegúrate de importar re solo una vez al principio del archivo si ya no está
            display_df = display_df[
                display_df.astype(str).apply(
                    lambda x: x.str.contains(re.escape(search_term), case=False, na=False)
                ).any(axis=1)
            ]
            st.dataframe(display_df, use_container_width=True, height=400)
        
        # Download button for metadata
        if st.button("Download Metadata as CSV"):
            csv_data = data['metadata'].to_csv(index=False)
            st.download_button(
                label="📥 Download Metadata CSV",
                data=csv_data,
                file_name="cleaned_metadata.csv",
                mime="text/csv"
            )
            # feedback
        st.markdown("---")
        st.subheader("💬 Help us improve")

        with st.form("feedback_form"):
            st.write("Have feedback or feature requests?")
            feedback = st.text_area("Let us know what worked, what didn't, or what you'd like to see.", height=150)
            submitted = st.form_submit_button("Submit Feedback")
            
            if submitted:
                with open("feedback_log.txt", "a", encoding="utf-8") as f:
                    f.write(f"{feedback}\n---\n")
                st.success("Thanks for your feedback! 🙌")
    
    with tab3:
        st.subheader("Expression Data Preview")
        
        if data['expression_data'] is not None:
            # Show first few rows and columns
            st.write("Data shape:", data['expression_data'].shape)
            st.dataframe(
                data['expression_data'].head(100),
                use_container_width=True,
                height=400
            )
            
            # Download button for expression data
            if st.button("Download Expression Data as CSV"):
                csv_data = data['expression_data'].to_csv(index=True)
                st.download_button(
                    label="📥 Download Expression Data CSV",
                    data=csv_data,
                    file_name="cleaned_expression_data.csv",
                    mime="text/csv"
                )
            
        else:
            st.info("No expression data available for this dataset.")
            
              # feedback
        st.markdown("---")
        st.subheader("💬 Help us improve")

        with st.form("feedback_form_2"):
            st.write("Have feedback or feature requests?")
            feedback = st.text_area("Let us know what worked, what didn't, or what you'd like to see.", height=150)
            submitted = st.form_submit_button("Submit Feedback")
            
            if submitted:
                with open("feedback_log.txt", "a", encoding="utf-8") as f:
                    f.write(f"{feedback}\n---\n")
                st.success("Thanks for your feedback! 🙌")
    
    with tab4:
        st.subheader("Missing Data Report")
        
        missing_report = data['missing_report']
        
        # Overall statistics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Missing Values", missing_report['total_missing'])
        with col2:
            st.metric("Completion Rate", f"{missing_report['completion_rate']:.1f}%")
        
        # Missing data by column
        if missing_report['missing_by_column']:
            st.subheader("Missing Values by Column")
            missing_df = pd.DataFrame([
                {'Column': col, 'Missing Count': count, 'Missing %': (count/len(data['metadata']))*100}
                for col, count in missing_report['missing_by_column'].items()
                if count > 0
            ])
            
            if not missing_df.empty:
                st.dataframe(missing_df, use_container_width=True)
                
                # Visualization
                st.bar_chart(missing_df.set_index('Column')['Missing Count'])
            else:
                st.success("No missing values detected in metadata!")
                
        
        # Data quality recommendations
        st.subheader("Data Quality Recommendations")
        recommendations = data_processor.generate_quality_recommendations(missing_report)
        for rec in recommendations:
            st.info(rec)

if __name__ == "__main__":
    main()
