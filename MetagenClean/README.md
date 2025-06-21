# MetagenClean

MetagenClean is a simple Streamlit web app for cleaning and processing metadata from GEO (Gene Expression Omnibus) datasets.  
You can input a GEO ID or upload a .soft/.txt file, and the app will:

- Normalize the "organism" field (e.g., "H. sapiens" → "Homo sapiens")
- Detect missing fields
- Let you download a clean CSV file