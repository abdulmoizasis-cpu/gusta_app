
import streamlit as st
import os
import openai
import psycopg2
from collections import Counter
import re
from dotenv import load_dotenv
import inflect
from urllib.parse import urlparse
import nltk
from metadata_filter import *
import ssl

def download_nltk_data():
    """
    Downloads the 'wordnet' and 'omw-1.4' corpora for NLTK if they are not already present.
    Includes a workaround for potential SSL certificate issues.
    """
    try:
        # Create an unverified SSL context to handle potential certificate errors
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context

    # Check for and download 'wordnet' and 'omw-1.4'
    try:
        # This will raise a LookupError if the resource is not found
        nltk.data.find('corpora/wordnet.zip')
        nltk.data.find('corpora/omw-1.4.zip')
        print("NLTK corpora are already downloaded.")
    except LookupError:
        print("One or more NLTK corpora not found. Downloading...")
        # If either is missing, download both for simplicity
        nltk.download('wordnet')
        nltk.download('omw-1.4')
        print("✅ NLTK corpora downloaded.")

    # Check for and download 'omw-1.4'
    try:
        nltk.data.find('corpora/omw-1.4.zip')
    except nltk.downloader.DownloadError:
        print("Downloading NLTK 'omw-1.4' corpus...")
        nltk.download('omw-1.4')
        print("✅ 'omw-1.4' downloaded.")

download_nltk_data()
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
# --- Configuration and Backend Functions ---
# This section contains all the necessary setup and logic from your existing files.
# Initialize the OpenAI client
client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

DOMAIN_NAME = "Pharmaceutical"
TABLE_DESCRIPTION = "This table contains information about companies and their service categories, including company details, service classifications, geographic location, and various analytical and manufacturing equipment."
ALL_COLUMNS = [
    "address - TextField - '26 Corporate Circle",
    "animals_housed - CharField - 'Transgenic Mouse'",
    "business_model - TextField - 'pharma/bio",
    "business_sector - CharField - 'Academics\\nAgrochemicals\\nBiopharmaceutical\\nBiosimilar/Biobetter\\nChemicals\\nCosmetics/Personal Care\\nGenerics\\nGovernment\\nNutraceuticals\\nVeterinary'",
    "city - CharField - 'Albany'",
    "company_id - IntegerField - 1766",
    "company_link - TextField - ",
    "company_name - CharField - 'Curia Inc.'",
    "company_name_hash - CharField - '9d53d1e503b0a4c9d28e47e513b3bc3d'",
    "company_profile_link - TextField - ",
    "company_summary - TextField - 'Curia Inc. is a contract research and manufacturing organization (CRMO) that provides a range of services to the pharmaceutical and biotechnology industries. It offers integrated solutions for drug discovery",
    "company_website - TextField - 'https://curiaglobal.com/'",
    "development_stage - CharField - 'Clinical\\nDiscovery\\nPhase 1\\nPhase 2\\nPhase 3\\nPreclinical'",
    "entity_hierarchy - CharField - 'Research Supplies|Life Sciences Supplies/Reagents|Imaging/Labeling Reagents|Labeling Reagents/Kits'",
    "entity_s - TextField - 'consultant\\nareas of expertise\\nbusiness development (corporate)\\ncorporate functions\\nstrategic consulting'",
    "facility_inspection - CharField - 'Biosafety level\\nBrazil (ANVISA)\\nBSL2\\nCertifications/Accreditations/Licenses\\nClean Rooms\\nCountry Regulatory/Accreditation\\nDEA\\nFacility Licenses\\nGLP Certification\\nGMP Certification\\nGMP-UK\\nGMP-USA\\nInspected\\nISO 13485:2016\\nISO 17025\\nISO 7 Clean Room (Class 10000)\\nISO 9001:2008\\nISO 9001:2015\\nISTA\\nItaly (AIFA/NSIS)\\nJapan (PMDA/MHLW)\\nLicensed/Registered\\nSouth Korea (KFDA/MFDS)\\nUnited Kingdom (MHRA)\\nUSA (FDA/ACVM)'",
    "latitude - CharField - '42.7032423'",
    "level_2 - CharField - 'Packaging Equipment'",
    "level_3 - CharField - 'Patient Access Solution'",
    "level_4 - CharField - 'Intrinsic Factor'",
    "level_5 - CharField - 'Protein-Protein Docking'",
    "level_6 - CharField - 'Polysaccharides'",
    "longitude - CharField - '-73.8745414'",
    "molecule_type - CharField - 'Antibody\\nBiological Molecules\\nCell Based\\nGene/DNA\\nmRNA\\nNucleic Acid Molecules\\nOligonucleotides\\nPeptide\\nProtein\\nSmall Molecules\\nViral Vectors'",
    "moment - DateTimeField - '2025-07-06T14:08:16'",
    "number_of_employees - FloatField - ",
    "parent_service - CharField - 'Supply Chain'",
    "private_public - CharField - 'public'",
    "process_equipment - TextField - '1D NMR Analysis\\n2D NMR Analysis\\nAgilent 8453 UV-Visible Spectrophotometers\\nAKTA Explorer\\nAnalytical Equipment\\nAPI Equipment\\nAPI Process Equipment\\nBioanalytical/Genomics Equipment\\nBiorad QX200\\nBioreactors API\\nC13 NMR\\nCalorimeter\\nCapillary Electrophoresis  (CE)\\nCapsule Filling/Equipment\\nCE-SDS\\nChiral Chromatography\\nChromatography Equipment\\ncIEF\\nClinical Chemistry/Immunoassay Analyzer Equipments\\nddPCR\\nDifferential Scanning Calorimetry (DSC)\\nDifferential Scanning Fluorimetry (DSF)\\nDot Blot Equipment\\nDry Granulation (Roller Compactor/Chilsonator)\\nDrying Process/Equipment\\nElectrophoresis\\nELISA Equipment\\nEnergy Dispersive X-Ray (EDX) Spectroscopy\\nF19 NMR\\nFluid Bed Coating\\nFluid Bed Dryer/Coater/Granulator\\nFluid Bed Drying\\nFormulation drying platforms\\nFPLC systems\\nFreeze Drying/Lyophilization Equipment\\nFTIR (FT-IR)\\nGenomics/Sequencing Equipment\\nGlass-Lined Reactors\\nGlass-Lined Reactors (=< 8000 L)\\nGranulation/Agglomeration Process\\nH1",
    "product_group - CharField - 'Antibiotic Products\\nAntibody Drug Conjugates (ADCs)\\nBioconjugates\\nBiologicals/Advanced Therapies\\nBiopharmaceutical Products\\nControlled Substances\\nCytotoxics\\nDrug Conjugates\\nHighly Potent Compounds\\nHormonal Products\\nNatural Products\\nSchedule I Drugs\\nSchedule II Drugs\\nSchedule III Drugs\\nSchedule IV Drugs\\nSchedule V Drugs\\nSterile API\\nSteroids\\nVaccine/Adjuvant'",
    "route - CharField - 'Inhalation\\nInjection\\nOphthalmic\\nOral\\nTopical'",
    "service_categories_id - AutoField - 13282",
    "service_category_id - IntegerField - 5749",
    "service_category_name - CharField - 'Staining Reagents'",
    "service_type - CharField - 'development stage'",
    "state - CharField - 'MA-Massachusetts'",
    "sub_service - CharField - 'Hydrochloric Acid'",
    "territory_name - CharField - 'China'",
    "therapeutic_category - CharField - 'Rare/Orphan Diseases'",
    "top_service - CharField - 'Clinical Trials'",
    "year_founded - IntegerField - 1991"
]

# Using Streamlit's cache to speed up the app by avoiding repeated API calls for the same input
@st.cache_data
def reformulate_for_query(user_input: str) -> list[str]:
    """Generates technical descriptions from a user query using the OpenAI API."""
    formatted_all_columns = "\n".join([f'"{item}"' for item in ALL_COLUMNS])
    
    # This is the template from your file, now inside the function
    reformulate_for_query_template = """
    Generate short, highly technical, domain-specific descriptions of the columns of a database table that most likely contain the information requested by the user using :
    1. user_input : A natural language user query
    2. table_description : A database table description.
    3. domain_name : The domain name of the database.
    4. all_columns : The names of all the columns in the table and their data types

    Methodology :
    1. {{common keywords and special terminology}} : Use common keywords and special terminology from the given domain based on your prior information of the domains keywords, the table description and the user input.
    2. {{data type}} and {{structure}} : Infer the data type from all_columns and infer the structure of data requested mentioning it explicitly and using it to guide the description to explain what it implies (e.g., integer/double columns as measurements, longtext/varchar columns as descriptive text/categorical labels/short text identifiers, datetime columns as temporal markers for timestamps/event times).
    3. {{represents}}  : Describe what the column data means and repesents in enough detail to uniquely identify what the column represents in context of the domain and table.
    4. {{column name}} : Analyse the column_names, their data types and sample item to identify the column name of the column most likely to contain the information requested by the user.
    5. {{relationship}} : Additionally analyse all the column names, their data types and sample item to determine other columns related to the current column and describe their relationship
    6. Combine 1-5 to generate a technical description of the next most relevant column as a single, detailed paragraph
    7. Repeat 6 for more columns potentially containing the information requested by the user in the order of most likely to least likely

    Output rules for a single description :
    1. Generate only a single clean paragraph of text and nothing else
    2. Do not format the paragraph
    3. Do not include JSON in the paragraph

    Output format for a single description :
    The column {{column name}} of type {{data type}} in {{structure}} describes {{represents}}, encompassing {{common keywords and special terminology}} and {{relationship}}

    Full output format :
    Description 1
    {{newline}}
    Description 2
    {{newline}}
    ...
    Description n

    Context :
    user_input : {user_input}
    table_description : {table_description}
    domain_name : {domain_name}
    all_columns : {all_columns}"""
    
    prompt = reformulate_for_query_template.format(
        user_input=user_input,
        table_description=TABLE_DESCRIPTION,
        domain_name=DOMAIN_NAME,
        all_columns=formatted_all_columns
    )
    
    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    descriptions_text = response.choices[0].message.content.strip()
    return [("The column " + desc.strip()) for desc in descriptions_text.split('The column') if desc.strip()]

@st.cache_data
def get_embedding(text: str) -> list[float]:
    """Generates a vector embedding for a given text string."""
    response = client.embeddings.create(model="text-embedding-3-small", input=text)
    return response.data[0].embedding

def vector_search(embedding: list[float], prefiltered_columns: list[str], conn) -> list:
    """Performs a vector similarity search and returns the top 15 matches."""
    if not prefiltered_columns :
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, description
                FROM public.column_embeddings
                ORDER BY embedding <=> %s
                LIMIT 20;
            """, (str(embedding),))
            return cursor.fetchall()
    else :
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, description
                FROM public.column_embeddings
                WHERE column_name = ANY(%s) -- This is the new filtering clause
                ORDER BY embedding <=> %s
                LIMIT 15;
            """, (prefiltered_columns, str(embedding)))
            return cursor.fetchall()

            

def get_keyword_match_counts(user_keywords: list[str], conn) -> dict:
    """
    Performs an exact-match keyword search and returns a dictionary mapping
    column names to their keyword match count.
    """
    if not user_keywords:
        return {}
    with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    column_name,
                    -- This part calculates the count of intersecting keywords
                    (SELECT COUNT(*)
                        FROM unnest(keywords) as k
                        WHERE k = ANY(%s)) as match_count,
                    keywords
                FROM
                    public.column_embeddings
                WHERE
                    keywords && %s
                ORDER BY
                    match_count DESC;
            """, (user_keywords, user_keywords))
            
            results = cursor.fetchall()
            return results
    
# --- The User Interface ---
st.title("Database Vector Search")
st.write("Enter a query to find the most relevant columns in the database.")

# Create the pre-filled text for the prompt context
# Display the context and provide a text area for the user input
user_input = st.text_input("Enter your user_input here:")

# Create a container to hold the results
results_container = st.container()

# Create columns for the Search and Clear buttons
col1, col2 = st.columns(2)

with col1:
    if st.button("Search"):
        if user_input:
            with st.spinner("Processing query... This may take a moment."):
                conn = psycopg2.connect(st.secrets["DB_URL"])
                
            user_keywords = extract_unique_words_advanced([user_input])
            # 2. Get the pre-filtered columns and their keyword match data
            keyword_matches_results = get_keyword_match_counts(user_keywords, conn)

            # Create a dictionary for easy lookup of match counts and db_keywords
            keyword_match_data = {
                column_name: {"count": match_count, "db_keywords": db_keywords}
                for column_name, match_count, db_keywords in keyword_matches_results
            }
            
            prefiltered_columns = list(keyword_match_data.keys())

            # 3. Generate technical descriptions for the vector search
            query_descriptions = reformulate_for_query(user_input)
            
            results_from_all_descriptions = []
            if query_descriptions:
                for desc in query_descriptions:
                    embedding = get_embedding(desc)
                    retrieved_columns = vector_search(embedding, prefiltered_columns, conn)
                    results_from_all_descriptions.append(retrieved_columns)

            # 5. Aggregate and rank the vector search results using the interleaved method
            if results_from_all_descriptions:
                interleaved_results = []
                max_len = max(len(res) for res in results_from_all_descriptions if res)
                
                for i in range(max_len):
                    for result_list in results_from_all_descriptions:
                        if i < len(result_list):
                            if result_list[i] not in interleaved_results:
                                interleaved_results.append(result_list[i])
                            if len(interleaved_results) >= 15:
                                break
                    if len(interleaved_results) >= 15:
                        break
            
                with results_container:
                    st.success("Search complete! Here are the most relevant columns found:")
                    
                    # 1. Create 5 columns for the headers with adjusted widths.
                    h_rank, h_col, h_desc, h_count, h_kwords = st.columns([1, 4, 8, 2, 3])
                    h_rank.write("**Rank**")
                    h_col.write("**Column Name**")
                    h_desc.write("**Description Snippet**")
                    h_count.write("**Match Count**")
                    h_kwords.write("**Keywords Matched**") # Use the correct variable for this header

                    # 2. Loop through the ranked vector search results.
                    for i, (col_name, col_desc) in enumerate(interleaved_results, 1):
                        # 3. Look up the keyword data for the current column.
                        match_info = keyword_match_data.get(col_name, {"count": 0, "db_keywords": []})
                        match_count = match_info["count"]
                        db_keywords = match_info["db_keywords"]
                        
                        # 4. Calculate the specific keywords that matched.
                        matched_keywords = list(set(user_keywords) & set(db_keywords))

                        # 5. Create 5 columns for the data row.
                        r_rank, r_col, r_desc, r_count, r_kwords = st.columns([1, 4, 8, 2, 3])
                        r_rank.write(f"**{i}**")
                        r_col.write(col_name)
                        r_desc.write(col_desc[:80] + "...")
                        r_count.write(f"**{match_count}**")
                        # 6. Display the list of matched keywords as a comma-separated string.
                        r_kwords.write(", ".join(matched_keywords))
            else:
                with results_container:
                    st.error("No matching columns could be found for your query.")
                
                conn.close()
        else:
            st.warning("Please enter a query in the 'user_input' box.")

with col2:
    if st.button("Clear Results"):
        st.info("Results will be cleared on the next search.")


