import streamlit as st
import os
import openai
import psycopg2
from psycopg2.pgvector import register_vector
from collections import Counter

# --- Configuration and Backend Functions ---
# This section contains all the necessary setup and logic from your existing files.
# Initialize the OpenAI client
client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# Database and Domain Configuration
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

def vector_search(embedding: list[float], conn) -> list:
    """Performs a vector similarity search and returns the top 15 matches."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT column_name, description
            FROM public.column_embeddings
            ORDER BY embedding <=> %s
            LIMIT 20;
        """, (embedding,))
        return cursor.fetchall()

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
                register_vector(conn)
                
                # 1. Generate descriptions from the user input
                query_descriptions = reformulate_for_query(user_input)
                
                results_from_all_descriptions = []
                if query_descriptions:
                    # 2. Get embeddings and perform vector search for each description
                    for desc in query_descriptions:
                        embedding = get_embedding(desc)
                        retrieved_columns = vector_search(embedding, conn)
                        results_from_all_descriptions.append(retrieved_columns)
                

                # 3. Aggregate results using the interleaved ranking logic
                if results_from_all_descriptions:
                    interleaved_results = []
                    # Find the length of the longest result list to set the loop range
                    max_len = max(len(res) for res in results_from_all_descriptions)
                    
                    # Loop rank by rank (i=0 is rank 1, i=1 is rank 2, etc.)
                    for i in range(max_len):
                        # Loop through each description's result list
                        for result_list in results_from_all_descriptions:
                            # Check if the current rank 'i' exists in the list
                            if i < len(result_list):
                                # Add the result only if it's not already in our final list
                                if result_list[i] not in interleaved_results:
                                    interleaved_results.append(result_list[i])
                                # Stop as soon as we have 15 unique results
                                if len(interleaved_results) >= 15:
                                    break
                        if len(interleaved_results) >= 15:
                            break
                    
                    # Display the newly ranked results
                    with results_container:
                        st.success("Search complete! Here are the top 15 most relevant columns found:")
                        
                        # Create headers for the results table
                        header_col1, header_col2, header_col3 = st.columns([1, 4, 8])
                        header_col1.write("**Rank**")
                        header_col2.write("**Column Name**")
                        header_col3.write("**Description Snippet**")

                        # Display each result from the interleaved list
                        for i, (col_name, col_desc) in enumerate(interleaved_results, 1):
                            res_col1, res_col2, res_col3 = st.columns([1, 4, 8])
                            res_col1.write(f"**{i}**")
                            res_col2.write(col_name)
                            res_col3.write(col_desc[:80] + "...") # Show first 80 chars
                else:
                    with results_container:
                        st.error("No matching columns could be found for your query.")
                
                conn.close()
        else:
            st.warning("Please enter a query in the 'user_input' box.")

with col2:
    if st.button("Clear Results"):
        # A placeholder button that doesn't need to do anything in Streamlit,
        # as the app's state will naturally clear on the next search.

        st.info("Results will be cleared on the next search.")

















