import os
import re
import tempfile
import PyPDF2
import pandas as pd
import streamlit as st
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

# Configuration
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB
MAX_WORKERS = 4  # Number of parallel processes
CHUNK_SIZE = 50  # Number of files to process at once

def extract_text_from_pdf(pdf_path):
    """Extract text content from a PDF file with error handling"""
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text() or ""  # Handle None returns
    except Exception as e:
        st.error(f"Error reading {os.path.basename(pdf_path)}: {str(e)}")
    return text

def extract_contact_info(text, filename):
    """Extract basic contact information from resume text"""
    # Extract email
    email = re.findall(r'[\w\.-]+@[\w\.-]+', text)
    email = email[0] if email else ""
    
    # Extract phone (various formats)
    phone = re.findall(r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', text)
    phone = phone[0] if phone else ""
    
    # Extract name - first non-empty line without special chars
    name = ""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if lines:
        for line in lines:
            if (not any(char.isdigit() for char in line) and 
                '@' not in line and 
                'http' not in line.lower() and 
                len(line.split()) <= 4):
                name = line.strip()
                break
    
    # Extract location (specific Indian cities)
    location = ""
    indian_cities = ['hyderabad', 'chennai', 'bangalore', 'pune', 'mumbai', 
                    'delhi', 'gurgaon', 'noida', 'kolkata', 'ahmedabad']
    match = re.search(r'\b(' + '|'.join(indian_cities) + r')\b', text, re.IGNORECASE)
    if match:
        location = match.group(0).title()
    
    return {
        'Name': name,
        'Phone': phone,
        'Email': email,
        'Location': location,
        'Filename': filename
    }

def process_resume_batch(resume_batch):
    """Process a batch of resumes in parallel"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_single_resume, resume_batch))
    return [r for r in results if r is not None]

def process_single_resume(uploaded_file):
    """Process a single resume file"""
    try:
        # Save to temp file to handle large files
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = tmp.name
        
        text = extract_text_from_pdf(tmp_path)
        if text:
            info = extract_contact_info(text, uploaded_file.name)
            os.unlink(tmp_path)  # Clean up temp file
            return info
    except Exception as e:
        st.error(f"Error processing {uploaded_file.name}: {str(e)}")
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return None

def create_excel_download_link(df):
    """Generate a link to download the DataFrame as an Excel file"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    return excel_data

def main():
    st.set_page_config(
        page_title="Large-Scale Resume Parser", 
        page_icon="📄", 
        layout="wide",
        menu_items={
            'About': "This application can process 500+ resumes (up to 10GB) efficiently."
        }
    )
    
    st.title("📄 Large-Scale Resume Parser")
    st.markdown("""
    Upload **500+ resumes** (up to 10GB) to extract:
    - Name, Phone, Email
    - Location (Hyderabad, Chennai, Bangalore, etc.)
    """)
    
    # File uploader with size limit
    uploaded_files = st.file_uploader(
        "Upload Resume PDFs", 
        type="pdf", 
        accept_multiple_files=True,
        help=f"Select multiple PDF files (up to {MAX_FILE_SIZE/1024/1024/1024:.0f}GB total)"
    )
    
    if uploaded_files:
        total_size = sum(f.size for f in uploaded_files)
        if total_size > MAX_FILE_SIZE:
            st.error(f"Total size {total_size/1024/1024/1024:.2f}GB exceeds {MAX_FILE_SIZE/1024/1024/1024:.0f}GB limit")
            return
            
        st.success(f"{len(uploaded_files)} files ({total_size/1024/1024:.2f}MB) uploaded successfully!")
        
        if st.button("Process Resumes", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            data = []
            
            # Process in chunks to manage memory
            for i in range(0, len(uploaded_files), CHUNK_SIZE):
                batch = uploaded_files[i:i + CHUNK_SIZE]
                status_text.text(f"Processing files {i+1} to {min(i+CHUNK_SIZE, len(uploaded_files))}...")
                
                batch_results = process_resume_batch(batch)
                data.extend(batch_results)
                
                progress = min((i + CHUNK_SIZE) / len(uploaded_files), 1.0)
                progress_bar.progress(progress)
            
            if data:
                df = pd.DataFrame(data)
                
                # Display summary
                st.subheader(f"Processed {len(data)} resumes")
                st.dataframe(df.head(100))  # Show first 100 rows
                
                # Download options
                st.subheader("Download Results")
                excel_data = create_excel_download_link(df)
                st.download_button(
                    label="Download as Excel",
                    data=excel_data,
                    file_name="resume_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Click to download all extracted data"
                )
                
                # Show stats
                st.info(f"""
                **Processing Complete**:
                - Total resumes processed: {len(data)}
                - With phone numbers: {len(df[df['Phone'] != ''])}
                - With emails: {len(df[df['Email'] != ''])}
                - With locations: {len(df[df['Location'] != ''])}
                """)
            else:
                st.warning("No valid data was extracted from the uploaded files.")

if __name__ == "__main__":
    main()
