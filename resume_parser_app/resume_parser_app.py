import os
import re
import PyPDF2
import pandas as pd
import streamlit as st
from io import BytesIO

def extract_text_from_pdf(pdf_file):
    """Extract text content from a PDF file"""
    text = ""
    try:
        reader = PyPDF2.PdfReader(pdf_file)
        for page in reader.pages:
            text += page.extract_text()
    except Exception as e:
        st.error(f"Error reading PDF: {str(e)}")
    return text

def extract_contact_info(text, filename):
    """Extract basic contact information from resume text"""
    # Extract email
    email = re.findall(r'[\w\.-]+@[\w\.-]+', text)
    email = email[0] if email else ""
    
    # Extract phone (various formats)
    phone = re.findall(r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]', text)
    phone = phone[0] if phone else ""
    
    # Extract name - improved approach
    name = ""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    if lines:
        potential_names = []
        for line in lines:
            if not any(char.isdigit() for char in line) and \
               '@' not in line and \
               'http' not in line.lower() and \
               len(line.split()) >= 1 and len(line.split()) <= 4:
                potential_names.append(line)
        
        if potential_names:
            name = potential_names[0]
            name = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Prof\.?)\s*', '', name, flags=re.IGNORECASE)
            name = name.strip()
    
    # Extract years of experience
    experience = "N/A"
    exp_patterns = [
        r'(\d+)\+?\s*(years?|yrs?)\s*(of)?\s*(experience|exp)',
        r'experience\s*:\s*(\d+)\s*(years?|yrs?)',
        r'(\d+)\s*-\s*(\d+)\s*years?\s*experience'
    ]
    for pattern in exp_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            experience = match.group(1) if match.group(1) else "1"
            break
    
    # Extract location (only specific Indian cities)
    location = ""
    indian_cities = ['hyderabad', 'chennai', 'bangalore', 'pune', 'mumbai', 'delhi', 
                    'gurgaon', 'noida', 'kolkata', 'ahmedabad']
    location_pattern = r'\b(' + '|'.join(indian_cities) + r')\b'
    match = re.search(location_pattern, text, re.IGNORECASE)
    if match:
        location = match.group(0).title()  # Capitalize first letter
    
    return {
        'Name': name,
        'Phone': phone,
        'Email': email,
        'Years of Experience': experience,
        'Location': location if location else "",  # Blank if no matching city found
        'Filename': filename
    }

def process_resumes(uploaded_files):
    """Process multiple PDF files"""
    data = []
    for uploaded_file in uploaded_files:
        text = extract_text_from_pdf(uploaded_file)
        if text:
            info = extract_contact_info(text, uploaded_file.name)
            data.append(info)
    return data

def create_excel_download_link(df):
    """Generate a link to download the DataFrame as an Excel file"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    return excel_data

def main():
    st.set_page_config(page_title="Basic Resume Parser", page_icon="📄", layout="wide")
    
    st.title("📄 Basic Resume Parser")
    st.markdown("""
    Upload multiple resume PDFs to extract:
    - Name, Phone, Email
    - Years of Experience
    - Location (only if Hyderabad, Chennai, Bangalore, Pune, etc.)
    """)
    
    # File uploader
    uploaded_files = st.file_uploader(
        "Upload Resume PDFs", 
        type="pdf", 
        accept_multiple_files=True,
        help="Select multiple PDF files containing resumes"
    )
    
    if uploaded_files:
        st.success(f"{len(uploaded_files)} file(s) uploaded successfully!")
        
        # Process files when button is clicked
        if st.button("Process Resumes", type="primary"):
            with st.spinner("Extracting information from resumes..."):
                data = process_resumes(uploaded_files)
                
            if data:
                df = pd.DataFrame(data)
                
                # Display results
                st.subheader("Extracted Data")
                st.dataframe(df)
                
                # Download button
                st.subheader("Download Results")
                excel_data = create_excel_download_link(df)
                st.download_button(
                    label="Download as Excel",
                    data=excel_data,
                    file_name="resume_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Click to download the extracted data as an Excel file"
                )
            else:
                st.warning("No information could be extracted from the uploaded files.")

if __name__ == "__main__":
    main()