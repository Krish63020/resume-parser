import os
import re
import tempfile
import PyPDF2
import pandas as pd
import streamlit as st
from io import BytesIO, StringIO
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta

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
                text += page.extract_text() or ""
    except Exception as e:
        st.error(f"Error reading {os.path.basename(pdf_path)}: {str(e)}")
    return text

def calculate_experience(text):
    """Flexible experience calculation from multiple patterns"""
    # 1. Check for direct experience mentions
    exp_patterns = [
        r'(\d+)\+?\s*(?:years?|yrs?)\s*(?:of\s*)?experience',
        r'experience\s*:\s*(\d+)\s*(?:years?|yrs?)',
        r'(\d+)\s*(?:years?|yrs?)\s*experience',
        r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)\s*experience'
    ]
    
    for pattern in exp_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return f"{match.group(1)} years" if match.group(1) else "1 year"
    
    # 2. Calculate from work history dates (flexible format)
    date_pattern = r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*\d{4}|\d{1,2}/\d{4}|\d{4}\s*-\s*\d{4}'
    dates = re.findall(date_pattern, text, re.IGNORECASE)
    
    if len(dates) >= 2:
        try:
            date_objs = []
            for date_str in dates:
                try:
                    if '-' in date_str and len(date_str.split('-')) == 2:  # 2010-2014 format
                        start, end = date_str.split('-')
                        date_objs.append(datetime(int(start), 1, 1))
                        date_objs.append(datetime(int(end), 1, 1))
                    elif '/' in date_str:
                        month, year = date_str.split('/')
                        date_objs.append(datetime(int(year), int(month), 1))
                    else:
                        parts = date_str.split()
                        if parts:
                            month = parts[0][:3]
                            year = int(parts[-1]) if parts[-1].isdigit() else None
                            if year:
                                date_objs.append(datetime(year, datetime.strptime(month, '%b').month, 1))
                except:
                    continue
            
            if len(date_objs) >= 2:
                min_date = min(date_objs)
                max_date = max(date_objs)
                delta = relativedelta(max_date, min_date)
                if delta.years == 0:
                    return f"{delta.months} months"
                elif delta.months == 0:
                    return f"{delta.years} years"
                else:
                    return f"{delta.years} years {delta.months} months"
        except:
            pass
    
    # 3. Calculate from project durations (flexible format)
    project_pattern = r'(?:project|role|experience).*?(?:duration|period|time)\s*[:~-]?\s*(\d+)\s*(?:years?|yrs?)?\s*(\d+)?\s*(?:months?)?'
    match = re.search(project_pattern, text, re.IGNORECASE)
    if match:
        years = int(match.group(1)) if match.group(1) else 0
        months = int(match.group(2)) if match.group(2) else 0
        if years == 0:
            return f"{months} months"
        elif months == 0:
            return f"{years} years"
        else:
            return f"{years} years {months} months"
    
    return "Not specified"

def extract_highest_qualification(text):
    """Flexible degree extraction without institution names"""
    degree_patterns = [
        (r'\b(?:B\.?\s?Tech(?:nology)?|Bachelor\s*of\s*Tech(?:nology)?)(?:\s*in\s*[A-Za-z\s]+)?', "BTech"),
        (r'\b(?:M\.?\s?Tech(?:nology)?|Master\s*of\s*Tech(?:nology)?)(?:\s*in\s*[A-Za-z\s]+)?', "MTech"),
        (r'\b(?:B\.?\s?E\.?|Bachelor\s*of\s*Engineering)(?:\s*in\s*[A-Za-z\s]+)?', "BE"),
        (r'\b(?:M\.?\s?E\.?|Master\s*of\s*Engineering)(?:\s*in\s*[A-Za-z\s]+)?', "ME"),
        (r'\b(?:B\.?\s?Sc\.?|Bachelor\s*of\s*Science)(?:\s*in\s*[A-Za-z\s]+)?', "BSc"),
        (r'\b(?:M\.?\s?Sc\.?|Master\s*of\s*Science)(?:\s*in\s*[A-Za-z\s]+)?', "MSc"),
        (r'\b(?:B\.?\s?A\.?|Bachelor\s*of\s*Arts)(?:\s*in\s*[A-Za-z\s]+)?', "BA"),
        (r'\b(?:M\.?\s?A\.?|Master\s*of\s*Arts)(?:\s*in\s*[A-Za-z\s]+)?', "MA"),
        (r'\b(?:B\.?\s?Com\.?|Bachelor\s*of\s*Commerce)(?:\s*in\s*[A-Za-z\s]+)?', "BCom"),
        (r'\b(?:M\.?\s?Com\.?|Master\s*of\s*Commerce)(?:\s*in\s*[A-Za-z\s]+)?', "MCom"),
        (r'\b(?:Ph\.?\s?D\.?|Doctor\s*of\s*Philosophy)(?:\s*in\s*[A-Za-z\s]+)?', "PhD"),
        (r'\b(?:Post\s*Graduat(?:e|ion)|PG)(?:\s*in\s*[A-Za-z\s]+)?', "Post Graduation"),
        (r'\b(?:M\.?\s?B\.?\s?A\.?|Master\s*of\s*Business\s*Administration)(?:\s*in\s*[A-Za-z\s]+)?', "MBA"),
        (r'\b(?:B\.?\s?B\.?\s?A\.?|Bachelor\s*of\s*Business\s*Administration)(?:\s*in\s*[A-Za-z\s]+)?', "BBA"),
        (r'\b(?:Diploma)(?:\s*in\s*[A-Za-z\s]+)?', "Diploma"),
        (r'\b(?:Bachelor)(?:\s*of\s*[A-Za-z\s]+)?', "Bachelor"),
        (r'\b(?:Master)(?:\s*of\s*[A-Za-z\s]+)?', "Master"),
        (r'\b(?:Undergraduate)(?:\s*in\s*[A-Za-z\s]+)?', "Undergraduate"),
        (r'\b(?:High\s*School|Secondary\s*School)', "High School")
    ]
    
    for pattern, degree_type in degree_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Extract the full degree name with specialization if available
            degree = match.group(0).strip()
            # Clean up any extra spaces or punctuation
            degree = re.sub(r'\s+', ' ', degree)
            return degree
    
    return "Not specified"

def extract_skills(text):
    """Flexible skills extraction from various formats"""
    skills = {
        'Languages': [],
        'Database': [],
        'Concepts': [],
        'Tools & Frameworks': [],
        'Other Technical': [],
        'Soft Skills': []
    }
    
    # Try to find categorized skills first
    skill_categories = {
        'Languages': r'(?:Languages|Programming|Proficient in)\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)',
        'Database': r'(?:Database|DBMS|Databases)\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)',
        'Concepts': r'(?:Concepts|Knowledge|Principles)\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)',
        'Tools & Frameworks': r'(?:Tools|Frameworks|Technologies|Software)\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)',
        'Other Technical': r'(?:Other|Additional|Technical)\s*Skills?\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)',
        'Soft Skills': r'(?:Soft\s*Skills|Interpersonal|Communication)\s*Skills?\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)'
    }
    
    for category, pattern in skill_categories.items():
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            skills_text = match.group(1)
            # Extract individual skills with flexible separators
            extracted = re.split(r'[,•·;\n/]', skills_text)
            skills[category] = [
                skill.strip() for skill in extracted 
                if skill.strip() and len(skill.strip()) > 2
            ][:10]  # Limit to 10 per category
    
    # Also look for general skills section (more flexible pattern)
    general_skills = re.search(
        r'(?:Skills|Technical\s*Skills|Expertise|Competencies)\s*[:~-]?\s*(.*?)(?:\n\n|\n\s*\n|$)',
        text, re.IGNORECASE | re.DOTALL
    )
    if general_skills:
        skills_text = general_skills.group(1)
        # Auto-categorize general skills with flexible matching
        tech_keywords = {
            'Languages': ['python', 'java', 'c++', 'c#', 'javascript', 'html', 'css', 'sql'],
            'Database': ['mysql', 'mongodb', 'postgresql', 'oracle', 'sql server', 'nosql'],
            'Tools & Frameworks': ['git', 'docker', 'vscode', 'pycharm', 'jupyter', 'flask', 'django'],
            'Concepts': ['oop', 'data structures', 'algorithms', 'machine learning', 'ai'],
            'Soft Skills': ['communication', 'teamwork', 'leadership', 'problem solving']
        }
        
        for skill in re.split(r'[,•·;\n/]', skills_text):
            skill = skill.strip()
            if not skill or len(skill) < 2:
                continue
                
            # Check each category
            categorized = False
            for category, keywords in tech_keywords.items():
                if any(keyword in skill.lower() for keyword in keywords):
                    skills[category].append(skill)
                    categorized = True
                    break
            
            if not categorized:
                # Check if it looks like a technical skill
                if any(char.isupper() for char in skill) or any(word.isupper() for word in skill.split()):
                    skills['Other Technical'].append(skill)
                else:
                    skills['Soft Skills'].append(skill)
    
    # Format skills for output
    formatted_skills = []
    for category, items in skills.items():
        if items:
            formatted_skills.append(f"{category}: {', '.join(items)}")
    
    return '\n'.join(formatted_skills) if formatted_skills else "Not specified"

def extract_resume_info(text, filename):
    """Flexible information extraction from resume text"""
    # Extract name (more flexible pattern)
    name = ""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    for line in lines:
        # Skip lines with numbers, emails, URLs, or too many words
        if (not any(char.isdigit() for char in line) and 
            '@' not in line and 
            'http' not in line.lower() and 
            len(line.split()) <= 4):
            name = line.strip()
            break
    
    # Extract email (flexible pattern)
    email_match = re.search(r'[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}', text)
    email = email_match.group(0) if email_match else "Not specified"
    
    # Extract phone (flexible international formats)
    phone_match = re.search(r'[\+\(]?[1-9][0-9 .\-\(\)]{7,}[0-9]', text)
    phone = phone_match.group(0) if phone_match else "Not specified"
    
    # Extract location (flexible city names)
    location = "Not specified"
    cities = ['hyderabad', 'chennai', 'bangalore', 'pune', 'mumbai', 
             'delhi', 'gurgaon', 'noida', 'kolkata', 'ahmedabad']
    for city in cities:
        if re.search(r'\b' + city + r'\b', text, re.IGNORECASE):
            location = city.title()
            break
    
    # Extract highest qualification
    qualification = extract_highest_qualification(text)
    
    # Extract skills
    skills = extract_skills(text)
    
    # Calculate experience
    experience = calculate_experience(text)
    
    return {
        'Name': name if name else "Not specified",
        'Highest Qualification': qualification,
        'Mobile Number': phone,
        'Email ID': email,
        'Location': location,
        'Skills': skills,
        'Years of Experience': experience,
        'Filename': filename
    }

def process_resume_batch(resume_batch):
    """Process a batch of resumes in parallel"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_single_resume, resume_batch))
    return [r for r in results if r is not None]

def process_single_resume(uploaded_file):
    """Process a single resume file"""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(uploaded_file.getbuffer())
            tmp_path = tmp.name
        
        text = extract_text_from_pdf(tmp_path)
        if text:
            info = extract_resume_info(text, uploaded_file.name)
            return info
    except Exception as e:
        st.error(f"Error processing {uploaded_file.name}: {str(e)}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
    return None

def create_excel_download_link(df):
    """Generate a link to download the DataFrame as an Excel file"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    return excel_data

def create_csv_download_link(df):
    """Generate a CSV download link with just emails"""
    output = StringIO()
    df[['Email ID']].to_csv(output, index=False)
    csv_data = output.getvalue()
    return csv_data.encode('utf-8')

def main():
    st.set_page_config(
        page_title="Flexible Resume Parser", 
        page_icon="📄", 
        layout="wide",
        menu_items={
            'About': "Extracts information from resumes with flexible formats"
        }
    )
    
    st.title("📄 Flexible Resume Parser")
    st.markdown("""
    Upload resumes to extract:
    - Name, Mobile, Email, Location
    - Highest Qualification (Degree only)
    - Skills (Categorized technical and soft skills)
    - Years of Experience (calculated from multiple sources)
    """)
    
    uploaded_files = st.file_uploader(
        "Upload Resume PDFs", 
        type="pdf", 
        accept_multiple_files=True,
        help=f"Max {MAX_FILE_SIZE/1024/1024/1024:.0f}GB total"
    )
    
    if uploaded_files:
        total_size = sum(f.size for f in uploaded_files)
        if total_size > MAX_FILE_SIZE:
            st.error(f"Total size {total_size/1024/1024/1024:.2f}GB exceeds limit")
            return
            
        st.success(f"{len(uploaded_files)} files uploaded")
        
        if st.button("Process Resumes", type="primary"):
            with st.spinner("Processing resumes..."):
                data = []
                progress_bar = st.progress(0)
                
                for i in range(0, len(uploaded_files), CHUNK_SIZE):
                    batch = uploaded_files[i:i + CHUNK_SIZE]
                    batch_results = process_resume_batch(batch)
                    data.extend(batch_results)
                    progress = min((i + CHUNK_SIZE) / len(uploaded_files), 1.0)
                    progress_bar.progress(progress)
                    st.info(f"Processed {min(i + CHUNK_SIZE, len(uploaded_files))}/{len(uploaded_files)} files")
                
                if data:
                    df = pd.DataFrame(data)
                    
                    # Display sample results
                    st.subheader("Sample Extracted Data")
                    st.dataframe(df.head())
                    
                    # Download options
                    st.subheader("Download Options")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        excel_data = create_excel_download_link(df)
                        st.download_button(
                            "Download Full Data (Excel)",
                            excel_data,
                            "resume_data.xlsx",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="Contains all extracted fields"
                        )
                    
                    with col2:
                        csv_data = create_csv_download_link(df)
                        st.download_button(
                            "Download Emails Only (CSV)",
                            csv_data,
                            "resume_emails.csv",
                            "text/csv",
                            help="Contains only email addresses"
                        )
                    
                    # Show statistics
                    st.subheader("Extraction Statistics")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Resumes Processed", len(df))
                        st.metric("With Valid Email", len(df[df['Email ID'] != "Not specified"]))
                    with col2:
                        st.metric("Most Common Qualification", 
                                  df['Highest Qualification'].value_counts().idxmax() 
                                  if not df['Highest Qualification'].empty else "N/A")
                        st.metric("Average Experience", 
                                 df['Years of Experience'].value_counts().idxmax() 
                                 if not df['Years of Experience'].empty else "N/A")
                else:
                    st.warning("No data could be extracted from the resumes. Please check if the files contain readable text.")

if __name__ == "__main__":
    main()
