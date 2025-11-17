# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import json

# Page configuration
st.set_page_config(
    page_title="Resume Matcher",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for sleek, modern design
st.markdown("""
<style>
    /* Import modern font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global dark theme */
    .stApp {
        background-color: #0d1117;
        color: #ffffff;
        font-family: 'Inter', sans-serif;
    }
    
    .main .block-container {
        padding: 1rem 2rem;
        max-width: none;
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    header[data-testid="stHeader"] {display:none;}
    
    /* Main title */
    .main-title {
        font-size: 2rem;
        font-weight: 600;
        color: #ffffff;
        text-align: center;
        margin-bottom: 2rem;
        letter-spacing: -0.5px;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.2rem;
        font-weight: 500;
        color: #8b949e;
        margin-bottom: 1rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Job cards */
    .job-card {
        background: linear-gradient(135deg, #21262d 0%, #30363d 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 0.75rem;
        transition: all 0.2s ease;
        cursor: pointer;
    }
    
    .job-card:hover {
        background: linear-gradient(135deg, #30363d 0%, #40464d 100%);
        border-color: #58a6ff;
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(88, 166, 255, 0.1);
    }
    
    .job-card.selected {
        background: linear-gradient(135deg, #0d419d 0%, #1f6feb 100%);
        border-color: #58a6ff;
        box-shadow: 0 0 20px rgba(88, 166, 255, 0.3);
    }
    
    .job-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #ffffff;
        margin-bottom: 0.5rem;
    }
    
    .job-description {
        font-size: 0.9rem;
        color: #8b949e;
        line-height: 1.4;
        margin-bottom: 0.75rem;
    }
    
    .job-skills {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
    }
    
    .skill-tag {
        background: rgba(88, 166, 255, 0.1);
        color: #58a6ff;
        padding: 0.25rem 0.75rem;
        border-radius: 16px;
        font-size: 0.8rem;
        font-weight: 500;
        border: 1px solid rgba(88, 166, 255, 0.2);
    }
    
    /* Candidate cards */
    .candidate-card {
        background: linear-gradient(135deg, #21262d 0%, #30363d 100%);
        border: 1px solid #30363d;
        border-radius: 16px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        position: relative;
        transition: all 0.2s ease;
    }
    
    .candidate-card:hover {
        border-color: #58a6ff;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
        transform: translateY(-2px);
    }
    
    .candidate-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 1rem;
    }
    
    .candidate-name {
        font-size: 1.2rem;
        font-weight: 600;
        color: #ffffff;
        margin: 0;
    }
    
    .candidate-info {
        font-size: 0.9rem;
        color: #8b949e;
        margin-top: 0.25rem;
    }
    
    /* Ranking badges */
    .rank-badge {
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 700;
        text-align: center;
        min-width: 60px;
        position: relative;
    }
    
    .rank-1 {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        color: #ffffff;
        box-shadow: 0 0 15px rgba(40, 167, 69, 0.4);
    }
    
    .rank-2 {
        background: linear-gradient(135deg, #007bff 0%, #6610f2 100%);
        color: #ffffff;
        box-shadow: 0 0 15px rgba(0, 123, 255, 0.4);
    }
    
    .rank-3 {
        background: linear-gradient(135deg, #ffc107 0%, #fd7e14 100%);
        color: #000000;
        box-shadow: 0 0 15px rgba(255, 193, 7, 0.4);
    }
    
    .rank-4, .rank-5 {
        background: linear-gradient(135deg, #6c757d 0%, #495057 100%);
        color: #ffffff;
    }
    
    /* Match score */
    .match-score {
        background: rgba(40, 167, 69, 0.1);
        color: #28a745;
        padding: 0.5rem 1rem;
        border-radius: 12px;
        font-size: 1rem;
        font-weight: 600;
        margin-top: 1rem;
        text-align: center;
        border: 1px solid rgba(40, 167, 69, 0.2);
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        background-color: #21262d;
        border: 1px solid #30363d;
        border-radius: 8px;
        color: #ffffff;
    }
    
    .stSelectbox > div > div > div {
        color: #ffffff;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #21262d !important;
        border: 1px solid #30363d !important;
        border-radius: 8px !important;
        color: #ffffff !important;
        font-weight: 500 !important;
    }
    
    .streamlit-expanderHeader:hover {
        background-color: #30363d !important;
        border-color: #58a6ff !important;
    }
    
    .streamlit-expanderContent {
        background-color: #161b22 !important;
        border: 1px solid #30363d !important;
        border-top: none !important;
        border-radius: 0 0 8px 8px !important;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(135deg, #238636 0%, #2ea043 100%) !important;
        color: #ffffff !important;
        border: 1px solid #2ea043 !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        padding: 0.5rem 1rem !important;
        transition: all 0.2s ease !important;
    }
    
    .stButton > button:hover {
        background: linear-gradient(135deg, #2ea043 0%, #34d058 100%) !important;
        border-color: #34d058 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(46, 160, 67, 0.3) !important;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    ::-webkit-scrollbar-track {
        background: #21262d;
    }
    ::-webkit-scrollbar-thumb {
        background: #30363d;
        border-radius: 4px;
    }
    ::-webkit-scrollbar-thumb:hover {
        background: #40464d;
    }
</style>
""", unsafe_allow_html=True)

class SimpleResumeMatcher:
    def __init__(self):
        try:
            self.session = get_active_session()
        except:
            self.session = None
    
    def get_jobs(self):
        """Get all unique jobs from Snowflake table"""
        if self.session:
            try:
                query = """
                SELECT DISTINCT 
                    REQ_ID as JOB_ID,
                    JOB_TITLE,
                    JOB_TITLE || ' (' || REQ_ID || ')' as TITLE,
                    CASE 
                        WHEN CATEGORY = 'MECHANICALCHEMICALQUALITYENGINEERING' THEN 'Core_Engg'
                        WHEN CATEGORY = 'IT' THEN 'Engineering'
                        ELSE CATEGORY 
                    END as DEPARTMENT,
                    'Job requirements and details for ' || JOB_TITLE || ' (' || REQ_ID || ')' as DESCRIPTION,
                    COALESCE(REQ_TECH_SKILLS, '["Skills vary by position"]') as REQUIRED_SKILLS
                FROM HACKATHON_2025.JOE.GOLD_TABLE_COMPARISONS
                WHERE REQ_ID IS NOT NULL AND JOB_TITLE IS NOT NULL
                ORDER BY REQ_ID, JOB_TITLE
                """
                return self.session.sql(query).to_pandas()
            except Exception as e:
                st.error(f"Error connecting to Snowflake: {str(e)}")
                pass
        
        # Fallback sample data
        return pd.DataFrame([
            {
                'JOB_ID': 'REQ001',
                'TITLE': 'Software Engineer (REQ001)',
                'DEPARTMENT': 'Engineering',
                'DESCRIPTION': 'Job requirements and details for Software Engineer (REQ001)',
                'REQUIRED_SKILLS': 'Python, JavaScript, React, SQL, AWS'
            }
        ])
    
    def get_jobs_original(self):
        """Get jobs with original category names for filtering"""
        if self.session:
            try:
                query = """
                SELECT DISTINCT 
                    REQ_ID as JOB_ID,
                    JOB_TITLE || ' (' || REQ_ID || ')' as TITLE,
                    CATEGORY as DEPARTMENT,
                    'Job requirements and details for ' || JOB_TITLE || ' (' || REQ_ID || ')' as DESCRIPTION,
                    '["Skills vary by position"]' as REQUIRED_SKILLS
                FROM HACKATHON_2025.JOE.GOLD_TABLE_COMPARISONS
                ORDER BY REQ_ID
                """
                return self.session.sql(query).to_pandas()
            except Exception as e:
                st.error(f"Error connecting to Snowflake: {str(e)}")
                pass
        
        # Fallback sample data
        return pd.DataFrame([
            {
                'JOB_ID': 'REQ001',
                'TITLE': 'REQ001',
                'DEPARTMENT': 'MECHANICALCHEMICALQUALITYENGINEERING',
                'DESCRIPTION': 'Job requirements and details for REQ001',
                'REQUIRED_SKILLS': '["Skills vary by position"]'
            }
        ])
    
    def get_candidates(self, job_id):
        """Get candidates for a specific job from Snowflake table"""
        if self.session:
            try:
                query = f"""
                SELECT 
                    'Candidate ' || REPLACE(RESUME_ID, '.pdf', '') as NAME,
                    ROUND(MATCH_SCORE * 100, 0) as MATCH_SCORE,
                    RANK_WITHIN_REQ as RANK,
                    COALESCE(RESUME_CURRENT_JOB, 'Not specified') as CURRENT_ROLE,
                    COALESCE(RESUME_YOE, 0) as YEARS_EXPERIENCE,
                    COALESCE(RESUME_TECH_SKILLS, 'Not specified') as TECH_SKILLS,
                    FILE_PATH,
                    FILE_URL_CLICKABLE
                FROM HACKATHON_2025.JOE.GOLD_TABLE_COMPARISONS
                WHERE REQ_ID = '{job_id}'
                ORDER BY RANK_WITHIN_REQ
                """
                return self.session.sql(query).to_pandas()
            except Exception as e:
                st.error(f"Error loading candidates: {str(e)}")
                pass
        
        # Sample candidates data
        candidates_data = {
            'REQ001': [
                {'NAME': 'Candidate 1', 'CURRENT_ROLE': 'Senior Developer at TechCorp', 'YEARS_EXPERIENCE': 5, 'TECH_SKILLS': 'Python, JavaScript, React, AWS', 'RANK': 1, 'MATCH_SCORE': 94},
                {'NAME': 'Candidate 2', 'CURRENT_ROLE': 'Full Stack Engineer at StartupXYZ', 'YEARS_EXPERIENCE': 7, 'TECH_SKILLS': 'JavaScript, Node.js, Docker, Kubernetes', 'RANK': 2, 'MATCH_SCORE': 89},
                {'NAME': 'Candidate 3', 'CURRENT_ROLE': 'Backend Developer at DataCorp', 'YEARS_EXPERIENCE': 4, 'TECH_SKILLS': 'Python, SQL, AWS, Machine Learning', 'RANK': 3, 'MATCH_SCORE': 82},
            ]
        }
        
        if job_id in candidates_data:
            return pd.DataFrame(candidates_data[job_id])
        else:
            return pd.DataFrame()

def main():
    # Initialize matcher
    matcher = SimpleResumeMatcher()
    
    # Main title
    st.markdown('<h1 class="main-title">Resume Matcher</h1>', unsafe_allow_html=True)
    
    # Create two columns
    col1, col2 = st.columns([1, 2], gap="large")
    
    with col1:
        st.markdown('<div class="section-header">Job Categories</div>', unsafe_allow_html=True)
        
        # Get unique categories from the database
        try:
            category_query = """
            SELECT DISTINCT 
                CASE 
                    WHEN CATEGORY = 'MECHANICALCHEMICALQUALITYENGINEERING' THEN 'Core_Engg'
                    WHEN CATEGORY = 'IT' THEN 'Engineering'
                    ELSE CATEGORY 
                END as CATEGORY
            FROM HACKATHON_2025.JOE.GOLD_TABLE_COMPARISONS 
            WHERE CATEGORY IS NOT NULL
            ORDER BY CATEGORY
            """
            categories_df = matcher.session.sql(category_query).to_pandas() if matcher.session else pd.DataFrame()
            categories = categories_df['CATEGORY'].tolist() if not categories_df.empty else ['HR', 'Engineering', 'IT', 'Banking', 'Sales']
        except:
            categories = ['HR', 'Engineering', 'IT', 'Banking', 'Sales']
        
        # Category filter
        selected_category = st.selectbox(
            "Filter by Department:",
            options=['All'] + categories,
            index=0,
            key="category_filter"
        )
        
        st.markdown('<div class="section-header">Job Positions</div>', unsafe_allow_html=True)
        
        # Get jobs
        jobs_df = matcher.get_jobs()
        
        # Map job titles to categories
        job_category_mapping = {
            'Senior Software Engineer': 'Engineering',
            'Product Marketing Manager': 'Sales',
            'Senior UX Designer': 'IT',
            'Data Scientist': 'Engineering',
            'HR Business Partner': 'HR',
            'Systems Administrator': 'IT',
            'Investment Banking Analyst': 'Banking',
            'Sales Development Representative': 'Sales',
            'Software Engineer': 'Engineering',
            'Marketing Manager': 'Sales',
            'Product Designer': 'IT'
        }
        
        # Filter jobs by selected category using the DEPARTMENT column from database
        if selected_category != 'All':
            jobs_df = jobs_df[jobs_df['DEPARTMENT'] == selected_category]
        
        # Initialize session state for expanded job and selected job
        if 'expanded_job_id' not in st.session_state:
            st.session_state.expanded_job_id = None
        if 'selected_job_id' not in st.session_state:
            st.session_state.selected_job_id = None
        
        # Display all job positions as clickable accordion sections
        if not jobs_df.empty:
            for idx, job in jobs_df.iterrows():
                job_id = job['JOB_ID']
                # Create unique key combining job_id and index to avoid duplicates
                unique_key = f"{job_id}_{idx}"
                is_expanded = st.session_state.expanded_job_id == unique_key
                
                # Parse skills - handle both JSON format and comma-separated strings, show only top 3
                try:
                    if job['REQUIRED_SKILLS'].startswith('['):
                        # JSON format
                        skills = json.loads(job['REQUIRED_SKILLS'])
                    else:
                        # Comma-separated string or single value
                        skills = [skill.strip() for skill in str(job['REQUIRED_SKILLS']).split(',')]
                    # Limit to top 3 skills
                    top_skills = [skill for skill in skills if skill][:3]
                    skills_html = ''.join([f'<span class="skill-tag">{skill}</span>' for skill in top_skills])
                except:
                    skills_html = '<span class="skill-tag">Skills not available</span>'
                
                # Create clickable job header with department name replacement
                job_title_display = job['TITLE'].replace('MECHANICALCHEMICALQUALITYENGINEERING', 'Core_Engg').replace('IT', 'Engineering')
                expand_symbol = "▼" if is_expanded else "▶"
                if st.button(f"{expand_symbol} {job_title_display}", key=f"toggle_{unique_key}"):
                    # Toggle expansion - if this job is expanded, collapse it; otherwise expand it and collapse others
                    if st.session_state.expanded_job_id == unique_key:
                        st.session_state.expanded_job_id = None
                        st.session_state.selected_job_id = None
                    else:
                        st.session_state.expanded_job_id = unique_key
                        st.session_state.selected_job_id = job_id  # Still use original job_id for candidate lookup
                    st.rerun()
                
                # Show job details if this job is expanded
                if is_expanded:
                    st.markdown(f'''
                    <div class="job-card selected" style="margin-top: -0.5rem; margin-bottom: 1rem;">
                        <div style="color: #8b949e; font-size: 0.9rem; line-height: 1.4; margin-bottom: 1rem;">
                            {job['DESCRIPTION']}
                        </div>
                        <div style="margin-bottom: 1rem;">
                            <strong style="color: #ffffff; font-size: 0.9rem;">Required Skills:</strong>
                        </div>
                        <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
                            {skills_html}
                        </div>
                    </div>
                    ''', unsafe_allow_html=True)
        else:
            st.write("No jobs available")
    
    with col2:
        st.markdown('<div class="section-header">Top Candidates</div>', unsafe_allow_html=True)
        
        if st.session_state.selected_job_id:
            # Get candidates for selected job
            candidates_df = matcher.get_candidates(st.session_state.selected_job_id)
            
            if not candidates_df.empty:
                # Display all candidates
                for _, candidate in candidates_df.iterrows():
                    rank_class = f"rank-{candidate['RANK']}"
                    
                    # Prepare resume button HTML
                    resume_button_html = ""
                    if 'FILE_URL_CLICKABLE' in candidate and candidate['FILE_URL_CLICKABLE']:
                        resume_button_html = f'''
                        <div style="margin-top: 1rem;">
                            <a href="{candidate['FILE_URL_CLICKABLE']}" target="_blank" style="text-decoration: none;">
                                <button style="
                                    background: linear-gradient(135deg, #238636 0%, #2ea043 100%);
                                    color: white;
                                    border: 1px solid #2ea043;
                                    border-radius: 6px;
                                    padding: 0.5rem 1rem;
                                    font-size: 0.9rem;
                                    font-weight: 500;
                                    cursor: pointer;
                                    transition: all 0.2s ease;
                                " onmouseover="this.style.background='linear-gradient(135deg, #2ea043 0%, #34d058 100%)'; this.style.transform='translateY(-1px)'" 
                                   onmouseout="this.style.background='linear-gradient(135deg, #238636 0%, #2ea043 100%)'; this.style.transform='translateY(0)'">
                                    📄 View Resume
                                </button>
                            </a>
                        </div>
                        '''
                    elif 'FILE_PATH' in candidate and candidate['FILE_PATH']:
                        resume_button_html = f'''
                        <div style="margin-top: 1rem; color: #8b949e; font-size: 0.8rem;">
                            📁 Resume: {candidate['FILE_PATH']}
                        </div>
                        '''
                    
                    # Display candidate card with embedded button
                    st.markdown(f'''
                        <div class="candidate-card">
                            <div class="candidate-header">
                                <div>
                                    <div class="candidate-name">{candidate['NAME']}</div>
                                    <div class="candidate-info">Current Role: {candidate['CURRENT_ROLE']}</div>
                                    <div class="candidate-info">Experience: {candidate.get('YEARS_EXPERIENCE', 0)} years</div>
                                </div>
                                <div class="rank-badge {rank_class}">
                                    #{candidate['RANK']}
                                </div>
                            </div>
                            <div class="match-score">
                                {candidate['MATCH_SCORE']:.0f}% Match
                            </div>
                            {resume_button_html}
                        </div>
                    ''', unsafe_allow_html=True)
            else:
                st.markdown('<div style="text-align: center; color: #8b949e; padding: 2rem;">No candidates found for this position.</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="text-align: center; color: #8b949e; padding: 2rem;">Select a job to view candidates.</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()