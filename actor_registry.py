"""
Actor Registry - Central place to define and manage actor configurations.

TO ADD A NEW ACTOR:
1. Add a new entry to SEED_ACTORS below
2. That's it. No other code changes needed.
"""

from sqlalchemy.orm import Session
from models import ActorConfig
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# SEED ACTOR CONFIGURATIONS
# Add new actors here - they'll be automatically created in the database
# =============================================================================

SEED_ACTORS = {
    # =========================================================================
    # LINKEDIN
    # =========================================================================
    "linkedin": {
        "display_name": "LinkedIn Jobs",
        "actor_id": "curious_coder~linkedin-jobs-scraper",
        "default_timeout_minutes": 15,
        "default_max_results": 1000,
        "schedule_type": "custom",  # Allows custom date/time scheduling
        
        # What inputs does this actor accept?
        "input_schema": [
            {"name": "urls", "type": "textarea", "label": "Search URLs", "required": True, 
             "placeholder": "https://www.linkedin.com/jobs/search/?keywords=...", "help": "One URL per line"},
            {"name": "max_results", "type": "number", "label": "Max Results", "default": 1000},
            {"name": "scrape_company", "type": "boolean", "label": "Scrape Company Details", "default": True},
        ],
        
        # How to build the Apify input payload
        "input_template": {
            "urls": "{{urls_array}}",
            "count": "{{max_results}}",
            "scrapeCompany": "{{scrape_company}}"
        },
        
        # How to extract standardized fields from output
        "output_mapping": {
            "job_id": "id",
            "title": "title",
            "company_name": "companyName",
            "company_url": "companyWebsite",
            "company_linkedin_url": "companyLinkedinUrl", 
            "company_logo": "companyLogo",
            "company_description": "companyDescription",
            "company_slogan": "companySlogan",
            "company_address": "companyAddress",
            "employee_count": "companyEmployeesCount",
            "industry": "industries",
            "location": "location",
            "posted_at": "postedAt",
            "description": "descriptionText",
            "apply_url": "applyUrl",
            "job_url": "link",
            "salary_info": "salaryInfo",
            "salary": "salary",
            "benefits": "benefits",
            "applicants_count": "applicantsCount",
            "seniority_level": "seniorityLevel",
            "employment_type": "employmentType", 
            "job_function": "jobFunction",
            "job_poster_name": "jobPosterName",
            "job_poster_title": "jobPosterTitle",
            "job_poster_photo": "jobPosterPhoto",
            "job_poster_profile_url": "jobPosterProfileUrl",
            "tracking_id": "trackingId",
            "ref_id": "refId"
        },
        
        # What fields can be filtered and how
        "filter_config": {
            "company_name_excludes": {"field": "companyName", "type": "exclude_contains"},
            "industry_excludes": {"field": "industries", "type": "exclude_contains"},
            "min_employees": {"field": "companyEmployeesCount", "type": "min_value"},
            "max_employees": {"field": "companyEmployeesCount", "type": "max_value"}
        },
        
        # How to format for Clay webhook
        "clay_template": {
            "ID": "{{job_id}}",
            "Job Title": "{{title}}",
            "Company name": "{{company_name}}",
            "Company website": "{{company_url}}",
            "Company LinkedIn URL": "{{company_linkedin_url}}",
            "Company Logo": "{{company_logo}}",
            "Company description": "{{company_description}}",
            "Company Slogan": "{{company_slogan}}",
            "Company Address": "{{company_address}}",
            "Employee Count": "{{employee_count}}",
            "Industry": "{{industry}}",
            "Location": "{{location}}",
            "Posted at": "{{posted_at}}",
            "Job Description": "{{description}}",
            "Apply URL": "{{apply_url}}",
            "JOB URL": "{{job_url}}",
            "Salary Info": "{{salary_info}}",
            "Salary": "{{salary}}",
            "Benefits": "{{benefits}}",
            "Applicants Count": "{{applicants_count}}",
            "Seniority Level": "{{seniority_level}}",
            "Employment Type": "{{employment_type}}",
            "Job Function": "{{job_function}}",
            "Job Poster Name": "{{job_poster_name}}",
            "Job Poster Title": "{{job_poster_title}}",
            "Job Poster Photo": "{{job_poster_photo}}",
            "Job Poster LinkedIn": "{{job_poster_profile_url}}",
            "Tracking ID": "{{tracking_id}}",
            "Ref ID": "{{ref_id}}"
        }
    },
    
    # =========================================================================
    # INDEED
    # =========================================================================
    "indeed": {
        "display_name": "Indeed Jobs",
        "actor_id": "curious_coder~indeed-scraper",
        "default_timeout_minutes": 35,
        "default_max_results": 200,
        "schedule_type": "custom",  # Allows custom date/time scheduling
        
        "input_schema": [
            {"name": "search_url", "type": "url", "label": "Search URL", "required": True,
             "placeholder": "https://www.indeed.com/jobs?q=...", "help": "Indeed search URL"},
            {"name": "max_results", "type": "number", "label": "Max Results", "default": 200},
            {"name": "start_page", "type": "number", "label": "Start Page", "default": 1},
        ],
        
        "input_template": {
            "count": "{{max_results}}",
            "proxy": {"useApifyProxy": True, "apifyProxyGroups": []},
            "scrapeJobs.scrapeCompany": True,
            "scrapeJobs.searchUrl": "{{search_url}}",
            "startPage": "{{start_page}}",
            "useBrowser": True
        },
        
        "output_mapping": {
            "job_id": "id",
            "title": "displayTitle",
            "company_name": "company",
            "company_url": "companyDetails.aboutSectionViewModel.aboutCompany.websiteUrl.url",
            "company_description": "companyDetails.aboutSectionViewModel.aboutCompany.description",
            "employee_range": "companyDetails.aboutSectionViewModel.aboutCompany.employeeRange",
            "industry": "companyDetails.aboutSectionViewModel.aboutCompany.industry",
            "location": "formattedLocation",
            "posted_at": "pubDate",
            "description": "jobDescription",
            "apply_url": "thirdPartyApplyUrl",
            "job_url": "link",
            "salary": "salarySnippet.text",
            "is_new": "newJob"
        },
        
        "filter_config": {
            "company_name_excludes": {"field": "company", "type": "exclude_contains"},
            "industry_excludes": {"field": "companyDetails.aboutSectionViewModel.aboutCompany.industry", "type": "exclude_contains"}
        },
        
        "clay_template": {
            "ID": "{{job_id}}",
            "Job Title": "{{title}}",
            "Company name": "{{company_name}}",
            "Company website": "{{company_url}}",
            "Company description": "{{company_description}}",
            "Employee Range": "{{employee_range}}",
            "Industry": "{{industry}}",
            "Location": "{{location}}",
            "Posted at": "{{posted_at}}",
            "Job Description": "{{description}}",
            "Apply URL": "{{apply_url}}",
            "JOB URL": "{{job_url}}",
            "Salary": "{{salary}}",
            "NEW JOB": "{{is_new}}"
        }
    },
    
    # =========================================================================
    # FANTASTIC JOBS (Career Site Scraper)
    # =========================================================================
    "fantastic_jobs": {
        "display_name": "Fantastic Jobs (Career Sites)",
        "actor_id": "fantastic-jobs~advanced-linkedin-job-search-api",
        "default_timeout_minutes": 20,
        "default_max_results": 100,
        "schedule_type": "simple",  # Simple dropdown only
        
        "input_schema": [
            {"name": "title_search", "type": "textarea", "label": "Job Titles", "required": True,
             "placeholder": "Test Engineer\nQuality Engineer\nEMC Engineer", "help": "One title per line"},
            {"name": "location_search", "type": "text", "label": "Locations", "default": "united states",
             "help": "Comma-separated locations"},
            {"name": "employer_search", "type": "text", "label": "Employer Filter", 
             "help": "Comma-separated employer names (optional)"},
            {"name": "time_range", "type": "select", "label": "Time Range", "default": "7d",
             "options": [
                 {"value": "1h", "label": "Last 1 Hour"},
                 {"value": "24h", "label": "Last 24 Hours"},
                 {"value": "7d", "label": "Last 7 Days"},
                 {"value": "6m", "label": "Last 6 Months"}
             ]},
            {"name": "max_results", "type": "number", "label": "Max Results", "default": 100},
            {"name": "min_employees", "type": "number", "label": "Min Employees", "help": "Optional"},
            {"name": "max_employees", "type": "number", "label": "Max Employees", "help": "Optional"},
        ],
        
        "input_template": {
            "titleSearch": "{{title_search_array}}",
            "locationSearch": "{{location_search_array}}",
            "employerSearch": "{{employer_search_array}}",
            "timeRange": "{{time_range}}",
            "limit": "{{max_results}}",
            "liOrganizationEmployeesGte": "{{min_employees}}",
            "liOrganizationEmployeesLte": "{{max_employees}}",
            "includeAi": True,
            "includeLinkedIn": True,
            "aiHasSalary": False,
            "aiVisaSponsorshipFilter": False,
            "populateAiRemoteLocation": False,
            "populateAiRemoteLocationDerived": False,
            "remote only (legacy)": False,
            "descriptionType": "text"
        },
        
        "output_mapping": {
            "job_id": "id",
            "title": "title",
            "company_name": "organization",
            "company_url": "organization_url",
            "company_linkedin_url": "linkedin_org_url",
            "company_description": "linkedin_org_description",
            "employee_count": "linkedin_org_employees",
            "employee_range": "linkedin_org_size",
            "industry": "linkedin_org_industry",
            "location": "locations_derived[0]",
            "posted_at": "date_posted",
            "description": "description_text",
            "apply_url": "url",
            "job_url": "url",
            "salary_min": "ai_salary_minvalue",
            "salary_max": "ai_salary_maxvalue",
            "salary_currency": "ai_salary_currency",
            "experience_level": "ai_experience_level",
            "work_arrangement": "ai_work_arrangement",
            "key_skills": "ai_key_skills",
            "requirements_summary": "ai_requirements_summary",
            "remote": "remote_derived",
            "source_domain": "source_domain",
            "company_hq": "linkedin_org_headquarters",
            "company_type": "linkedin_org_type",
            "company_founded": "linkedin_org_foundeddate",
            "company_specialties": "linkedin_org_specialties"
        },
        
        "filter_config": {
            "company_name_excludes": {"field": "organization", "type": "exclude_contains"},
            "industry_excludes": {"field": "linkedin_org_industry", "type": "exclude_contains"},
            "min_employees": {"field": "linkedin_org_employees", "type": "min_value"},
            "max_employees": {"field": "linkedin_org_employees", "type": "max_value"}
        },
        
        "clay_template": {
            "ID": "{{job_id}}",
            "Job Title": "{{title}}",
            "Company name": "{{company_name}}",
            "Company website": "{{company_url}}",
            "Company domain": "{{source_domain}}",
            "Linkedin URL": "{{company_linkedin_url}}",
            "Company description": "{{company_description}}",
            "Employee Count": "{{employee_count}}",
            "Company Size": "{{employee_range}}",
            "Industry": "{{industry}}",
            "Location": "{{location}}",
            "Remote": "{{remote}}",
            "Work Arrangement": "{{work_arrangement}}",
            "Posted at": "{{posted_at}}",
            "Job Description": "{{description}}",
            "Apply URL": "{{apply_url}}",
            "JOB URL": "{{job_url}}",
            "Salary Min": "{{salary_min}}",
            "Salary Max": "{{salary_max}}",
            "Currency": "{{salary_currency}}",
            "Experience Level": "{{experience_level}}",
            "Key Skills": "{{key_skills}}",
            "Requirements Summary": "{{requirements_summary}}",
            "Company HQ": "{{company_hq}}",
            "Company Type": "{{company_type}}",
            "Company Founded": "{{company_founded}}",
            "Company Specialties": "{{company_specialties}}"
        }
    }
}


# =============================================================================
# ACTOR REGISTRY SERVICE
# =============================================================================

class ActorRegistry:
    """Service to manage actor configurations"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def seed_actors(self):
        """Create or update actor configs from SEED_ACTORS"""
        for actor_key, config in SEED_ACTORS.items():
            existing = self.db.query(ActorConfig).filter(
                ActorConfig.actor_key == actor_key
            ).first()
            
            if existing:
                # Update existing
                existing.display_name = config["display_name"]
                existing.actor_id = config["actor_id"]
                existing.default_timeout_minutes = config.get("default_timeout_minutes", 15)
                existing.default_max_results = config.get("default_max_results", 100)
                existing.input_schema = config.get("input_schema", [])
                existing.input_template = config.get("input_template", {})
                existing.output_mapping = config.get("output_mapping", {})
                existing.filter_config = config.get("filter_config", {})
                existing.clay_template = config.get("clay_template", {})
                existing.schedule_type = config.get("schedule_type", "simple")
                logger.info(f"Updated actor config: {actor_key}")
            else:
                # Create new
                new_actor = ActorConfig(
                    actor_key=actor_key,
                    display_name=config["display_name"],
                    actor_id=config["actor_id"],
                    default_timeout_minutes=config.get("default_timeout_minutes", 15),
                    default_max_results=config.get("default_max_results", 100),
                    input_schema=config.get("input_schema", []),
                    input_template=config.get("input_template", {}),
                    output_mapping=config.get("output_mapping", {}),
                    filter_config=config.get("filter_config", {}),
                    clay_template=config.get("clay_template", {}),
                    schedule_type=config.get("schedule_type", "simple")
                )
                self.db.add(new_actor)
                logger.info(f"Created actor config: {actor_key}")
        
        self.db.commit()
        logger.info(f"Seeded {len(SEED_ACTORS)} actor configurations")
    
    def get_actor(self, actor_key: str) -> ActorConfig:
        """Get actor config by key"""
        actor = self.db.query(ActorConfig).filter(
            ActorConfig.actor_key == actor_key,
            ActorConfig.is_active == True
        ).first()
        
        if not actor:
            raise ValueError(f"Actor '{actor_key}' not found or inactive")
        
        return actor
    
    def list_actors(self, active_only: bool = True):
        """List all actor configs"""
        query = self.db.query(ActorConfig)
        if active_only:
            query = query.filter(ActorConfig.is_active == True)
        return query.all()
    
    def get_actor_choices(self):
        """Get list of actors for dropdown"""
        actors = self.list_actors()
        return [{"key": a.actor_key, "name": a.display_name} for a in actors]
