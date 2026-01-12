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
        "default_timeout_minutes": 90,  # Allow up to 1.5 hours for large LinkedIn searches
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
        "default_timeout_minutes": 120,  # 2 hours for Indeed jobs
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
            "company_rating": "companyRating",
            "company_review_count": "companyReviewCount",
            "company_overview_link": "companyOverviewLink",
            "employee_range": "companyDetails.aboutSectionViewModel.aboutCompany.employeeRange",
            "company_founded": "companyDetails.aboutSectionViewModel.aboutCompany.founded",
            "company_headquarters": "companyDetails.aboutSectionViewModel.aboutCompany.headquartersLocation.address",
            "company_ceo_approval": "companyDetails.aboutSectionViewModel.aboutCompany.ceoApproval",
            "industry": "companyDetails.aboutSectionViewModel.aboutCompany.industry",
            "sector_names": "companyDetails.aboutSectionViewModel.aboutCompany.sectorNames",
            "location": "formattedLocation",
            "location_city": "jobLocationCity",
            "location_state": "jobLocationState",
            "location_full_address": "location.fullAddress",
            "location_street_address": "location.streetAddress",
            "location_postal_code": "location.postalCode",
            "location_latitude": "location.latitude",
            "location_longitude": "location.longitude",
            "posted_at": "pubDate",
            "posted_time_formatted": "formattedRelativeTime",
            "description": "jobDescription",
            "description_html": "jobDescriptionHTML",
            "apply_url": "thirdPartyApplyUrl",
            "original_apply_url": "originalApplyUrl",
            "job_url": "link",
            "salary": "salarySnippet.text",
            "salary_currency": "salarySnippet.currency",
            "is_new": "newJob",
            "is_sponsored": "sponsored",
            "is_featured": "featuredEmployer",
            "is_urgent": "urgentlyHiring",
            "indeed_apply_enabled": "indeedApplyEnabled",
            "employer_responsive": "employerResponsive",
            "job_types": "jobTypes",
            "organic_apply_starts": "jobStats.organicApplyStarts",
            "high_volume_hiring": "highVolumeHiringModel.highVolumeHiring",
            "is_repost": "isRepost",
            "feed_id": "feedId",
            "fcc_id": "fccId"
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
            "Rating": "{{rating}}",
            "Review Count": "{{review_count}}",
            "State Name": "{{state_name}}",
            "City Name": "{{city_name}}",
            "Postal Code": "{{postal_code}}",
            "Viewjob Link": "{{viewjob_link}}",
            "Source": "{{source}}",
            "Extract Date": "{{extract_date}}",
            "CEO Name": "{{ceo_name}}",
            "CEO Approval": "{{ceo_approval}}",
            "Recommend To Friend": "{{recommend_to_friend}}",
            "Career Opportunities": "{{career_opportunities}}",
            "Work-Life Balance": "{{work_life_balance}}",
            "Compensation Benefits": "{{compensation_benefits}}",
            "Company Culture": "{{company_culture}}",
            "Job Type": "{{job_type}}",
            "NEW JOB": "{{is_new}}",
            "Is Sponsored": "{{is_sponsored}}",
            "Is Featured": "{{is_featured}}",
            "Is Urgent": "{{is_urgent}}",
            "Indeed Apply": "{{indeed_apply_enabled}}",
            "Employer Responsive": "{{employer_responsive}}",
            "Organic Apply Starts": "{{organic_apply_starts}}",
            "High Volume Hiring": "{{high_volume_hiring}}"
        }
    },
    
    # =========================================================================
    # FANTASTIC JOBS (Career Site Scraper)
    # =========================================================================
    "fantastic_jobs": {
        "display_name": "Fantastic Jobs (Career Sites)",
        "actor_id": "fantastic-jobs~advanced-linkedin-job-search-api",
        "default_timeout_minutes": 120,  # 2 hours for Fantastic Jobs
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
            "company_logo": "organization_logo",
            "company_slogan": "linkedin_org_slogan",
            "employee_count": "linkedin_org_employees",
            "employee_range": "linkedin_org_size",
            "industry": "linkedin_org_industry",
            "location": "locations_derived[0]",
            "posted_at": "date_posted",
            "description": "description_text",
            "apply_url": "url",
            "job_url": "external_apply_url",
            "salary_min": "ai_salary_minvalue",
            "salary_max": "ai_salary_maxvalue",
            "salary_currency": "ai_salary_currency",
            "experience_level": "ai_experience_level",
            "work_arrangement": "ai_work_arrangement",
            "key_skills": "ai_key_skills",
            "requirements_summary": "ai_requirements_summary",
            "remote": "remote_derived",
            "source": "source",
            "source_domain": "source_domain",
            "company_hq": "linkedin_org_headquarters",
            "company_type": "linkedin_org_type",
            "company_founded": "linkedin_org_foundeddate",
            "company_specialties": "linkedin_org_specialties",
            "company_followers": "linkedin_org_followers",
            "seniority_level": "seniority",
            "benefits": "ai_benefits",
            "core_responsibilities": "ai_core_responsibilities",
            "working_hours": "ai_working_hours",
            "job_language": "ai_job_language",
            "visa_sponsorship": "ai_visa_sponsorship",
            "keywords": "ai_keywords",
            "education_requirements": "ai_education_requirements",
            "employment_type": "employment_type",
            "date_valid_through": "date_validthrough",
            "direct_apply": "directapply",
            "city": "cities_derived[0]",
            "state": "regions_derived[0]",
            "country": "countries_derived[0]",
            "timezone": "timezones_derived[0]",
            "latitude": "lats_derived[0]",
            "longitude": "lngs_derived[0]"
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
            "Company LinkedIn URL": "{{company_linkedin_url}}",
            "Company Logo": "{{company_logo}}",
            "Company description": "{{company_description}}",
            "Company Slogan": "{{company_slogan}}",
            "Employee Count": "{{employee_count}}",
            "Company Size": "{{employee_range}}",
            "Company Followers": "{{company_followers}}",
            "Industry": "{{industry}}",
            "Company HQ": "{{company_hq}}",
            "Company Type": "{{company_type}}",
            "Company Founded": "{{company_founded}}",
            "Company Specialties": "{{company_specialties}}",
            "Location": "{{location}}",
            "City": "{{city}}",
            "State": "{{state}}",
            "Country": "{{country}}",
            "Timezone": "{{timezone}}",
            "Latitude": "{{latitude}}",
            "Longitude": "{{longitude}}",
            "Remote": "{{remote}}",
            "Work Arrangement": "{{work_arrangement}}",
            "Employment Type": "{{employment_type}}",
            "Posted at": "{{posted_at}}",
            "Valid Through": "{{date_valid_through}}",
            "Job Description": "{{description}}",
            "Core Responsibilities": "{{core_responsibilities}}",
            "Apply URL": "{{apply_url}}",
            "JOB URL": "{{job_url}}",
            "Direct Apply": "{{direct_apply}}",
            "Salary Min": "{{salary_min}}",
            "Salary Max": "{{salary_max}}",
            "Currency": "{{salary_currency}}",
            "Experience Level": "{{experience_level}}",
            "Seniority Level": "{{seniority_level}}",
            "Key Skills": "{{key_skills}}",
            "Keywords": "{{keywords}}",
            "Requirements Summary": "{{requirements_summary}}",
            "Education Requirements": "{{education_requirements}}",
            "Benefits": "{{benefits}}",
            "Working Hours": "{{working_hours}}",
            "Job Language": "{{job_language}}",
            "Visa Sponsorship": "{{visa_sponsorship}}",
            "Source": "{{source}}",
            "Source Domain": "{{source_domain}}"
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
