from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import json
import re
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime, timedelta,timezone



app = Flask(__name__)
worker_running = False

# ✅ ADD: Batch update configuration
BATCH_SIZE = 10  # Update MongoDB when 10 jobs are completed
TEMP_RESULTS_DIR = Path("temp_completed_jobs")  # Local storage for completed jobs
TEMP_RESULTS_DIR.mkdir(exist_ok=True)  # Create directory if it doesn't exist

def connect_to_mongodb(max_retries=3, retry_delay=10):  # Increased delay
    """Network-resilient MongoDB connection for Atlas"""
    
    mongodb_uri = os.getenv("MONGODB_URI")
    mongodb_db_name = os.getenv("MONGODB_DB_NAME")
    
    if not mongodb_uri:
        raise ValueError("❌ MONGODB_URI environment variable is required")
    if not mongodb_db_name:
        raise ValueError("❌ MONGODB_DB_NAME environment variable is required")
    
    for attempt in range(max_retries):
        try:
            print(f"🔌 Connecting to MongoDB (attempt {attempt + 1}/{max_retries})")
            
            # ✅ NETWORK-RESILIENT: Better settings for poor connectivity
            client = MongoClient(
                mongodb_uri,
                # Connection timeouts
                serverSelectionTimeoutMS=60000,    # 60 seconds (increased)
                connectTimeoutMS=120000,            # 120 seconds (increased)
                socketTimeoutMS=120000,            # 2 minutes (increased)
                
                # Network resilience
                retryWrites=True,
                retryReads=True,                   # Also retry reads
                
                # Read preference - allow secondary reads
                readPreference='secondaryPreferred',  # Use secondary if primary fails
                
                # Connection pool
                maxPoolSize=5,                     # Reduced pool size
                minPoolSize=1,
                maxIdleTimeMS=300000,              # 5 minutes
                
                # TLS/SSL settings for Atlas
                tls=True,
                tlsAllowInvalidCertificates=False,
                
                # Heartbeat settings
                heartbeatFrequencyMS=30000,        # 30 seconds
                
                # Write concern
                w='majority',
                journal=True
            )
            
            # Test connection with longer timeout
            print("🔄 Testing connection...")
            client.admin.command('ping', maxTimeMS=60000)
            
            # Test database access
            db = client[mongodb_db_name]
            db.command('ping')
            
            print(f"✅ MongoDB connected successfully!")
            print(f"📊 Server info: {client.server_info()['version']}")
            
            return client, db
            
        except Exception as e:
            print(f"❌ MongoDB connection failed (attempt {attempt + 1}): {e}")
            
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"💥 All {max_retries} connection attempts failed!")
                
                # ✅ PROVIDE TROUBLESHOOTING INFO
                print("\n🔧 TROUBLESHOOTING TIPS:")
                print("1. Check your internet connection")
                print("2. Verify MongoDB Atlas cluster is running")
                print("3. Check firewall/antivirus blocking port 27017")
                print("4. Try connecting from MongoDB Compass")
                print("5. Verify your IP is whitelisted in Atlas")
                
                raise ConnectionFailure(f"Could not connect to MongoDB after {max_retries} attempts")

# ✅ Connect with retry
try:
    client, db = connect_to_mongodb()
    parent_jobs_collection = db["parent_jobs"]
    child_jobs_collection = db["child_jobs"]
except Exception as e:
    print(f"💥 CRITICAL: Cannot start application without MongoDB: {e}")
    exit(1)  # Exit if we can't connect



API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
# ScrapingBee endpoint
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"


def simple_db_retry(operation, max_retries=3):
    """Simple retry for transient MongoDB issues - NO connection recreation"""
    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s

def save_completed_job_locally(job_id, job_data):
    """Save completed job result to local file temporarily"""
    try:
        temp_file = TEMP_RESULTS_DIR / f"{job_id}.json"
        
        # Add completion timestamp
        job_data['local_saved_at'] = time.time()
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(job_data, f, indent=2, default=str)
        
        print(f"💾 Saved job {job_id} to local storage: {temp_file}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to save job {job_id} locally: {e}")
        return False

def get_local_completed_jobs():
    """Get list of all locally stored completed jobs"""
    try:
        completed_files = list(TEMP_RESULTS_DIR.glob("*.json"))
        return [f.stem for f in completed_files]  # Return job IDs (filenames without .json)
    except Exception as e:
        print(f"❌ Error reading local completed jobs: {e}")
        return []

def load_completed_job_from_local(job_id):
    """Load a completed job from local storage"""
    try:
        temp_file = TEMP_RESULTS_DIR / f"{job_id}.json"
        
        if not temp_file.exists():
            return None
        
        with open(temp_file, 'r', encoding='utf-8') as f:
            job_data = json.load(f)
        
        return job_data
        
    except Exception as e:
        print(f"❌ Failed to load job {job_id} from local storage: {e}")
        return None

def delete_local_completed_job(job_id):
    """Delete completed job from local storage after MongoDB update"""
    try:
        temp_file = TEMP_RESULTS_DIR / f"{job_id}.json"
        
        if temp_file.exists():
            temp_file.unlink()
            print(f"🗑️ Deleted local file: {temp_file}")
            return True
        
        return False
        
    except Exception as e:
        print(f"❌ Failed to delete local job {job_id}: {e}")
        return False

def batch_update_mongodb():
    """Update MongoDB with completed jobs when we have enough batched"""
    try:
        local_jobs = get_local_completed_jobs()
        
        if len(local_jobs) < BATCH_SIZE:
            print(f"📊 Local completed jobs: {len(local_jobs)}/{BATCH_SIZE} - waiting for more")
            return False
        
        print(f"🚀 BATCH UPDATE: Processing {len(local_jobs)} completed jobs to MongoDB")
        
        # Take the first BATCH_SIZE jobs for processing
        jobs_to_process = local_jobs[:BATCH_SIZE]
        successful_updates = []
        failed_updates = []
        
        for job_id in jobs_to_process:
            try:
                # Load job data from local storage
                job_data = load_completed_job_from_local(job_id)
                
                if not job_data:
                    print(f"❌ Could not load job {job_id} from local storage")
                    failed_updates.append(job_id)
                    continue
                
                # Update MongoDB with completed job
                def _update_job_in_mongodb():
                    return child_jobs_collection.update_one(
                        {"job_id": job_id},
                        {"$set": {
                            "status": job_data['status'],
                            "completed_at": job_data['completed_at'],
                            "result": job_data['result'],
                            "credits_used": job_data['credits_used'],
                            "processing_duration": job_data['processing_duration'],
                            "batch_updated_at": time.time()  # Track when batch update happened
                        }}
                    )
                
                update_result = simple_db_retry(_update_job_in_mongodb)  # ✅ NEW METHOD
                
                if update_result.modified_count > 0:
                    # Successfully updated MongoDB, delete local file
                    if delete_local_completed_job(job_id):
                        successful_updates.append(job_id)
                        print(f"✅ Batch updated job {job_id} to MongoDB")
                    else:
                        print(f"⚠️ Updated MongoDB but failed to delete local file for {job_id}")
                        successful_updates.append(job_id)
                else:
                    print(f"⚠️ No MongoDB update for job {job_id} (job may not exist)")
                    failed_updates.append(job_id)
                
            except Exception as e:
                print(f"❌ Failed to batch update job {job_id}: {e}")
                failed_updates.append(job_id)
        
        print(f"📊 BATCH UPDATE COMPLETE:")
        print(f"   ✅ Successful: {len(successful_updates)} jobs")
        print(f"   ❌ Failed: {len(failed_updates)} jobs")
        
        if successful_updates:
            # ✅ REMOVED: Don't call check_parent_completions here to avoid recursion
            # The parent completion check will happen in the main worker loop
            pass
        
        return len(successful_updates) > 0
        
    except Exception as e:
        print(f"❌ Error in batch_update_mongodb: {e}")
        return False
    
def force_batch_update_mongodb():
    """Force update MongoDB with ALL completed jobs regardless of batch size"""
    try:
        local_jobs = get_local_completed_jobs()
        
        if len(local_jobs) == 0:
            print(f"📊 No local completed jobs to update")
            return False
        
        print(f"🚀 FORCE BATCH UPDATE: Processing {len(local_jobs)} completed jobs to MongoDB")
        
        # Process ALL jobs, not just BATCH_SIZE
        successful_updates = []
        failed_updates = []
        
        for job_id in local_jobs:
            try:
                # Load job data from local storage
                job_data = load_completed_job_from_local(job_id)
                
                if not job_data:
                    print(f"❌ Could not load job {job_id} from local storage")
                    failed_updates.append(job_id)
                    continue
                
                # Update MongoDB with completed job
                def _update_job_in_mongodb():
                    return child_jobs_collection.update_one(
                        {"job_id": job_id},
                        {"$set": {
                            "status": job_data['status'],
                            "completed_at": job_data['completed_at'],
                            "result": job_data['result'],
                            "credits_used": job_data['credits_used'],
                            "processing_duration": job_data['processing_duration'],
                            "force_updated_at": time.time()  # Track forced update
                        }}
                    )
                
                update_result = simple_db_retry(_update_job_in_mongodb)  # ✅ NEW METHOD
                
                if update_result.modified_count > 0:
                    # Successfully updated MongoDB, delete local file
                    if delete_local_completed_job(job_id):
                        successful_updates.append(job_id)
                        print(f"✅ Force updated job {job_id} to MongoDB")
                    else:
                        print(f"⚠️ Updated MongoDB but failed to delete local file for {job_id}")
                        successful_updates.append(job_id)
                else:
                    print(f"⚠️ No MongoDB update for job {job_id} (job may not exist)")
                    failed_updates.append(job_id)
                
            except Exception as e:
                print(f"❌ Failed to force update job {job_id}: {e}")
                failed_updates.append(job_id)
        
        print(f"📊 FORCE BATCH UPDATE COMPLETE:")
        print(f"   ✅ Successful: {len(successful_updates)} jobs")
        print(f"   ❌ Failed: {len(failed_updates)} jobs")
        
        return len(successful_updates) > 0
        
    except Exception as e:
        print(f"❌ Error in force_batch_update_mongodb: {e}")
        return False

def scrape_complete_homepage(url, use_js_render='true', use_premium_proxy='false', max_retries=1):
    """
    Scrape homepage with retry logic and extended timeout, then format to single key
    Immediately skip 404/400 errors without retrying
    """
    params = {
        'api_key': API_KEY,
        'url': url,
        'wait': 5000
    }
    
    # Add JavaScript rendering parameter if needed
    if use_js_render.lower() == 'true':
        params['render_js'] = 'true'
    
    # Add premium proxy parameter if needed
    if use_premium_proxy.lower() == 'true':
        params['premium_proxy'] = 'true'

    last_error = None
    
    # Retry loop - try up to max_retries times
    for attempt in range(max_retries):
        try:
            # Extended timeout: 60s connect, 160s read (3 minutes 40 sec total)
            response = requests.get(
                SCRAPINGBEE_URL, 
                params=params,
                timeout=(60, 160)
            )

            # 🚨 CHECK FOR 404/400 ERRORS - IMMEDIATE SKIP
            if response.status_code == 404:
                error_msg = f"404 Not Found: {url}"
                return {
                    'success': False,
                    'error': error_msg,
                    'error_type': '404_not_found',
                    'skip_retry': True,
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }
            
            elif response.status_code == 400:
                error_msg = f"400 Bad Request: {url}"
                return {
                    'success': False,
                    'error': error_msg,
                    'error_type': '400_bad_request', 
                    'skip_retry': True,
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }

            elif response.status_code != 200:
                error_msg = f"ScrapingBee failed: {response.status_code}"
                last_error = {"error": error_msg, "details": response.text, "attempt": attempt + 1}
                
                if attempt == max_retries - 1:
                    return {
                        'success': False,
                        'error': error_msg,
                        'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                    }
                
                wait_time = attempt + 1
                time.sleep(wait_time)
                continue

            # Parse the response
            soup = BeautifulSoup(response.text, 'html.parser')

            data = {
                'url': url,
                'page_info': {
                    'title': soup.title.text.strip() if soup.title else 'No title found',
                    'meta_description': '',
                    'meta_keywords': ''
                },
                'header': {
                    'logo': '',
                    'navigation': []
                },
                'main_content': {
                    'headings': {'h1': [], 'h2': [], 'h3': []},
                    'paragraphs': [],
                    'sections': []
                },
                'sidebar': [],
                'footer': {
                    'copyright': '',
                    'links': [],
                    'contact': {}
                },
                'images': [],
                'forms': []
            }

            # Meta information
            desc = soup.find('meta', {'name': 'description'})
            if desc: data['page_info']['meta_description'] = desc.get('content', '')
            keywords = soup.find('meta', {'name': 'keywords'})
            if keywords: data['page_info']['meta_keywords'] = keywords.get('content', '')

            # Header section
            header = soup.find('header') or soup.find(class_=lambda c: c and 'header' in str(c).lower())
            if header:
                logo = header.find('img')
                if logo: data['header']['logo'] = logo.get('src', '')
                nav = header.find('nav') or header.find(class_=lambda c: c and 'nav' in str(c).lower())
                if nav:
                    for link in nav.find_all('a'):
                        href = link.get('href', '')
                        text = link.get_text(strip=True)
                        if href and text:
                            data['header']['navigation'].append({'text': text, 'url': href})

            # Headings
            for level in ['h1', 'h2', 'h3']:
                for tag in soup.find_all(level):
                    text = tag.get_text(strip=True)
                    if text:
                        data['main_content']['headings'][level].append(text)

            # Paragraphs
            for p in soup.find_all('p'):
                text = p.get_text(strip=True)
                if text:
                    data['main_content']['paragraphs'].append(text)

            # Content sections
            content_areas = [
                soup.find(id='content'),
                soup.find(class_='content'),
                soup.find(class_='main-content'),
                soup.find('main'),
                soup.find(class_='entry-content')
            ]
            for div in soup.find_all('div'):
                if div.find_parent(['header', 'footer', 'nav']):
                    continue
                classes = div.get('class', [])
                if classes and any(c in ' '.join(classes).lower() for c in ['content', 'section', 'container']):
                    content_areas.append(div)

            for area in content_areas:
                if not area:
                    continue
                section_data = {
                    'type': area.name,
                    'id': area.get('id', ''),
                    'classes': area.get('class', []),
                    'text': area.get_text(strip=True),
                    'children': []
                }
                for child in area.children:
                    if hasattr(child, 'name') and child.name not in ['script', 'style']:
                        text = child.get_text(strip=True)
                        if text:
                            section_data['children'].append({
                                'element': child.name,
                                'text': text[:500] + ('...' if len(text) > 500 else '')
                            })
                if section_data['children']:
                    data['main_content']['sections'].append(section_data)

            # Sidebar
            sidebar = soup.find(id='sidebar') or soup.find(class_='sidebar')
            if sidebar:
                for block in sidebar.find_all(['div', 'section', 'widget']):
                    text = block.get_text(strip=True)
                    if text:
                        data['sidebar'].append({'element': block.name, 'text': text})

            # Footer
            footer = soup.find('footer') or soup.find(class_=lambda c: c and 'footer' in str(c).lower())
            if footer:
                copyright_text = footer.find(string=lambda t: t and ('©' in t or 'copyright' in t.lower()))
                if copyright_text:
                    data['footer']['copyright'] = copyright_text.strip()
                for link in footer.find_all('a'):
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    if href and text:
                        data['footer']['links'].append({'text': text, 'url': href})
                email = footer.find(string=lambda t: t and '@' in t and '.' in t.split('@')[1])
                if email:
                    data['footer']['contact']['email'] = email.strip()
                phone = footer.find(string=lambda t: t and re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', t))
                if phone:
                    data['footer']['contact']['phone'] = phone.strip()

            # Images
            for img in soup.find_all('img'):
                src = img.get('src', '')
                alt = img.get('alt', '')
                if src:
                    data['images'].append({'src': src, 'alt': alt})

            # Forms
            for form in soup.find_all('form'):
                form_data = {
                    'action': form.get('action', ''),
                    'method': form.get('method', ''),
                    'fields': []
                }
                for field in form.find_all(['input', 'textarea', 'select']):
                    name = field.get('name', '')
                    placeholder = field.get('placeholder', '')
                    field_type = field.get('type', field.name)
                    if name or placeholder:
                        form_data['fields'].append({
                            'type': field_type,
                            'name': name,
                            'placeholder': placeholder
                        })
                if form_data['fields']:
                    data['forms'].append(form_data)

            # Add retry and credit information to the structured data
            data['retry_info'] = {
                'attempts_made': attempt + 1,
                'successful': True
            }
            data['credits_used'] = calculate_credits(use_js_render, use_premium_proxy)
            data['extraction_method'] = 'manual_parsing'

            # Format the structured data into single key format
            formatted_result = format_to_single_key(data)
            
            # Mark as successful and return
            formatted_result['success'] = True
            return formatted_result

        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout on attempt {attempt + 1}: {str(e)}"
            last_error = {"error": error_msg, "timeout": True, "attempt": attempt + 1}
            
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'error': error_msg,
                    'retry_info': {
                        'attempts_made': max_retries,
                        'successful': False,
                        'final_error': 'timeout_after_retries'
                    },
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }
            
            wait_time = attempt + 1
            time.sleep(wait_time)
            
        except requests.exceptions.ConnectTimeout as e:
            error_msg = f"Connection timeout on attempt {attempt + 1}: {str(e)}"
            last_error = {"error": error_msg, "timeout": True, "attempt": attempt + 1}
            
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'error': error_msg,
                    'retry_info': {
                        'attempts_made': max_retries,
                        'successful': False,
                        'final_error': 'connect_timeout_after_retries'
                    },
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }
            
            wait_time = attempt + 1
            time.sleep(wait_time)
            
        except requests.exceptions.ReadTimeout as e:
            error_msg = f"Read timeout on attempt {attempt + 1}: {str(e)}"
            last_error = {"error": error_msg, "timeout": True, "attempt": attempt + 1}
            
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'error': error_msg,
                    'retry_info': {
                        'attempts_made': max_retries,
                        'successful': False,
                        'final_error': 'read_timeout_after_retries'
                    },
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }
            
            wait_time = attempt + 1
            time.sleep(wait_time)
            
        except Exception as e:
            error_msg = f"Unexpected error on attempt {attempt + 1}: {str(e)}"
            last_error = {"error": error_msg, "attempt": attempt + 1}
            
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'error': error_msg,
                    'retry_info': {
                        'attempts_made': max_retries,
                        'successful': False,
                        'final_error': 'unexpected_error_after_retries'
                    },
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }
            
            wait_time = attempt + 1
            time.sleep(wait_time)

    # This should never be reached, but just in case
    return {
        'success': False,
        'error': last_error.get('error', 'Unknown error occurred') if last_error else 'Unknown error occurred',
        'retry_info': {
            'attempts_made': max_retries,
            'successful': False
        },
        'credits_used': calculate_credits(use_js_render, use_premium_proxy)
    }

    
def format_to_single_key(data):
    """
    Format structured data into a single key containing the entire page content
    as it appears on the webpage - shows ALL images without filtering
    """
    
    # Build the complete page content as a single string
    page_content = []
    
    # Determine data format
    if 'page_title' in data:  # AI format (from AI extraction)
        # Add page title
        if data.get('page_title'):
            page_content.append(f"PAGE TITLE: {data['page_title']}")
            page_content.append("")
        
        # Add meta description if available
        if data.get('meta_description'):
            page_content.append(f"META DESCRIPTION: {data['meta_description']}")
            page_content.append("")
        
        # Add navigation links
        nav_links = data.get('navigation_links', [])
        if nav_links:
            page_content.append("NAVIGATION:")
            for nav_item in nav_links:
                page_content.append(f"  • {nav_item}")
            page_content.append("")
        
        # Add main headings (H1)
        h1_list = data.get('h1_headings', [])
        if h1_list:
            page_content.append("MAIN HEADINGS:")
            for h1 in h1_list:
                page_content.append(f"  {h1}")
            page_content.append("")
        
        # Add section headings (H2)
        h2_list = data.get('h2_headings', [])
        if h2_list:
            page_content.append("SECTION HEADINGS:")
            for h2 in h2_list:
                page_content.append(f"  {h2}")
            page_content.append("")
        
        # Add sub-section headings (H3)
        h3_list = data.get('h3_headings', [])
        if h3_list:
            page_content.append("SUB-SECTION HEADINGS:")
            for h3 in h3_list:
                page_content.append(f"  {h3}")
            page_content.append("")
        
        # Add main content
        main_content = data.get('main_content_text', [])
        if main_content:
            page_content.append("MAIN CONTENT:")
            for i, content in enumerate(main_content, 1):
                content_text = content[:500] + ('...' if len(content) > 500 else '')
                page_content.append(f"  {i}. {content_text}")
            page_content.append("")
        
        # Add footer text
        footer_text = data.get('footer_text', '')
        footer_links = data.get('footer_links', [])
        if footer_text or footer_links:
            page_content.append("FOOTER:")
            if footer_text:
                page_content.append(f"  {footer_text}")
            if footer_links:
                footer_links_text = ', '.join(footer_links[:10])  # Limit footer links
                page_content.append(f"  Footer Links: {footer_links_text}")
            page_content.append("")
        
        # SIMPLE: Show ALL images - no filtering whatsoever
        images = data.get('all_images', [])
        if images:
            page_content.append(f"IMAGES: {len(images)} images found on the page")
            for i, img in enumerate(images, 1):  # Show ALL images
                page_content.append(f"  {i}. {img}")
            page_content.append("")
        
        # Add forms
        forms = data.get('all_forms', [])
        if forms:
            page_content.append(f"FORMS: {len(forms)} forms found on the page")
            for i, form in enumerate(forms[:3], 1):  # Keep form limit as forms can be very long
                form_text = form[:200] + ('...' if len(form) > 200 else '')
                page_content.append(f"  {i}. {form_text}")
            if len(forms) > 3:
                page_content.append(f"  ... and {len(forms) - 3} more forms")
            page_content.append("")
    
    else:  # Structured format (manual extraction fallback)
        # Add page title
        if data.get('page_info', {}).get('title'):
            page_content.append(f"PAGE TITLE: {data['page_info']['title']}")
            page_content.append("")
        
        # Add meta description if available
        if data.get('page_info', {}).get('meta_description'):
            page_content.append(f"META DESCRIPTION: {data['page_info']['meta_description']}")
            page_content.append("")
        
        # Add header/navigation content
        header_nav = data.get('header', {}).get('navigation', [])
        if header_nav:
            page_content.append("NAVIGATION:")
            for nav_item in header_nav:
                if isinstance(nav_item, dict):
                    page_content.append(f"  • {nav_item.get('text', '')}")
                else:
                    page_content.append(f"  • {nav_item}")
            page_content.append("")
        
        # Add main headings in order (H1, H2, H3)
        headings = data.get('main_content', {}).get('headings', {})
        
        # H1 headings
        h1_list = headings.get('h1', [])
        if h1_list:
            page_content.append("MAIN HEADINGS:")
            for h1 in h1_list:
                page_content.append(f"  {h1}")
            page_content.append("")
        
        # H2 headings
        h2_list = headings.get('h2', [])
        if h2_list:
            page_content.append("SECTION HEADINGS:")
            for h2 in h2_list:
                page_content.append(f"  {h2}")
            page_content.append("")
        
        # H3 headings
        h3_list = headings.get('h3', [])
        if h3_list:
            page_content.append("SUB-SECTION HEADINGS:")
            for h3 in h3_list:
                page_content.append(f"  {h3}")
            page_content.append("")
        
        # Add main content paragraphs
        paragraphs = data.get('main_content', {}).get('paragraphs', [])
        if paragraphs:
            page_content.append("MAIN CONTENT:")
            for i, paragraph in enumerate(paragraphs, 1):
                # Limit paragraph length for readability
                para_text = paragraph[:500] + ('...' if len(paragraph) > 500 else '')
                page_content.append(f"  {i}. {para_text}")
            page_content.append("")
        
        # Add sidebar content
        sidebar = data.get('sidebar', [])
        if sidebar:
            page_content.append("SIDEBAR CONTENT:")
            for item in sidebar:
                if isinstance(item, dict):
                    text = item.get('text', '')[:300] + ('...' if len(item.get('text', '')) > 300 else '')
                    page_content.append(f"  • {text}")
                else:
                    page_content.append(f"  • {str(item)[:300]}")
            page_content.append("")
        
        # Add footer information
        footer = data.get('footer', {})
        footer_content = []
        
        # Footer copyright
        if footer.get('copyright'):
            footer_content.append(f"Copyright: {footer['copyright']}")
        
        # Footer links
        footer_links = footer.get('links', [])
        if footer_links:
            links_text = []
            for link in footer_links[:10]:  # Limit to first 10 links
                if isinstance(link, dict):
                    if link.get('url'):
                        links_text.append(link.get('url'))
                    else:
                        links_text.append(link.get('text', ''))
                else:
                    links_text.append(str(link))
            if links_text:
                footer_content.append(f"Footer Links: {', '.join(links_text)}")
        
        # Footer contact
        contact = footer.get('contact', {})
        if contact.get('email'):
            footer_content.append(f"Contact Email: {contact['email']}")
        if contact.get('phone'):
            footer_content.append(f"Contact Phone: {contact['phone']}")
        
        if footer_content:
            page_content.append("FOOTER:")
            for item in footer_content:
                page_content.append(f"  {item}")
            page_content.append("")
        
        # SIMPLE: Show ALL images - no filtering whatsoever
        images = data.get('images', [])
        if images:
            page_content.append(f"IMAGES: {len(images)} images found on the page")
            for i, img in enumerate(images, 1):  # Show ALL images
                if isinstance(img, dict):
                    src = img.get('src', '')
                    alt = img.get('alt', '')
                    if alt:
                        page_content.append(f"  {i}. {src} (Alt: {alt})")
                    else:
                        page_content.append(f"  {i}. {src}")
                else:
                    page_content.append(f"  {i}. {img}")
            page_content.append("")
        
        # Add forms information
        forms = data.get('forms', [])
        if forms:
            page_content.append(f"FORMS: {len(forms)} forms found on the page")
            for i, form in enumerate(forms[:3], 1):  # Keep form limit
                if isinstance(form, dict):
                    action = form.get('action', 'No action')
                    method = form.get('method', 'GET')
                    fields = form.get('fields', [])
                    page_content.append(f"  {i}. Form (Action: {action}, Method: {method}, Fields: {len(fields)})")
                else:
                    form_text = str(form)[:200] + ('...' if len(str(form)) > 200 else '')
                    page_content.append(f"  {i}. {form_text}")
            if len(forms) > 3:
                page_content.append(f"  ... and {len(forms) - 3} more forms")
            page_content.append("")
    
    # Join all content with newlines
    complete_content = "\n".join(page_content).strip()
    
    # ✅ CHANGED: Return in your expected API format
    return {
        "page_content": complete_content,
        "metadata": {
            "url": data.get('url', ''),
            "extraction_method": "manual_parsing",
            "retry_info": {
                "attempts_made": 1,
                "successful": True
            },
            "credits_used": data.get('credits_used', 1)
        },
        "success": True
    }
                                                                                                                                               


def calculate_credits(js_render, premium_proxy):
    """Calculate the credit cost based on the configuration"""
    js = js_render.lower() == 'true'
    premium = premium_proxy.lower() == 'true'
    
    if premium:
        if js:
            return 25  # Premium proxy with JS rendering
        else:
            return 10  # Premium proxy without JS rendering
    elif js:
        return 5  # With JS rendering
    else:
        return 1  # Base cost


def normalize_url(url):
    """Normalize URL by adding protocol if missing"""
    if not url:
        return url
    
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"  # ✅ NO trailing slash
    
    return url
    
def check_parent_completions():
    """Check all active parent jobs for completion"""
    try:
        # Get all active parent jobs
        active_parents = parent_jobs_collection.find({"status": {"$ne": "completed"}})
        
        for parent in active_parents:
            parent_job_id = parent["job_id"]
            
            # Count child job statuses
            total_children = child_jobs_collection.count_documents({
                "parent_job_id": parent_job_id
            })
            
            completed_children = child_jobs_collection.count_documents({
                "parent_job_id": parent_job_id,
                "status": "completed"
            })
            
            error_children = child_jobs_collection.count_documents({
                "parent_job_id": parent_job_id,
                "status": "error"
            })
            
            processing_children = child_jobs_collection.count_documents({
                "parent_job_id": parent_job_id,
                "status": "processing"
            })
            
            # ✅ NEW: If no jobs are processing and we have local jobs, force update
            if processing_children == 0:
                local_jobs = get_local_completed_jobs()
                if len(local_jobs) > 0:
                    print(f"🎯 No more jobs processing for {parent_job_id}, forcing update of {len(local_jobs)} local jobs")
                    force_batch_update_mongodb()
                    
                    # Recalculate after forced update
                    completed_children = child_jobs_collection.count_documents({
                        "parent_job_id": parent_job_id,
                        "status": "completed"
                    })
                    
                    error_children = child_jobs_collection.count_documents({
                        "parent_job_id": parent_job_id,
                        "status": "error"
                    })
            
            # ✅ ADD SAFETY CHECK: Only proceed if we have children and all are done
            if total_children > 0 and (completed_children + error_children) == total_children:
                
                completion_time = time.time()
                total_duration = completion_time - parent.get('submission_start_time', completion_time)
                
                # Calculate total credits properly from all child jobs
                child_results = child_jobs_collection.find({"parent_job_id": parent_job_id})
                total_credits = 0
                for child in child_results:
                    total_credits += child.get('credits_used', 0)
                
                # Update parent as completed in MongoDB
                parent_jobs_collection.update_one(
                    {"job_id": parent_job_id},
                    {"$set": {
                        "status": "completed",
                        "completed_at": completion_time,
                        "total_duration_seconds": round(total_duration, 2),
                        "summary": {
                            "total_jobs": total_children,
                            "successful": completed_children,
                            "errors": error_children,
                            "total_credits_used": total_credits
                        }
                    }}
                )
                
                print(f"🎯 PARENT JOB COMPLETED: {parent_job_id}")
                print(f"   ⏱️ Total time: {total_duration:.2f} seconds")
                print(f"   ✅ Successful: {completed_children}/{total_children}")
                print(f"   💰 Credits used: {total_credits}")
                
    except Exception as e:
        print(f"❌ Error checking parent completions: {str(e)}")


@app.route('/api/multiple_domains', methods=['GET'])
def submit_multiple_domains_job():
    """Submit multiple domain scraping job - Database Queue Approach"""
    try:
        submission_start_time = time.time()
        
        raw_urls = request.args.getlist('url')
        js_render = request.args.get('js_render', 'false')
        premium_proxy = request.args.get('premium_proxy', 'false')
        parent_job_id = request.args.get('job_id')
        
        if not raw_urls:
            return jsonify({"error": "No URLs provided"}), 400
        
        # ✅ NORMALIZE URLs - add protocols if missing
        urls = []
        invalid_urls = []
        
        for raw_url in raw_urls:
            try:
                normalized_url = normalize_url(raw_url)
                if normalized_url:
                    urls.append(normalized_url)
                    print(f"📝 Normalized: {raw_url} → {normalized_url}")
                else:
                    invalid_urls.append(raw_url)
            except Exception as e:
                print(f"❌ Invalid URL: {raw_url} - {e}")
                invalid_urls.append(raw_url)
        
        if not urls:
            return jsonify({"error": "No valid URLs provided after normalization"}), 400
        
        if invalid_urls:
            print(f"⚠️ Skipped {len(invalid_urls)} invalid URLs: {invalid_urls}")
        
        if not parent_job_id:
            parent_job_id = f"multi_{int(time.time())}"
        
        print(f"🎯 MULTI-DOMAIN JOB STARTED: {parent_job_id} with {len(urls)} valid URLs")
        
        # ✅ Calculate deletion time (24 hours from now)
        delete_at = datetime.now(timezone.utc) + timedelta(hours=24)
        
        # Create parent job in MongoDB
        parent_job = {
            "job_id": parent_job_id,
            "status": "queued",
            "total_urls": len(urls),
            "js_render": js_render,
            "premium_proxy": premium_proxy,
            "submitted_at": time.time(),
            "submission_start_time": submission_start_time,
            "estimated_credits": calculate_credits(js_render, premium_proxy) * len(urls),
            "delete_at": delete_at,
            "original_urls": raw_urls,      # ✅ Keep track of original URLs
            "normalized_urls": urls,        # ✅ Keep track of normalized URLs
            "invalid_urls": invalid_urls    # ✅ Keep track of invalid URLs
        }
        parent_jobs_collection.insert_one(parent_job)
        print(f"📝 Created parent job: {parent_job_id}")
        
        # ✅ Save ALL child jobs to MongoDB with proper queue priority
        child_job_ids = []
        for i, url in enumerate(urls):
            child_job_id = f"{parent_job_id}_url{i}"
            child_job = {
                "job_id": child_job_id,
                "parent_job_id": parent_job_id,
                "url": url,  # ✅ Now using normalized URL with https://
                "js_render": js_render,
                "premium_proxy": premium_proxy,
                "status": "queued",
                "queue_priority": i,
                "submitted_at": time.time(),
                "processing_started_at": None,
                "completed_at": None,
                "result": None,
                "credits_used": 0,
                "retry_count": 0,
                "job_type": "multi-domain",
                "delete_at": delete_at
            }
            
            child_jobs_collection.insert_one(child_job)
            child_job_ids.append(child_job_id)
        
        submission_end_time = time.time()
        submission_duration = submission_end_time - submission_start_time
        
        print(f"📋 Saved {len(child_job_ids)} jobs to MongoDB queue with priorities 0-{len(urls)-1}")
        print(f"🕒 SUBMISSION TIME: {submission_duration:.3f} seconds for {len(urls)} URLs")
        
        return jsonify({
            "message": f"Multi-domain job {parent_job_id} submitted successfully",
            "parent_job_id": parent_job_id,
            "total_urls": len(urls),
            "valid_urls": len(urls),
            "invalid_urls": len(invalid_urls),
            "child_job_ids": child_job_ids,
            "estimated_credits": parent_job["estimated_credits"],
            "status": "queued",
            "submission_time_seconds": round(submission_duration, 3),
            "processing_order": f"url0 -> url1 -> ... -> url{len(urls)-1}",
            "auto_delete_at": delete_at.isoformat(),
            "url_normalization": {
                "original_urls": raw_urls,
                "normalized_urls": urls,
                "invalid_urls": invalid_urls
            }
        })
        
    except Exception as e:
        print(f"❌ ERROR in submit_multiple_domains_job: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
def database_worker():
    """Simple continuous processing with file cleanup when idle"""
    print("🚀 Database worker started")
    
    max_concurrent = 10
    executor = ThreadPoolExecutor(max_workers=max_concurrent)
    active_futures = {}
    
    while worker_running:
        try:
            # Fill available slots with queued jobs
            while len(active_futures) < max_concurrent and worker_running:
                # ✅ SIMPLE RETRY - no function wrapper
                job = simple_db_retry(lambda: child_jobs_collection.find_one_and_update(
                    {"status": "queued"},
                    {"$set": {
                        "status": "processing",
                        "processing_started_at": time.time(),
                        "worker_claimed_at": time.time()
                    }},
                    sort=[("submitted_at", 1), ("queue_priority", 1), ("job_id", 1)]
                ))
                
                if not job:
                    break
                
                future = executor.submit(process_database_job, job["job_id"])
                active_futures[future] = job["job_id"]
                print(f"🎬 Started job {job['job_id']} (slot {len(active_futures)}/{max_concurrent})")
            
            # Check for completed jobs
            if active_futures:
                completed_futures = []
                for future in list(active_futures.keys()):
                    if future.done():
                        completed_futures.append(future)
                
                for future in completed_futures:
                    job_id = active_futures[future]
                    try:
                        future.result()
                        print(f"✅ Completed job {job_id} - slot freed ({len(active_futures)-1}/{max_concurrent})")
                    except Exception as e:
                        print(f"❌ Job {job_id} failed: {str(e)}")
                        # ✅ SIMPLE RETRY
                        simple_db_retry(lambda: child_jobs_collection.update_one(
                            {"job_id": job_id},
                            {"$set": {
                                "status": "error",
                                "completed_at": time.time(),
                                "result": {"error": f"Job execution failed: {str(e)}"}
                            }}
                        ))
                    
                    del active_futures[future]
                
                if completed_futures:
                    # ✅ SIMPLE CALL - no retry wrapper
                    check_parent_completions()
            
            # Cleanup when idle
            if len(active_futures) == 0:
                local_jobs = get_local_completed_jobs()
                if len(local_jobs) > 0:
                    print(f"🧹 No jobs running - cleaning up {len(local_jobs)} local files")
                    force_batch_update_mongodb()
                    check_parent_completions()
            
            # Sleep based on activity
            time.sleep(0.1 if len(active_futures) > 0 else 1)
                
        except Exception as e:
            print(f"❌ Database worker error: {str(e)}")
            time.sleep(5)  # Simple fixed delay
    
    # Cleanup
    executor.shutdown(wait=True)
    print("🛑 Database worker stopped")


def process_database_job(job_id, max_retries=2):
    """Process a single job - minimal retry logic"""
    try:
        print(f"🚀 STARTING DATABASE JOB: {job_id}")
        
        # ✅ SIMPLE RETRY
        job = simple_db_retry(lambda: child_jobs_collection.find_one({"job_id": job_id}))
        
        if not job:
            print(f"❌ Job {job_id} not found in database")
            return
        
        job_start_time = time.time()
        
        # Job-level retry loop
        for attempt in range(max_retries):
            try:
                print(f"🔄 Job attempt {attempt + 1}/{max_retries} for {job_id}")
                
                # ✅ SIMPLE UPDATE
                simple_db_retry(lambda: child_jobs_collection.update_one(
                    {"job_id": job_id},
                    {"$set": {"retry_count": attempt + 1}}
                ))
                
                # Call scraping function
                result = scrape_complete_homepage(
                    job['url'],
                    job['js_render'],
                    job['premium_proxy'],
                    max_retries=1
                )
                
                # Handle successful result
                if isinstance(result, dict) and result.get('success'):
                    job_end_time = time.time()
                    total_duration = job_end_time - job_start_time
                    
                    print(f"✅ JOB COMPLETED: {job_id} ({total_duration:.2f}s) - saving locally")
                    
                    credits_used = result.get('credits_used', calculate_credits(job['js_render'], job['premium_proxy']))
                    
                    completed_job_data = {
                        "job_id": job_id,
                        "status": "completed",
                        "completed_at": job_end_time,
                        "result": result,
                        "credits_used": credits_used,
                        "processing_duration": total_duration,
                        "url": job['url'],
                        "js_render": job['js_render'],
                        "premium_proxy": job['premium_proxy'],
                        "parent_job_id": job.get('parent_job_id')
                    }
                    
                    # Save locally and trigger batch update
                    if save_completed_job_locally(job_id, completed_job_data):
                        batch_update_mongodb()
                    else:
                        # ✅ FALLBACK - simple retry
                        simple_db_retry(lambda: child_jobs_collection.update_one(
                            {"job_id": job_id},
                            {"$set": {
                                "status": "completed",
                                "completed_at": job_end_time,
                                "result": result,
                                "credits_used": credits_used,
                                "processing_duration": total_duration
                            }}
                        ))
                    
                    return
                
                # Handle skip errors
                elif isinstance(result, dict) and result.get('skip_retry'):
                    job_end_time = time.time()
                    print(f"🚫 JOB SKIPPED: {job_id} - {result.get('error_type')} - saving locally")
                    
                    credits_used = result.get('credits_used', calculate_credits(job['js_render'], job['premium_proxy']))
                    
                    completed_job_data = {
                        "job_id": job_id,
                        "status": "error",
                        "completed_at": job_end_time,
                        "result": result,
                        "credits_used": credits_used,
                        "processing_duration": job_end_time - job_start_time,
                        "url": job['url'],
                        "js_render": job['js_render'],
                        "premium_proxy": job['premium_proxy'],
                        "parent_job_id": job.get('parent_job_id')
                    }
                    
                    if save_completed_job_locally(job_id, completed_job_data):
                        batch_update_mongodb()
                    else:
                        # ✅ FALLBACK - simple retry
                        simple_db_retry(lambda: child_jobs_collection.update_one(
                            {"job_id": job_id},
                            {"$set": {
                                "status": "error",
                                "completed_at": job_end_time,
                                "result": result,
                                "credits_used": credits_used,
                                "processing_duration": job_end_time - job_start_time
                            }}
                        ))
                    
                    return
                
                # Retry for other errors
                else:
                    error_msg = result.get('error', 'Unknown error') if isinstance(result, dict) else str(result)
                    print(f"❌ Job attempt {attempt + 1} failed for {job_id}: {error_msg}")
                    
                    if attempt == max_retries - 1:
                        # Final failure
                        job_end_time = time.time()
                        credits_used = 0
                        
                        completed_job_data = {
                            "job_id": job_id,
                            "status": "error",
                            "completed_at": job_end_time,
                            "result": {"error": f"Failed after {max_retries} attempts: {error_msg}"},
                            "credits_used": credits_used,
                            "processing_duration": job_end_time - job_start_time,
                            "url": job['url'],
                            "js_render": job['js_render'],
                            "premium_proxy": job['premium_proxy'],
                            "parent_job_id": job.get('parent_job_id')
                        }
                        
                        if save_completed_job_locally(job_id, completed_job_data):
                            batch_update_mongodb()
                        else:
                            # ✅ FALLBACK - simple retry
                            simple_db_retry(lambda: child_jobs_collection.update_one(
                                {"job_id": job_id},
                                {"$set": {
                                    "status": "error",
                                    "completed_at": job_end_time,
                                    "result": {"error": f"Failed after {max_retries} attempts: {error_msg}"},
                                    "credits_used": credits_used,
                                    "processing_duration": job_end_time - job_start_time
                                }}
                            ))
                        break
                    else:
                        time.sleep(attempt + 1)
                        
            except Exception as e:
                print(f"💥 Job {job_id} attempt {attempt + 1} exception: {str(e)}")
                
                if attempt == max_retries - 1:
                    job_end_time = time.time()
                    
                    completed_job_data = {
                        "job_id": job_id,
                        "status": "error",
                        "completed_at": job_end_time,
                        "result": {"error": f"Exception after {max_retries} attempts: {str(e)}"},
                        "credits_used": 0,
                        "processing_duration": job_end_time - job_start_time,
                        "url": job.get('url', ''),
                        "js_render": job.get('js_render', 'false'),
                        "premium_proxy": job.get('premium_proxy', 'false'),
                        "parent_job_id": job.get('parent_job_id')
                    }
                    
                    if save_completed_job_locally(job_id, completed_job_data):
                        batch_update_mongodb()
                    else:
                        # ✅ FALLBACK - simple retry
                        simple_db_retry(lambda: child_jobs_collection.update_one(
                            {"job_id": job_id},
                            {"$set": {
                                "status": "error",
                                "completed_at": job_end_time,
                                "result": {"error": f"Exception after {max_retries} attempts: {str(e)}"},
                                "credits_used": 0,
                                "processing_duration": job_end_time - job_start_time
                            }}
                        ))
                else:
                    time.sleep(attempt + 1)
                    
    except Exception as e:
        print(f"💥 Critical error in process_database_job {job_id}: {str(e)}")
        # Handle critical error with simple retry
        simple_db_retry(lambda: child_jobs_collection.update_one(
            {"job_id": job_id},
            {"$set": {
                "status": "error",
                "completed_at": time.time(),
                "result": {"error": f"Critical error: {str(e)}"},
                "credits_used": 0,
                "processing_duration": 0
            }}
        ))


@app.route('/api/submit', methods=['GET'])
def submit_job():
    """Submit a single URL scraping job with TTL"""
    try:
        submission_start_time = time.time()
        
        # Get parameters
        raw_url = request.args.get('url')
        js_render = request.args.get('js_render', 'false')
        premium_proxy = request.args.get('premium_proxy', 'false')
        job_id = request.args.get('job_id')
        
        if not raw_url:
            return jsonify({"error": "URL parameter is required"}), 400
        
        # ✅ NORMALIZE URL
        url = normalize_url(raw_url)
        print(f"📝 Normalized: {raw_url} → {url}")
        
        if not job_id:
            job_id = f"single_{int(time.time())}"
        
        print(f"🎯 SINGLE JOB SUBMITTED: {job_id} for URL: {url}")
        
        # ✅ Calculate deletion time (24 hours from now)
        delete_at = datetime.now(timezone.utc) + timedelta(hours=24)
        
        single_job = {
            "job_id": job_id,
            "parent_job_id": None,
            "url": url,  # ✅ Using normalized URL
            "js_render": js_render,
            "premium_proxy": premium_proxy,
            "status": "queued",
            "queue_priority": 0,
            "submitted_at": time.time(),
            "processing_started_at": None,
            "completed_at": None,
            "result": None,
            "credits_used": 0,
            "retry_count": 0,
            "job_type": "single",
            "delete_at": delete_at
        }
        
        simple_db_retry(lambda: child_jobs_collection.insert_one(single_job))
        
        submission_end_time = time.time()
        submission_duration = submission_end_time - submission_start_time
        
        print(f"📋 Single job {job_id} added to MongoDB queue (auto-delete: {delete_at})")
        print(f"🕒 SUBMISSION TIME: {submission_duration:.3f} seconds")
        
        return jsonify({
            "job_id": job_id,
            "message": f"Job queued. Check status at /api/status?job_id={job_id}",
            "status": "queued",
            "url": url,  # ✅ Return normalized URL
            "original_url": raw_url,  # ✅ Show original URL too
            "auto_delete_at": delete_at.isoformat()
        })
        
    except Exception as e:
        print(f"❌ ERROR in submit_job: {str(e)}")
        return jsonify({"error": str(e)}), 500



@app.route('/api/queue', methods=['GET'])
def get_queue_info():
    """Get information about the current job queue - matches previous API format"""
    try:
        # Count jobs by status
        queued_jobs = child_jobs_collection.count_documents({"status": "queued"})
        processing_jobs = child_jobs_collection.count_documents({"status": "processing"})
        completed_jobs = child_jobs_collection.count_documents({"status": "completed"})
        
        # Get completed job IDs
        completed_job_docs = child_jobs_collection.find(
            {"status": "completed"}, 
            {"job_id": 1}
        )
        completed_job_ids = [job["job_id"] for job in completed_job_docs]
        
        # Get processing jobs details
        processing_job_docs = child_jobs_collection.find(
            {"status": "processing"}, 
            {"job_id": 1, "url": 1, "processing_started_at": 1}
        )
        processing_jobs_list = {}
        for job in processing_job_docs:
            processing_jobs_list[job["job_id"]] = {
                "url": job["url"],
                "processing_started_at": job.get("processing_started_at")
            }
        
        # Format response to match your previous API
        return jsonify({
            "queue_size": queued_jobs,
            "processing_jobs": processing_jobs,
            "completed_jobs": completed_jobs,
            "completed_job_ids": completed_job_ids,
            "processing_jobs_list": processing_jobs_list,
            "status_counts": {
                "queued": queued_jobs,
                "processing": processing_jobs,
                "completed": completed_jobs
            }
        })
        
    except Exception as e:
        print(f"❌ ERROR in get_queue_info: {str(e)}")
        return jsonify({"error": str(e)}), 500
    

@app.route('/api/status', methods=['GET'])
def get_job_status():
    """Get the status or result of a job by ID - matches previous API format"""
    job_id = request.args.get('job_id')
    
    if not job_id:
        return jsonify({"error": "No job_id provided. Use '?job_id=YOUR_JOB_ID' parameter"}), 400
    
    try:
        # ✅ FIXED: Check child job with simple retry
        child_job = simple_db_retry(lambda: child_jobs_collection.find_one({"job_id": job_id}))
        
        if child_job:
            # Format response to match your previous API
            response_data = {
                "job_id": job_id,
                "status": child_job['status'],
                "url": child_job['url'],
                "submitted_at": child_job.get('submitted_at')
            }
            
            # Add completion data if job is done
            if child_job['status'] in ['completed', 'error']:
                response_data.update({
                    "completed_at": child_job.get('completed_at'),
                    "credits_used": child_job.get('credits_used', 0)
                })
                
                # Add result data if successful
                if child_job['status'] == 'completed' and child_job.get('result'):
                    result = child_job['result']
                    
                    # Format result to match your previous structure
                    if isinstance(result, dict):
                        response_data["result"] = result
            
            return jsonify(response_data)
        
        # ✅ FIXED: Check parent job with simple retry
        parent_job = simple_db_retry(lambda: parent_jobs_collection.find_one({"job_id": job_id}))
        
        if parent_job:
            # ✅ FIXED: Get child jobs with simple retry
            child_jobs = simple_db_retry(lambda: list(child_jobs_collection.find({"parent_job_id": job_id})))
            
            response_data = {
                "job_id": job_id,
                "status": parent_job['status'],
                "type": "parent_job",
                "total_urls": parent_job.get('total_urls', 0),
                "submitted_at": parent_job.get('submitted_at')
            }
            
            if parent_job['status'] == 'completed':
                response_data.update({
                    "completed_at": parent_job.get('completed_at'),
                    "summary": parent_job.get('summary', {})
                })
            
            # Add child job details
            response_data["child_jobs"] = [
                {
                    "job_id": child['job_id'],
                    "url": child['url'],
                    "status": child['status'],
                    "completed_at": child.get('completed_at'),
                    "credits_used": child.get('credits_used', 0)
                }
                for child in child_jobs
            ]
            
            return jsonify(response_data)
        
        return jsonify({"error": "Job not found"}), 404
        
    except Exception as e:
        print(f"❌ ERROR in get_job_status: {str(e)}")
        return jsonify({"error": "Database error"}), 500
    
def create_database_indexes():
    """Create indexes for better query performance"""
    try:
        # Existing indexes...
        child_jobs_collection.create_index([
            ("status", 1),
            ("submitted_at", 1), 
            ("queue_priority", 1),
            ("job_id", 1)
        ])
        
        # ✅ NEW: TTL Index - MongoDB will auto-delete after 24 hours
        child_jobs_collection.create_index(
            "delete_at", 
            expireAfterSeconds=0  # Delete exactly at the time specified in delete_at field
        )
        
        parent_jobs_collection.create_index(
            "delete_at", 
            expireAfterSeconds=0
        )
        
        print("✅ Database indexes created successfully (including TTL)")
    except Exception as e:
        print(f"⚠️ Warning: Could not create indexes: {e}")


@app.route('/api/delete', methods=['GET'])
def delete_job_by_param():
    """Delete a job and its result - matches previous API structure"""
    try:
        # Get job_id from query parameter (like your previous API)
        job_id = request.args.get('job_id')
        
        if not job_id:
            return jsonify({"error": "No job_id provided. Use '?job_id=YOUR_JOB_ID' parameter"}), 400
        
        # Check if it's a parent job (multi-domain job)
        parent_job = parent_jobs_collection.find_one({"job_id": job_id})
        
        if parent_job:
            # ✅ PARENT JOB DELETION
            print(f"🗑️ Deleting parent job {job_id} and all children from database")
            
            # Get list of child jobs before deletion (for reporting)
            child_jobs = list(child_jobs_collection.find({"parent_job_id": job_id}))
            child_job_ids = [child['job_id'] for child in child_jobs]
            
            # Delete all child jobs from MongoDB
            child_result = child_jobs_collection.delete_many({"parent_job_id": job_id})
            
            # Delete parent job from MongoDB
            parent_result = parent_jobs_collection.delete_one({"job_id": job_id})
            
            print(f"📊 Deleted parent job and {child_result.deleted_count} child jobs from database")
            
            # ✅ MATCH YOUR EXPECTED RESPONSE FORMAT
            return jsonify({
                "job_id": job_id,
                "message": f"Successfully deleted parent job '{job_id}' and all child jobs",
                "deleted_jobs": [job_id] + child_job_ids
            })
        
        else:
            # ✅ CHILD JOB DELETION - Check if it's a child job
            child_job = child_jobs_collection.find_one({"job_id": job_id})
            
            if not child_job:
                return jsonify({"error": "Job not found"}), 404
            
            # Delete the child job from MongoDB
            child_result = child_jobs_collection.delete_one({"job_id": job_id})
            
            print(f"🗑️ Deleted job {job_id} from database")
            
            # ✅ MATCH YOUR EXPECTED RESPONSE FORMAT
            return jsonify({
                "job_id": job_id,
                "message": f"Successfully deleted standalone job '{job_id}'",
                "deleted_jobs": [job_id]
            })
                
    except Exception as e:
        print(f"❌ Error in delete_job: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check with database ping"""
    try:
        # ✅ FIXED: Test database connection with simple retry
        simple_db_retry(lambda: client.admin.command('ping'), max_retries=1)
        
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "timestamp": time.time()
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }), 503
    
@app.route('/api/debug', methods=['GET'])
def debug_routes():
    """Debug route to check registered routes"""
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': rule.endpoint,
            'methods': list(rule.methods),
            'rule': rule.rule
        })
    return jsonify({"registered_routes": routes})


@app.route('/', methods=['GET'])
def home():
    """Home page with basic instructions"""
    return '''
    <html>
        <head>
            <title>Parallel Web Scraper API</title>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }
                code { background: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
                pre { background: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }
                table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f4f4f4; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .endpoint { margin-bottom: 30px; border-bottom: 1px solid #eee; padding-bottom: 20px; }
            </style>
        </head>
        <body>
            <h1>Parallel Web Scraper API</h1>
            <p>This API allows you to queue and process multiple scraping jobs in parallel.</p>
            
            <div class="endpoint">
                <h2>1. Submit a Job</h2>
                <p>Use the <code>/api/submit</code> endpoint to add a job to the queue.</p>
                
                <h3>Parameters:</h3>
                <table>
                    <tr>
                        <th>Parameter</th>
                        <th>Description</th>
                        <th>Default</th>
                    </tr>
                    <tr>
                        <td>url</td>
                        <td>The URL to scrape (required)</td>
                        <td>-</td>
                    </tr>
                    <tr>
                        <td>job_id</td>
                        <td>Custom ID for the job (optional)</td>
                        <td>Auto-generated</td>
                    </tr>
                    <tr>
                        <td>js_render</td>
                        <td>Whether to use JavaScript rendering</td>
                        <td>true</td>
                    </tr>
                    <tr>
                        <td>premium_proxy</td>
                        <td>Whether to use premium proxy</td>
                        <td>false</td>
                    </tr>
                </table>
                
                <h3>Example:</h3>
                <pre>GET /api/submit?url=example.com&job_id=my_custom_id&js_render=true&premium_proxy=false</pre>
            </div>
            
            <div class="endpoint">
                <h2>2. Check Job Status</h2>
                <p>Use the <code>/api/status</code> endpoint to check the status of a job.</p>
                <p><strong>Note:</strong> Once a completed job result is retrieved, it is removed from the system.</p>
                
                <h3>Parameters:</h3>
                <table>
                    <tr>
                        <th>Parameter</th>
                        <th>Description</th>
                    </tr>
                    <tr>
                        <td>job_id</td>
                        <td>ID of the job to check (required)</td>
                    </tr>
                </table>
                
                <h3>Example:</h3>
                <pre>GET /api/status?job_id=my_custom_id</pre>
            </div>
            
            <div class="endpoint">
                <h2>3. View Queue Status</h2>
                <p>Use the <code>/api/queue</code> endpoint to see the status of all jobs.</p>
                
                <h3>Example:</h3>
                <pre>GET /api/queue</pre>
            </div>
            
            <div class="endpoint">
                <h2>4. Clear Completed Jobs</h2>
                <p>Use the <code>/api/clear</code> endpoint to remove completed jobs from the system.</p>
                
                <h3>Example:</h3>
                <pre>GET /api/clear</pre>
            </div>
            
            <div class="endpoint">
                <h2>5. Direct Scraping (Legacy)</h2>
                <p>Use the <code>/api/scrape</code> endpoint for immediate scraping (no queuing).</p>
                
                <h3>Parameters:</h3>
                <table>
                    <tr>
                        <th>Parameter</th>
                        <th>Description</th>
                        <th>Default</th>
                    </tr>
                    <tr>
                        <td>url</td>
                        <td>The URL to scrape (required)</td>
                        <td>-</td>
                    </tr>
                    <tr>
                        <td>js_render</td>
                        <td>Whether to use JavaScript rendering</td>
                        <td>true</td>
                    </tr>
                    <tr>
                        <td>premium_proxy</td>
                        <td>Whether to use premium proxy</td>
                        <td>false</td>
                    </tr>
                </table>
                
                <h3>Example:</h3>
                <pre>GET /api/scrape?url=example.com&js_render=true&premium_proxy=false</pre>
            </div>
            
            <h2>Credit Usage:</h2>
            <table>
                <tr>
                    <th>Configuration</th>
                    <th>Credits</th>
                </tr>
                <tr>
                    <td>Basic (no JS, no premium proxy)</td>
                    <td>1</td>
                </tr>
                <tr>
                    <td>With JS rendering</td>
                    <td>5</td>
                </tr>
                <tr>
                    <td>With premium proxy (no JS)</td>
                    <td>10</td>
                </tr>
                <tr>
                    <td>With JS rendering + premium proxy</td>
                    <td>25</td>
                </tr>
            </table>
            
            <h3>Testing Tool:</h3>
            <form action="/api/submit" method="get">
                <p>
                    <label for="url">URL to scrape:</label>
                    <input type="text" id="url" name="url" placeholder="example.com" required style="width: 300px;">
                </p>
                <p>
                    <label for="job_id">Job ID (optional):</label>
                    <input type="text" id="job_id" name="job_id" placeholder="my_custom_id" style="width: 300px;">
                </p>
                <p>
                    <label for="js_render">Use JavaScript rendering:</label>
                    <select id="js_render" name="js_render">
                        <option value="true">Yes (5 credits)</option>
                        <option value="false">No (1 credit)</option>
                    </select>
                </p>
                <p>
                    <label for="premium_proxy">Use premium proxy:</label>
                    <select id="premium_proxy" name="premium_proxy">
                        <option value="false">No</option>
                        <option value="true">Yes (increases cost)</option>
                    </select>
                </p>
                <p>
                    <button type="submit">Submit Scraping Job</button>
                </p>
            </form>
        </body>
    </html>
    '''

if __name__ == '__main__':
    print("🚀 Starting Pure Database-Driven Scraping Service...")
    
    # Set worker flags
    worker_running = True

    # Create database indexes (will include TTL)
    create_database_indexes()
    
    # Start database worker thread
    worker_thread = threading.Thread(target=database_worker, daemon=True)
    worker_thread.start()
    print("✅ Database worker thread started")
    
    # Simple - just run Flask
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    print("👋 Application stopped")
