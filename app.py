from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import json
import re
import os
import threading
import queue
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import concurrent.futures
import threading


app = Flask(__name__)


API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
# ScrapingBee endpoint
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"

# File system paths
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

# for thread locking (prevent concurrent modifications in parent json)
parent_job_lock = threading.Lock()

# In-memory processing queue
processing_jobs = {}  # Jobs being processed
request_queue = queue.Queue()
MAX_CONCURRENT_REQUESTS = 15
worker_running = False

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
            # Extended timeout: 60s connect, 240s read (4 minutes total)
            response = requests.get(
                SCRAPINGBEE_URL, 
                params=params,
                timeout=(60, 240)
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
    
    # Return in single key format
    return {
        "page_content": complete_content,
        "metadata": {
            "url": data.get('url', ''),
            "extraction_method": data.get('extraction_method', 'unknown'),
            "retry_info": data.get('retry_info', {}),
            "credits_used": data.get('credits_used', 0)
        }
    }
                                                                                                                                               
def handle_job_timeout(job_id):
    """Handle a job that timed out"""
    try:
        if job_id in processing_jobs:
            job = processing_jobs[job_id]
            timeout_result = {
                'url': job['url'],
                'js_render': job['js_render'],
                'premium_proxy': job['premium_proxy'],
                'status': 'timeout',
                'submitted_at': job['submitted_at'],
                'completed_at': time.time(),
                'result': {
                    "error": "Job timed out - took too long to complete (exceeded 5 minutes)",
                    "timeout": True,
                    "timeout_duration": "5 minutes",
                    "retry_info": {
                        "max_retries_attempted": True,
                        "note": "URL was retried 3 times before final timeout"
                    }
                },
                'credits_used': 0  # Don't charge for timeouts
            }
            
            # Handle parent job reference
            parent_job_id = job.get('parent_job_id')
            if parent_job_id:
                timeout_result['parent_job_id'] = parent_job_id
            
            # Save timeout result
            save_job_result(job_id, timeout_result)
            
            # Update parent job if applicable
            if parent_job_id:
                update_child_job_status(parent_job_id, job_id, 'timeout')
                check_parent_job_completion(parent_job_id)
            
            # Remove from processing
            del processing_jobs[job_id]
            print(f"✅ Timeout handled for job: {job_id}")
    
    except Exception as e:
        print(f"Error handling timeout for job {job_id}: {str(e)}")

def handle_job_error(job_id, error_message):
    """Handle a job that failed with an error (after retries)"""
    try:
        if job_id in processing_jobs:
            job = processing_jobs[job_id]
            error_result = {
                'url': job['url'],
                'js_render': job['js_render'],
                'premium_proxy': job['premium_proxy'],
                'status': 'error',
                'submitted_at': job['submitted_at'],
                'completed_at': time.time(),
                'result': {
                    "error": error_message,
                    "retry_info": {
                        "note": "Failed after multiple retry attempts"
                    }
                },
                'credits_used': 0  # Don't charge for errors after retries
            }
            
            # Handle parent job reference
            parent_job_id = job.get('parent_job_id')
            if parent_job_id:
                error_result['parent_job_id'] = parent_job_id
            
            # Save error result
            save_job_result(job_id, error_result)
            
            # Update parent job if applicable
            if parent_job_id:
                update_child_job_status(parent_job_id, job_id, 'error')
                check_parent_job_completion(parent_job_id)
            
            # Remove from processing
            del processing_jobs[job_id]
    
    except Exception as e:
        print(f"Error handling job error for {job_id}: {str(e)}")

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

def save_job_result(job_id, job_data):
    """Save job result to a file"""
    try:
        file_path = RESULTS_DIR / f"{job_id}.json"
        with open(file_path, 'w') as f:
            json.dump(job_data, f)
        return True
    except Exception as e:
        print(f"Error saving job result: {str(e)}")
        return False

# Modify the get_job_result function to not delete the file after reading
def get_job_result(job_id):
    """Get job result from file without deleting it"""
    try:
        file_path = RESULTS_DIR / f"{job_id}.json"
        if not file_path.exists():
            return None
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Remove the file_path.unlink() line to keep the file
        
        return data
    except Exception as e:
        print(f"Error reading job result: {str(e)}")
        return None
    
def worker():
    """Background worker to process queued requests in batches of 50"""
    global worker_running
    worker_running = True
    
    while worker_running:
        try:
            # Clean old results (older than 24 hours)
            clean_old_results()
            
            # Get up to 15 jobs from the queue (changed from 5)
            batch = []
            for _ in range(MAX_CONCURRENT_REQUESTS):
                try:
                    job_id = request_queue.get(block=True, timeout=1)
                    if job_id in processing_jobs:
                        batch.append(job_id)
                    request_queue.task_done()
                except queue.Empty:
                    break
            
            if not batch:
                continue
            
            print(f"🔄 Processing batch of {len(batch)} jobs with 5-minute per-URL timeout...")
            
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {
                    executor.submit(process_job, job_id): job_id 
                    for job_id in batch
                }
                
                completed_futures = []
                
                # Wait for futures to complete - no batch timeout, only individual URL timeouts
                for future in concurrent.futures.as_completed(futures):
                    try:
                        # 5-minute timeout for individual job result
                        future.result(timeout=20 * 60)  # 5 minutes per URL
                        completed_futures.append(future)
                        job_id = futures[future]
                        print(f"✅ Job {job_id} completed successfully")
                        
                    except concurrent.futures.TimeoutError:
                        job_id = futures[future]
                        print(f"⏰ Job {job_id} timed out after 5 minutes!")
                        handle_job_timeout(job_id)
                        
                    except Exception as e:
                        job_id = futures[future]
                        print(f"❌ Job {job_id} failed with error: {str(e)}")
                        handle_job_error(job_id, str(e))
        
        except Exception as e:
            print(f"Worker error: {str(e)}")
            time.sleep(1)

@app.route('/api/multiple_domains', methods=['GET'])
def submit_multiple_domains_job():
    """Submit multiple domain scraping job with true queue processing"""
    try:
        urls = request.args.getlist('url')
        js_render = request.args.get('js_render', 'false')
        premium_proxy = request.args.get('premium_proxy', 'false')
        parent_job_id = request.args.get('job_id')
        
        if not urls:
            return jsonify({"error": "No URLs provided"}), 400
        
        if not parent_job_id:
            parent_job_id = f"multi_{int(time.time())}"
        
        # Create a tracker file for this multi-domain job
        tracker = {
            'parent_job_id': parent_job_id,
            'status': 'queued',
            'urls': urls,
            'url_count': len(urls),
            'js_render': js_render,
            'premium_proxy': premium_proxy,
            'child_jobs': [],
            'submitted_at': time.time(),
            'started_at': time.time(),
            'completed_at': None,
            'estimated_credits': calculate_credits(js_render, premium_proxy) * len(urls)
        }
        
        # Ensure the directory exists and write the file properly
        RESULTS_DIR.mkdir(exist_ok=True)
        tracker_file_path = RESULTS_DIR / f"{parent_job_id}.json"
        
        # Write the tracker file with error handling
        try:
            with open(tracker_file_path, 'w') as f:
                json.dump(tracker, f, indent=2)
        except Exception as e:
            return jsonify({"error": f"Failed to create job tracker: {str(e)}"}), 500
        
        # Verify the file was written correctly
        if not tracker_file_path.exists() or tracker_file_path.stat().st_size == 0:
            return jsonify({"error": "Failed to create job tracker file"}), 500
        
        # Create child jobs
        child_jobs = []
        for i, url in enumerate(urls):
            child_job_id = f"{parent_job_id}_url{i}"
            child_job = {
                'job_id': child_job_id,
                'url': url,
                'js_render': js_render,
                'premium_proxy': premium_proxy,
                'status': 'queued',
                'submitted_at': time.time(),
                'parent_job_id': parent_job_id
            }
            child_jobs.append(child_job)
        
        # Update tracker with child jobs
        tracker['child_jobs'] = child_jobs
        
        # Update the tracker file with child jobs
        try:
            with open(tracker_file_path, 'w') as f:
                json.dump(tracker, f, indent=2)
        except Exception as e:
            return jsonify({"error": f"Failed to update job tracker: {str(e)}"}), 500
        
        # ✅ TRUE QUEUE PROCESSING - Add ALL jobs to processing queue
        for child_job in child_jobs:
            processing_jobs[child_job['job_id']] = child_job
        
        # ✅ Submit ALL jobs at once - no batching, true queue behavior
        with ThreadPoolExecutor(max_workers=15) as executor:
            # Submit all jobs simultaneously to the thread pool
            future_to_job = {
                executor.submit(process_job, child_job['job_id']): child_job['job_id']
                for child_job in child_jobs  # ALL jobs submitted at once
            }
            
            # Wait for ALL jobs to complete (threads will pick up next job automatically)
            for future in concurrent.futures.as_completed(future_to_job):
                try:
                    future.result(timeout=300)  # 5-minute timeout per job
                except Exception as e:
                    job_id = future_to_job[future]
                    print(f"❌ Job {job_id} failed: {str(e)}")
        
        return jsonify({
            "message": f"Multi-domain job {parent_job_id} submitted successfully",
            "parent_job_id": parent_job_id,
            "total_urls": len(urls),
            "estimated_credits": tracker['estimated_credits'],
            "status": "queued",
            "processing_mode": "true_queue",  # Indicate the processing mode
            "max_parallel_jobs": 15
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def process_job(job_id, max_job_retries=2):
    """Process a single job with retry logic at the job level - 2 job attempts, 1 URL attempt each"""
    try:
        if job_id not in processing_jobs:
            return
            
        job = processing_jobs[job_id]
        job['status'] = 'processing'
        
        # Update parent job if this is a child job
        parent_job_id = job.get('parent_job_id')
        if parent_job_id:
            update_child_job_status(parent_job_id, job_id, 'processing')
        
        last_error = None
        job_start_time = time.time()
        
        # Job-level retry loop - try the entire scraping job 2 times
        for job_attempt in range(max_job_retries):
            try:
                attempt_start_time = time.time()
                
                # Call the scraping function (1 URL attempt per job attempt)
                result = scrape_complete_homepage(
                    job['url'], 
                    job['js_render'], 
                    job['premium_proxy'],
                    max_retries=1
                )
                
                # Check if scraping was successful
                if isinstance(result, dict) and result.get('success'):
                    # Get credits from result if available
                    credit_cost = result.get('credits_used', calculate_credits(job['js_render'], job['premium_proxy']))
                    
                    # Create a completed job entry
                    completed_job = {
                        'url': job['url'],
                        'js_render': job['js_render'],
                        'premium_proxy': job['premium_proxy'],
                        'status': 'completed',
                        'submitted_at': job['submitted_at'],
                        'completed_at': time.time(),
                        'result': result,
                        'credits_used': credit_cost,
                        'job_retry_info': {
                            'job_attempts_made': job_attempt + 1,
                            'job_successful': True
                        }
                    }
                    
                    # If this is a child job, add parent reference
                    if parent_job_id:
                        completed_job['parent_job_id'] = parent_job_id
                    
                    # Save to file system
                    save_job_result(job_id, completed_job)
                    
                    # Update parent job if this is a child job
                    if parent_job_id:
                        update_child_job_status(parent_job_id, job_id, 'completed')
                    
                    # Remove from processing queue
                    if job_id in processing_jobs:
                        del processing_jobs[job_id]
                    
                    return  # Exit function on success
                
                # Check for immediate skip errors (404/400)
                elif isinstance(result, dict) and result.get('skip_retry'):
                    total_job_time = time.time() - job_start_time
                    
                    # Save result with error details
                    job_result = {
                        'job_id': job_id,
                        'url': job['url'],
                        'js_render': job['js_render'],
                        'premium_proxy': job['premium_proxy'],
                        'status': 'error',
                        'submitted_at': job['submitted_at'],
                        'completed_at': time.time(),
                        'result': {
                            'error': result.get('error'),
                            'error_type': result.get('error_type'),
                            'skipped_due_to_client_error': True
                        },
                        'credits_used': result.get('credits_used', 1),
                        'total_time': total_job_time,
                        'job_attempts': job_attempt + 1,
                        'job_retry_info': {
                            'job_attempts_made': job_attempt + 1,
                            'job_successful': False,
                            'skipped_immediately': True
                        }
                    }
                    
                    # If this is a child job, add parent reference
                    if parent_job_id:
                        job_result['parent_job_id'] = parent_job_id
                    
                    # Save to file system
                    save_job_result(job_id, job_result)
                    
                    # Update parent job if this is a child job
                    if parent_job_id:
                        update_child_job_status(parent_job_id, job_id, 'error')
                    
                    # Remove from processing queue
                    if job_id in processing_jobs:
                        del processing_jobs[job_id]
                    
                    return  # Exit immediately, no more job attempts
                
                # Other errors (will retry)
                else:
                    error_msg = result.get('error', 'Unknown scraping error') if isinstance(result, dict) else str(result)
                    last_error = {
                        "job_attempt": job_attempt + 1,
                        "error": error_msg,
                        "result": result,
                        "attempt_duration": time.time() - attempt_start_time
                    }
                    
                    # If it's the last attempt, break and handle error
                    if job_attempt == max_job_retries - 1:
                        break
                    
                    # Wait before retry
                    wait_time = job_attempt + 1
                    time.sleep(wait_time)
                    
            except Exception as job_error:
                error_msg = f"Job attempt {job_attempt + 1} exception: {str(job_error)}"
                
                last_error = {
                    "job_attempt": job_attempt + 1,
                    "error": error_msg,
                    "exception": str(job_error),
                    "attempt_duration": time.time() - attempt_start_time
                }
                
                # If it's the last attempt, break and handle error
                if job_attempt == max_job_retries - 1:
                    break
                
                # Wait before retry
                wait_time = job_attempt + 1
                time.sleep(wait_time)
        
        # All job attempts failed - create error result
        error_result = {
            'url': job['url'],
            'js_render': job['js_render'],
            'premium_proxy': job['premium_proxy'],
            'status': 'error',
            'submitted_at': job['submitted_at'],
            'completed_at': time.time(),
            'result': {
                "error": f"Job failed after {max_job_retries} attempts",
                "last_error": last_error,
                "job_retry_info": {
                    "job_attempts_made": max_job_retries,
                    "job_successful": False,
                    "final_error": "max_job_retries_exceeded"
                }
            },
            'credits_used': 0  # Don't charge for failed jobs
        }
        
        # If this is a child job, add parent reference
        if parent_job_id:
            error_result['parent_job_id'] = parent_job_id
        
        # Save error result
        save_job_result(job_id, error_result)
        
        # Update parent job if this is a child job
        if parent_job_id:
            update_child_job_status(parent_job_id, job_id, 'error')
        
        # Remove from processing
        if job_id in processing_jobs:
            del processing_jobs[job_id]
            
    except Exception as e:
        if job_id in processing_jobs:
            del processing_jobs[job_id]


def update_child_job_status(parent_job_id, child_job_id, status):
    """Update the status of a child job in the parent tracker with simple threading lock"""
    try:
        file_path = RESULTS_DIR / f"{parent_job_id}.json"
        if not file_path.exists():
            return
        
        # Use simple threading lock to prevent concurrent writes
        with parent_job_lock:
            # Read current content
            with open(file_path, 'r') as f:
                content = f.read()
            
            if not content.strip():
                return
            
            try:
                parent_job = json.loads(content)
            except json.JSONDecodeError as e:
                return
            
            # Update child job status
            updated = False
            for child in parent_job.get('child_jobs', []):
                if child.get('job_id') == child_job_id:
                    child['status'] = status
                    updated = True
                    break
            
            if not updated:
                return
            
            # Write back to file
            with open(file_path, 'w') as f:
                json.dump(parent_job, f, indent=2)
            
            # Check completion after updating (while still holding the lock)
            check_parent_job_completion_locked(parent_job_id)
        
    except Exception as e:
        pass

def check_parent_job_completion_locked(parent_job_id):
    """Check completion while already holding the lock - INTERNAL USE ONLY"""
    try:
        file_path = RESULTS_DIR / f"{parent_job_id}.json"
        if not file_path.exists():
            return
        
        # Read current content (lock already held by caller)
        with open(file_path, 'r') as f:
            content = f.read()
        
        if not content.strip():
            return
        
        try:
            parent_job = json.loads(content)
        except json.JSONDecodeError as e:
            return
        
        # Check if already completed
        if parent_job.get('status') == 'completed':
            return
        
        # Check if all child jobs are completed, error, or timeout
        all_completed = True
        for child in parent_job.get('child_jobs', []):
            if child.get('status') not in ['completed', 'error', 'timeout']:
                all_completed = False
                break
        
        # If all completed, update parent job status
        if all_completed:
            completion_time = time.time()
            parent_job['status'] = 'completed'
            parent_job['completed_at'] = completion_time
            
            # Add timing to parent job data
            if 'started_at' in parent_job:
                total_duration = completion_time - parent_job['started_at']
                parent_job['timing'] = {
                    'total_duration_seconds': round(total_duration, 2),
                    'average_per_url_seconds': round(total_duration / parent_job['url_count'], 2)
                }
            
            # Collect results from all child jobs
            results = {}
            total_credits = 0
            timeout_count = 0
            error_count = 0
            success_count = 0
            
            for child in parent_job.get('child_jobs', []):
                child_job_id = child.get('job_id')
                child_url = child.get('url')
                
                # Get child job result
                child_result = get_job_result(child_job_id)
                if child_result:
                    status = child_result.get('status')
                    results[child_url] = {
                        'status': status,
                        'result': child_result.get('result'),
                        'credits_used': child_result.get('credits_used', 0)
                    }
                    total_credits += child_result.get('credits_used', 0)
                    
                    # Count status types
                    if status == 'completed':
                        success_count += 1
                    elif status == 'timeout':
                        timeout_count += 1
                    elif status == 'error':
                        error_count += 1
            
            # Add results and summary to parent job
            parent_job['results'] = results
            parent_job['total_credits_used'] = total_credits
            parent_job['summary'] = {
                'total_urls': len(parent_job.get('child_jobs', [])),
                'successful': success_count,
                'timeouts': timeout_count,
                'errors': error_count
            }
            
            # Write back to file
            with open(file_path, 'w') as f:
                json.dump(parent_job, f, indent=2)
            
    except Exception as e:
        pass

# Keep the old function for external calls (but make it use the lock)
def check_parent_job_completion(parent_job_id):
    """Public function to check parent job completion with locking"""
    with parent_job_lock:
        check_parent_job_completion_locked(parent_job_id)

def clean_old_results():
    """Clean results older than 24 hours"""
    try:
        current_time = time.time()
        one_day_ago = current_time - (24*60*60)
        
        for file_path in RESULTS_DIR.glob("*.json"):
            # Get file modification time
            file_mtime = file_path.stat().st_mtime
            if file_mtime < one_day_ago:
                file_path.unlink()
    except Exception as e:
        print(f"Error cleaning old results: {str(e)}")

@app.route('/api/submit', methods=['GET'])
def submit_job():
    """Submit a new scraping job to the queue"""
    url = request.args.get('url')
    js_render = request.args.get('js_render', 'true')
    premium_proxy = request.args.get('premium_proxy', 'false')
    job_id = request.args.get('job_id')
    
    if not url:
        return jsonify({"error": "No URL provided. Use '?url=example.com' parameter"}), 400
        
    if not job_id:
        # Generate a timestamp-based ID if not provided
        job_id = f"job_{int(time.time() * 1000)}"
                                     
    # Check if job_id already exists in processing or file system
    if job_id in processing_jobs or (RESULTS_DIR / f"{job_id}.json").exists():
        return jsonify({"error": f"Job ID '{job_id}' already exists. Please use a unique job ID"}), 400
        
    # Add http:// prefix if not present
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Create a new job and add to processing queue
    processing_jobs[job_id] = {
        'url': url,
        'js_render': js_render,
        'premium_proxy': premium_proxy,
        'status': 'queued',
        'submitted_at': time.time(),
        'completed_at': None,
        'credits_used': calculate_credits(js_render, premium_proxy)
    }
    
    # Add to queue
    request_queue.put(job_id)
    
    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "url": url,
        "message": f"Job queued. Check status at /api/status?job_id={job_id}"
    })

@app.route('/api/status', methods=['GET'])
def get_job_status():
    """Get the status or result of a job by ID"""
    job_id = request.args.get('job_id')
    
    if not job_id:
        return jsonify({"error": "No job_id provided. Use '?job_id=YOUR_JOB_ID' parameter"}), 400
    
    # First check if job is in processing queue
    if job_id in processing_jobs:
        job = processing_jobs[job_id]
        return jsonify({
            "job_id": job_id,
            "status": job['status'],
            "url": job.get('url', ''),
            "submitted_at": job['submitted_at'],
            "message": "Job is still processing..."
        })
    
    # Then check in file system
    result = get_job_result(job_id)
    if result:
        # Check if this is a parent job (has child_jobs field)
        if 'child_jobs' in result:
            # It's a multi-domain job
            response = {
                "job_id": job_id,
                "status": result['status'],
                "url_count": result.get('url_count', 0),
                "submitted_at": result['submitted_at'],
                "completed_at": result.get('completed_at')
            }
            
            # If completed, include results and summary
            if result['status'] == 'completed':
                response["total_credits_used"] = result.get('total_credits_used', 0)
                response["results"] = result.get('results', {})
                response["summary"] = result.get('summary', {})  # ✅ Add summary with timeout info
            else:
                # Include progress info
                child_statuses = {}
                for child in result.get('child_jobs', []):
                    child_statuses[child.get('url')] = child.get('status')
                response["progress"] = child_statuses
            
            return jsonify(response)
        else:
            # It's a regular job
            if result['status'] == 'completed':
                response = {
                    "job_id": job_id,
                    "status": "completed",
                    "url": result['url'],
                    "submitted_at": result['submitted_at'],
                    "completed_at": result['completed_at'],
                    "credits_used": result.get('credits_used', 0),
                    "result": result['result']
                }
            elif result['status'] == 'timeout':  # ✅ Handle timeout status
                response = {
                    "job_id": job_id,
                    "status": "timeout",
                    "url": result['url'],
                    "submitted_at": result['submitted_at'],
                    "completed_at": result['completed_at'],
                    "timeout": result['result'],
                    "credits_used": result.get('credits_used', 0)
                }
            else:  # Error status
                response = {
                    "job_id": job_id,
                    "status": "error",
                    "url": result['url'],
                    "submitted_at": result['submitted_at'],
                    "completed_at": result['completed_at'],
                    "error": result['result'],
                    "credits_used": result.get('credits_used', 0)
                }
            
            return jsonify(response)
    
    # Job not found in either place
    return jsonify({"error": f"Job '{job_id}' not found. It may have already been retrieved or never existed."}), 404


@app.route('/api/queue', methods=['GET'])
def get_queue_status():
    """Get status of all jobs in the queue and results directory"""
    # Count jobs in processing queue by status
    processing_status_counts = {
        'queued': 0,
        'processing': 0
    }
    
    for job in processing_jobs.values():
        if job['status'] in processing_status_counts:
            processing_status_counts[job['status']] += 1
    
    # Count completed jobs in file system
    completed_count = len(list(RESULTS_DIR.glob("*.json")))
    
    # Combine counts
    status_counts = {
        'queued': processing_status_counts['queued'],
        'processing': processing_status_counts['processing'],
        'completed': completed_count
    }
    
    return jsonify({
        "queue_size": request_queue.qsize(),
        "processing_jobs": len(processing_jobs),
        "completed_jobs": completed_count,
        "status_counts": status_counts,
        "processing_jobs_list": {
            job_id: {
                "status": job["status"],
                "url": job["url"],
                "submitted_at": job["submitted_at"],
                "credits_used": job.get("credits_used", 0)
            } for job_id, job in processing_jobs.items()
        },
        "completed_job_ids": [file.stem for file in RESULTS_DIR.glob("*.json")]
    })

@app.route('/api/clear', methods=['GET'])
def clear_completed():
    """Clear all completed jobs"""
    try:
        completed_files = list(RESULTS_DIR.glob("*.json"))
        count = len(completed_files)
        
        for file_path in completed_files:
            file_path.unlink()
        
        return jsonify({
            "message": f"Cleared {count} completed jobs",
            "remaining_processing_jobs": len(processing_jobs)
        })
    except Exception as e:
        return jsonify({
            "error": f"Error clearing completed jobs: {str(e)}"
        }), 500
# Add a new endpoint to delete a specific job result by job_id
@app.route('/api/delete', methods=['GET'])
def delete_job_result():
    """Delete a specific job result by job_id - handles parent/child relationships"""
    job_id = request.args.get('job_id')
    
    if not job_id:
        return jsonify({"error": "No job_id provided. Use '?job_id=YOUR_JOB_ID' parameter"}), 400
    
    try:
        file_path = RESULTS_DIR / f"{job_id}.json"
        
        if not file_path.exists():
            return jsonify({
                "error": f"No job result found for job_id: {job_id}"
            }), 404
        
        # Read the job data to determine if it's a parent or child job
        with open(file_path, 'r') as f:
            job_data = json.load(f)
        
        deleted_jobs = []
        
        # Check if this is a parent job (has child_jobs field)
        if 'child_jobs' in job_data:
            # This is a parent job - delete all child jobs first
            child_jobs = job_data.get('child_jobs', [])
            
            for child in child_jobs:
                child_job_id = child.get('job_id')
                if child_job_id:
                    child_file_path = RESULTS_DIR / f"{child_job_id}.json"
                    if child_file_path.exists():
                        child_file_path.unlink()
                        deleted_jobs.append(child_job_id)
            
            # Delete the parent job
            file_path.unlink()
            deleted_jobs.append(job_id)
            
            return jsonify({
                "message": f"Successfully deleted parent job '{job_id}' and {len(child_jobs)} child jobs",
                "deleted_jobs": deleted_jobs,
                "parent_job_id": job_id,
                "child_jobs_deleted": len(child_jobs)
            })
        
        # Check if this is a child job (has parent_job_id field)
        elif 'parent_job_id' in job_data:
            parent_job_id = job_data.get('parent_job_id')
            
            # Delete the child job first
            file_path.unlink()
            deleted_jobs.append(job_id)
            
            # Update the parent job to remove this child from the list
            if parent_job_id:
                parent_file_path = RESULTS_DIR / f"{parent_job_id}.json"
                if parent_file_path.exists():
                    try:
                        with open(parent_file_path, 'r') as f:
                            parent_data = json.load(f)
                        
                        # Remove this child job from the parent's child_jobs list
                        if 'child_jobs' in parent_data:
                            parent_data['child_jobs'] = [
                                child for child in parent_data['child_jobs'] 
                                if child.get('job_id') != job_id
                            ]
                            
                            # Update URL count and remove from results if present
                            if 'results' in parent_data:
                                # Find and remove the URL from results
                                url_to_remove = job_data.get('url')
                                if url_to_remove and url_to_remove in parent_data['results']:
                                    del parent_data['results'][url_to_remove]
                            
                            # Update counts
                            parent_data['url_count'] = len(parent_data.get('child_jobs', []))
                            
                            # Recalculate total credits if needed
                            if 'results' in parent_data:
                                total_credits = sum(
                                    result.get('credits_used', 0) 
                                    for result in parent_data['results'].values()
                                )
                                parent_data['total_credits_used'] = total_credits
                            
                            # Save updated parent job
                            with open(parent_file_path, 'w') as f:
                                json.dump(parent_data, f)
                    
                    except Exception as e:
                        print(f"Error updating parent job {parent_job_id}: {str(e)}")
            
            return jsonify({
                "message": f"Successfully deleted child job '{job_id}' and updated parent job '{parent_job_id}'",
                "deleted_jobs": deleted_jobs,
                "child_job_id": job_id,
                "parent_job_id": parent_job_id
            })
        
        else:
            # This is a standalone job (neither parent nor child)
            file_path.unlink()
            deleted_jobs.append(job_id)
            
            return jsonify({
                "message": f"Successfully deleted standalone job '{job_id}'",
                "deleted_jobs": deleted_jobs,
                "job_id": job_id
            })
            
    except json.JSONDecodeError:
        # File exists but is not valid JSON - delete it anyway
        if file_path.exists():
            file_path.unlink()
        return jsonify({
            "message": f"Deleted corrupted job file for job_id: {job_id}",
            "deleted_jobs": [job_id],
            "warning": "File contained invalid JSON"
        })
        
    except Exception as e:
        return jsonify({
            "error": f"Error deleting job result: {str(e)}"
        }), 500

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

# Function to ensure the results directory exists at startup
def ensure_results_dir():
    """Ensure the results directory exists"""
    if not RESULTS_DIR.exists():
        RESULTS_DIR.mkdir()

# Start the worker thread when the app starts
if __name__ == '__main__':
    # Ensure results directory exists
    ensure_results_dir()

    # Start the worker thread
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    # Run the Flask app
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
else:
    # For when importing as a module or running with a WSGI server
    ensure_results_dir()
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start() # type: ignore
