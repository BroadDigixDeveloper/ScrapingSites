import asyncio
import aiohttp
from flask import Flask, request, jsonify
from bs4 import BeautifulSoup
import json
import os
import time
from pathlib import Path
import threading
import signal
# from concurrent.futures import ThreadPoolExecutor
import functools

# Flask app setup
app = Flask(__name__)

API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"

# File system paths
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

# Thread locking for file operations
parent_job_lock = threading.Lock()

# Async queue and concurrency control
job_queue = asyncio.Queue()
processing_jobs = {}
MAX_CONCURRENT_REQUESTS = 10
worker_running = False

# Global aiohttp session and event loop
session = None
loop = None
# executor = ThreadPoolExecutor(max_workers=4)

def async_route(f):
    """Decorator to run async functions in Flask routes"""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        # Get or create event loop for this thread
        try:
            current_loop = asyncio.get_event_loop()
        except RuntimeError:
            current_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(current_loop)
        
        # Run the async function
        return current_loop.run_until_complete(f(*args, **kwargs))
    return wrapper

def run_in_background_loop(coro):
    """Run coroutine in the background event loop"""
    global loop
    if loop is None or loop.is_closed():
        return None
    
    # Schedule coroutine in the background loop
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future

class AsyncJobManager:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.active_tasks = set()
        
    async def add_task(self, coro):
        """Add a task to the active set"""
        task = asyncio.create_task(coro)
        self.active_tasks.add(task)
        task.add_done_callback(self.active_tasks.discard)
        return task

job_manager = AsyncJobManager()

async def get_session():
    """Get or create global aiohttp session with connection pooling"""
    global session
    if session is None or session.closed:
        # Configure connection pooling
        connector = aiohttp.TCPConnector(
            limit=20,                    # Total pool size
            limit_per_host=10,          # Max connections per host
            ttl_dns_cache=300,          # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=30,       # Keep connections alive
            enable_cleanup_closed=True
        )
        
        # Set timeout configuration
        timeout = aiohttp.ClientTimeout(
            total=190,      
            connect=30,     
            sock_read=160  
        )
        
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        print("🔗 Created new aiohttp session with connection pooling")
    
    return session

async def scrape_complete_homepage_async(url, use_js_render='true', use_premium_proxy='false', max_retries=1):
    """
    Async version of homepage scraping with connection reuse
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
    session = await get_session()
    
    # Retry loop - try up to max_retries times
    for attempt in range(max_retries):
        try:
            # ✅ Async HTTP request with connection reuse
            async with session.get(SCRAPINGBEE_URL, params=params) as response:
                
                # 🚨 CHECK FOR 404/400 ERRORS - IMMEDIATE SKIP
                if response.status == 404:
                    error_msg = f"404 Not Found: {url}"
                    return {
                        'success': False,
                        'error': error_msg,
                        'error_type': '404_not_found',
                        'skip_retry': True,
                        'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                    }
                
                elif response.status == 400:
                    error_msg = f"400 Bad Request: {url}"
                    return {
                        'success': False,
                        'error': error_msg,
                        'error_type': '400_bad_request', 
                        'skip_retry': True,
                        'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                    }

                elif response.status != 200:
                    error_msg = f"ScrapingBee failed: {response.status}"
                    response_text = await response.text()
                    last_error = {"error": error_msg, "details": response_text, "attempt": attempt + 1}
                    
                    if attempt == max_retries - 1:
                        return {
                            'success': False,
                            'error': error_msg,
                            'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                        }
                    
                    await asyncio.sleep(attempt + 1)
                    continue

                # Get response content
                response_text = await response.text()
                
                # Parse the response
                soup = BeautifulSoup(response_text, 'html.parser')

                # Your existing parsing logic here...
                data = await parse_html_content(soup, url)
                
                # Add retry and credit information
                data['retry_info'] = {
                    'attempts_made': attempt + 1,
                    'successful': True
                }
                data['credits_used'] = calculate_credits(use_js_render, use_premium_proxy)
                data['extraction_method'] = 'async_parsing'

                # Format the structured data into single key format
                formatted_result = format_to_single_key(data)
                
                # Mark as successful and return
                formatted_result['success'] = True
                return formatted_result

        except asyncio.TimeoutError:
            error_msg = f"Timeout on attempt {attempt + 1}"
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
            
            await asyncio.sleep(attempt + 1)
            
        except aiohttp.ClientError as e:
            error_msg = f"Connection error on attempt {attempt + 1}: {str(e)}"
            last_error = {"error": error_msg, "attempt": attempt + 1}
            
            if attempt == max_retries - 1:
                return {
                    'success': False,
                    'error': error_msg,
                    'retry_info': {
                        'attempts_made': max_retries,
                        'successful': False,
                        'final_error': 'connection_error_after_retries'
                    },
                    'credits_used': calculate_credits(use_js_render, use_premium_proxy)
                }
            
            await asyncio.sleep(attempt + 1)
            
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
            
            await asyncio.sleep(attempt + 1)

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

async def parse_html_content(soup, url):
    """Extract data from HTML soup - async version"""
    # This function doesn't need to be async since BeautifulSoup is CPU-bound,
    # but we make it async for consistency and future flexibility
    
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
    if desc: 
        data['page_info']['meta_description'] = desc.get('content', '')
    
    keywords = soup.find('meta', {'name': 'keywords'})
    if keywords: 
        data['page_info']['meta_keywords'] = keywords.get('content', '')

    # Header section
    header = soup.find('header') or soup.find(class_=lambda c: c and 'header' in str(c).lower())
    if header:
        logo = header.find('img')
        if logo: 
            data['header']['logo'] = logo.get('src', '')
        
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

    return data

async def process_job_async(job_id, max_job_retries=2):
    """Async version of job processing with semaphore control"""
    async with job_manager.semaphore:  # Limit concurrent jobs
        try:
            if job_id not in processing_jobs:
                return
                
            job = processing_jobs[job_id]
            job['status'] = 'processing'
            
            # Update parent job if this is a child job
            parent_job_id = job.get('parent_job_id')
            if parent_job_id:
                await update_child_job_status_async(parent_job_id, job_id, 'processing')
            
            last_error = None
            job_start_time = time.time()
            
            # Job-level retry loop
            for job_attempt in range(max_job_retries):
                try:
                    attempt_start_time = time.time()
                    
                    # Call the async scraping function
                    result = await scrape_complete_homepage_async(
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
                        await save_job_result_async(job_id, completed_job)
                        
                        # Update parent job if this is a child job
                        if parent_job_id:
                            await update_child_job_status_async(parent_job_id, job_id, 'completed')
                        
                        # Remove from processing queue
                        if job_id in processing_jobs:
                            del processing_jobs[job_id]
                        
                        print(f"✅ Async job {job_id} completed successfully")
                        return  # Exit function on success
                    
                    # Handle skip retry cases (404/400)
                    elif isinstance(result, dict) and result.get('skip_retry'):
                        # Save error result and exit
                        await handle_job_error_async(job_id, result, parent_job_id, job_attempt + 1)
                        return
                    
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
                        await asyncio.sleep(job_attempt + 1)
                        
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
                    await asyncio.sleep(job_attempt + 1)
            
            # All job attempts failed - create error result
            await handle_final_job_error_async(job_id, last_error, parent_job_id, max_job_retries)
                
        except Exception as e:
            print(f"❌ Critical error in async job {job_id}: {str(e)}")
            if job_id in processing_jobs:
                del processing_jobs[job_id]

async def handle_job_error_async(job_id, result, parent_job_id, attempts):
    """Handle job error asynchronously"""
    total_job_time = time.time() - processing_jobs[job_id]['submitted_at']
    
    job_result = {
        'job_id': job_id,
        'url': processing_jobs[job_id]['url'],
        'js_render': processing_jobs[job_id]['js_render'],
        'premium_proxy': processing_jobs[job_id]['premium_proxy'],
        'status': 'error',
        'submitted_at': processing_jobs[job_id]['submitted_at'],
        'completed_at': time.time(),
        'result': {
            'error': result.get('error'),
            'error_type': result.get('error_type'),
            'skipped_due_to_client_error': True
        },
        'credits_used': result.get('credits_used', 1),
        'total_time': total_job_time,
        'job_attempts': attempts,
        'job_retry_info': {
            'job_attempts_made': attempts,
            'job_successful': False,
            'skipped_immediately': True
        }
    }
    
    if parent_job_id:
        job_result['parent_job_id'] = parent_job_id
    
    await save_job_result_async(job_id, job_result)
    
    if parent_job_id:
        await update_child_job_status_async(parent_job_id, job_id, 'error')
    
    if job_id in processing_jobs:
        del processing_jobs[job_id]

async def handle_final_job_error_async(job_id, last_error, parent_job_id, max_retries):
    """Handle final job error after all retries"""
    error_result = {
        'url': processing_jobs[job_id]['url'],
        'js_render': processing_jobs[job_id]['js_render'],
        'premium_proxy': processing_jobs[job_id]['premium_proxy'],
        'status': 'error',
        'submitted_at': processing_jobs[job_id]['submitted_at'],
        'completed_at': time.time(),
        'result': {
            "error": f"Job failed after {max_retries} attempts",
            "last_error": last_error,
            "job_retry_info": {
                "job_attempts_made": max_retries,
                "job_successful": False,
                "final_error": "max_job_retries_exceeded"
            }
        },
        'credits_used': 0  # Don't charge for failed jobs
    }
    
    if parent_job_id:
        error_result['parent_job_id'] = parent_job_id
    
    await save_job_result_async(job_id, error_result)
    
    if parent_job_id:
        await update_child_job_status_async(parent_job_id, job_id, 'error')
    
    if job_id in processing_jobs:
        del processing_jobs[job_id]

async def save_job_result_async(job_id, job_data):
    """Async version of save_job_result"""
    try:
        # File I/O is still blocking, but we can run it in a thread pool if needed
        # For now, keeping it simple since file operations are usually fast
        file_path = RESULTS_DIR / f"{job_id}.json"
        
        # Convert any datetime objects to strings for JSON serialization
        json_data = json.dumps(job_data, indent=2, default=str)
        
        # Write to file
        with open(file_path, 'w') as f:
            f.write(json_data)
        
        return True
    except Exception as e:
        print(f"Error saving async job result: {str(e)}")
        return False

async def update_child_job_status_async(parent_job_id, child_job_id, status):
    """Async version of update_child_job_status"""
    try:
        file_path = RESULTS_DIR / f"{parent_job_id}.json"
        if not file_path.exists():
            return
        
        # Use threading lock for file operations (still needed for file safety)
        with parent_job_lock:
            # Read current content
            with open(file_path, 'r') as f:
                content = f.read()
            
            if not content.strip():
                return
            
            try:
                parent_job = json.loads(content)
            except json.JSONDecodeError:
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
        
        # Check completion after updating
        await check_parent_job_completion_async(parent_job_id)
        
    except Exception as e:
        print(f"Error updating child job status: {str(e)}")

async def check_parent_job_completion_async(parent_job_id):
    """Async version of parent job completion check"""
    try:
        file_path = RESULTS_DIR / f"{parent_job_id}.json"
        if not file_path.exists():
            return
        
        with parent_job_lock:
            # Read current content
            with open(file_path, 'r') as f:
                content = f.read()
            
            if not content.strip():
                return
            
            try:
                parent_job = json.loads(content)
            except json.JSONDecodeError:
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
                
                # Add timing information
                if 'started_at' in parent_job:
                    total_duration = completion_time - parent_job['started_at']
                    parent_job['timing'] = {
                        'total_duration_seconds': round(total_duration, 2),
                        'average_per_url_seconds': round(total_duration / parent_job['url_count'], 2)
                    }
                
                # Collect results from all child jobs
                await collect_child_results_async(parent_job)
                
                # Write back to file
                with open(file_path, 'w') as f:
                    json.dump(parent_job, f, indent=2)
        
    except Exception as e:
        print(f"Error checking parent job completion: {str(e)}")

async def collect_child_results_async(parent_job):
    """Collect results from child jobs asynchronously"""
    results = {}
    total_credits = 0
    timeout_count = 0
    error_count = 0
    success_count = 0
    
    for child in parent_job.get('child_jobs', []):
        child_job_id = child.get('job_id')
        child_url = child.get('url')
        
        # Get child job result
        child_result = get_job_result(child_job_id)  # This is still sync
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

async def async_worker():
    """Main async worker that processes jobs from the queue"""
    global worker_running
    worker_running = True
    
    print("🚀 Async worker started with event loop")
    
    # List to keep track of active job tasks
    active_tasks = []
    
    try:
        while worker_running:
            try:
                # Get job from queue with timeout
                try:
                    job_id = await asyncio.wait_for(job_queue.get(), timeout=300.0)  # 5 minutes
                except asyncio.TimeoutError:
                    # Timeout occurred - do cleanup and continue
                    print("⏰ 5-minute timeout reached - running cleanup")
                    await clean_old_results_async()
                    continue
                
                if job_id not in processing_jobs:
                    job_queue.task_done()
                    continue
                
                print(f"🔄 Processing async job: {job_id}")
                
                # Create and start the job task
                task = await job_manager.add_task(process_job_async(job_id))
                active_tasks.append(task)
                
                # Clean up completed tasks
                active_tasks = [t for t in active_tasks if not t.done()]
                
                # Mark queue task as done
                job_queue.task_done()
                
                # Brief yield to allow other coroutines to run
                await asyncio.sleep(0.001)
                
            except Exception as e:
                print(f"❌ Async worker error: {str(e)}")
                await asyncio.sleep(1)
        
        # Wait for all active tasks to complete when shutting down
        if active_tasks:
            print(f"⏳ Waiting for {len(active_tasks)} active tasks to complete...")
            await asyncio.gather(*active_tasks, return_exceptions=True)
    
    except Exception as e:
        print(f"❌ Critical async worker error: {str(e)}")
    finally:
        print("🛑 Async worker stopped")

async def clean_old_results_async():
    """Async version of cleanup function"""
    try:
        current_time = time.time()
        one_day_ago = current_time - (24*60*60)
        deleted_count = 0
        
        # File operations are still sync, but this is usually fast
        for file_path in RESULTS_DIR.glob("*.json"):
            try:
                file_mtime = file_path.stat().st_mtime
                if file_mtime < one_day_ago:
                    file_path.unlink()
                    deleted_count += 1
            except OSError:
                continue
        
        if deleted_count > 0:
            print(f"🧹 Async cleanup: deleted {deleted_count} old result files")
            
    except Exception as e:
        print(f"Error in async cleanup: {str(e)}")

def start_background_loop():
    """Start the background event loop in a separate thread"""
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Start the async worker in the background loop
    loop.run_until_complete(async_worker())

# Start background thread for async operations
background_thread = threading.Thread(target=start_background_loop, daemon=True)
background_thread.start()

# ✅ FLASK ROUTES

@app.route('/api/submit', methods=['GET'])
@async_route
async def submit_job():
    """Submit a new scraping job to the async queue"""
    url = request.args.get('url')
    js_render = request.args.get('js_render', 'true')
    premium_proxy = request.args.get('premium_proxy', 'false')
    job_id = request.args.get('job_id')
    
    if not url:
        return jsonify({"error": "No URL provided. Use '?url=example.com' parameter"}), 400
        
    if not job_id:
        job_id = f"async_job_{int(time.time() * 1000)}"
                                     
    # Check if job_id already exists
    if job_id in processing_jobs or (RESULTS_DIR / f"{job_id}.json").exists():
        return jsonify({"error": f"Job ID '{job_id}' already exists. Please use a unique job ID"}), 400
        
    # Add https:// prefix if not present
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Create a new job
    processing_jobs[job_id] = {
        'url': url,
        'js_render': js_render,
        'premium_proxy': premium_proxy,
        'status': 'queued',
        'submitted_at': time.time(),
        'completed_at': None,
        'credits_used': calculate_credits(js_render, premium_proxy)
    }
    
    # Add to async queue using background loop
    future = run_in_background_loop(job_queue.put(job_id))
    if future:
        try:
            future.result(timeout=5)  # Wait max 5 seconds for queue operation
        except Exception as e:
            return jsonify({"error": f"Failed to queue job: {str(e)}"}), 500
    
    return jsonify({  
        "job_id": job_id,
        "status": "queued",
        "url": url,
        "message": f"Async job queued. Check status at /api/status?job_id={job_id}",
        "processing_type": "asynchronous"
    })

@app.route('/api/multiple_domains', methods=['GET'])
@async_route
async def submit_multiple_domains_job():
    """Submit multiple domain scraping job - Flask + Async version"""
    try:
        raw_urls = request.args.getlist('url')
        js_render = request.args.get('js_render', 'false')
        premium_proxy = request.args.get('premium_proxy', 'false')
        parent_job_id = request.args.get('job_id')
        
        if not raw_urls:
            return jsonify({"error": "No URLs provided"}), 400
        
        # Normalize URLs
        urls = []
        invalid_urls = []
        
        for raw_url in raw_urls:
            try:
                normalized_url = raw_url.strip()
                if not normalized_url.startswith(('http://', 'https://')):
                    normalized_url = 'https://' + normalized_url
                
                urls.append(normalized_url)
                print(f"📝 Normalized: {raw_url} → {normalized_url}")
                
            except Exception as e:
                print(f"❌ Invalid URL: {raw_url} - {e}")
                invalid_urls.append(raw_url)
        
        if not urls:
            return jsonify({"error": "No valid URLs provided after normalization"}), 400
        
        if not parent_job_id:
            parent_job_id = f"flask_multi_{int(time.time())}"
        
        # Create parent job tracker
        tracker = {
            'parent_job_id': parent_job_id,
            'status': 'queued',
            'original_urls': raw_urls,
            'normalized_urls': urls,
            'invalid_urls': invalid_urls,
            'url_count': len(urls),
            'js_render': js_render,
            'premium_proxy': premium_proxy,
            'child_jobs': [],
            'submitted_at': time.time(),
            'started_at': time.time(),
            'completed_at': None,
            'estimated_credits': calculate_credits(js_render, premium_proxy) * len(urls),
            'processing_type': 'flask_asynchronous'
        }
        
        # Save tracker file
        RESULTS_DIR.mkdir(exist_ok=True)
        tracker_file_path = RESULTS_DIR / f"{parent_job_id}.json"
        
        with open(tracker_file_path, 'w') as f:
            json.dump(tracker, f, indent=2)
        
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
        
        with open(tracker_file_path, 'w') as f:
            json.dump(tracker, f, indent=2)
        
        # Add jobs to async queue using background loop
        for child_job in child_jobs:
            processing_jobs[child_job['job_id']] = child_job
            future = run_in_background_loop(job_queue.put(child_job['job_id']))
            if future:
                try:
                    future.result(timeout=1)  # Quick timeout per job
                except Exception as e:
                    print(f"Warning: Failed to queue child job {child_job['job_id']}: {e}")
        
        print(f"🎯 FLASK MULTI-DOMAIN JOB: {parent_job_id} with {len(urls)} URLs")
        print(f"📋 Queued {len(child_jobs)} async jobs for processing")
        
        return jsonify({
            "message": f"Flask multi-domain job {parent_job_id} submitted successfully",
            "parent_job_id": parent_job_id,
            "total_urls": len(urls),
            "valid_urls": len(urls),
            "invalid_urls": len(invalid_urls),
            "estimated_credits": tracker['estimated_credits'],
            "status": "queued",
            "processing_type": "flask_asynchronous",
            "url_normalization": {
                "original_urls": raw_urls,
                "normalized_urls": urls,
                "invalid_urls": invalid_urls
            },
            "note": f"All {len(urls)} URLs have been queued for async processing."
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/status', methods=['GET'])
def get_job_status():
    """Get the status or result of a job by ID - Flask version"""
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
            "processing_type": "flask_asynchronous",
            "message": "Flask async job is still processing..."
        })
    
    # Check in file system
    result = get_job_result(job_id)
    if result:
        if 'child_jobs' in result:
            # Multi-domain job
            response = {
                "job_id": job_id,
                "status": result['status'],
                "url_count": result.get('url_count', 0),
                "submitted_at": result['submitted_at'],
                "completed_at": result.get('completed_at'),
                "processing_type": result.get('processing_type', 'flask_asynchronous')
            }
            
            if result['status'] == 'completed':
                response["total_credits_used"] = result.get('total_credits_used', 0)
                response["results"] = result.get('results', {})
                response["summary"] = result.get('summary', {})
                response["timing"] = result.get('timing', {})
            else:
                child_statuses = {}
                for child in result.get('child_jobs', []):
                    child_statuses[child.get('url')] = child.get('status')
                response["progress"] = child_statuses
            
            return jsonify(response)
        else:
            # Single job
            response = {
                "job_id": job_id,
                "status": result['status'],
                "url": result['url'],
                "submitted_at": result['submitted_at'],
                "completed_at": result['completed_at'],
                "credits_used": result.get('credits_used', 0),
                "processing_type": result.get('processing_type', 'flask_asynchronous')
            }
            
            if result['status'] == 'completed':
                response["result"] = result['result']
            elif result['status'] == 'timeout':
                response["timeout"] = result['result']
            else:
                response["error"] = result['result']
            
            return jsonify(response)
    
    return jsonify({"error": f"Job '{job_id}' not found."}), 404

@app.route('/api/queue', methods=['GET'])
def get_queue_status():
    """Get async queue status - Flask version"""
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
    
    # Get queue size from background loop
    queue_size = 0
    if loop and not loop.is_closed():
        try:
            future = asyncio.run_coroutine_threadsafe(
                asyncio.create_task(asyncio.coroutine(lambda: job_queue.qsize())()),
                loop
            )
            queue_size = future.result(timeout=1)
        except:
            queue_size = "unavailable"
    
    return jsonify({
        "queue_size": queue_size,
        "processing_jobs": len(processing_jobs),
        "completed_jobs": completed_count,
        "active_tasks": len(job_manager.active_tasks) if job_manager else 0,
        "max_concurrent": MAX_CONCURRENT_REQUESTS,
        "processing_type": "flask_asynchronous",
        "background_loop_status": "running" if loop and not loop.is_closed() else "stopped",
        "status_counts": {
            'queued': processing_status_counts['queued'],
            'processing': processing_status_counts['processing'],
            'completed': completed_count
        },
        "processing_jobs_list": {
            job_id: {
                "status": job["status"],
                "url": job["url"],
                "submitted_at": job["submitted_at"],
                "credits_used": job.get("credits_used", 0)
            } for job_id, job in processing_jobs.items()
        }
    })

@app.route('/api/clear', methods=['GET'])
def clear_completed():
    """Clear all completed jobs - Flask version"""
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

@app.route('/api/delete', methods=['GET'])
def delete_job_result():
    """Delete a specific job result by job_id - Flask version"""
    job_id = request.args.get('job_id')
    
    if not job_id:
        return jsonify({"error": "No job_id provided"}), 400
    
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
            # Delete all child jobs first
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
        
        else:
            # Check if this is a child job (has parent_job_id field)
            parent_job_id = job_data.get('parent_job_id')
            
            if parent_job_id:
                # This is a child job - update parent job's child list
                parent_file_path = RESULTS_DIR / f"{parent_job_id}.json"
                
                if parent_file_path.exists():
                    try:
                        with open(parent_file_path, 'r') as f:
                            parent_data = json.load(f)
                        
                        # Remove this child from parent's child_jobs list
                        if 'child_jobs' in parent_data:
                            parent_data['child_jobs'] = [
                                child for child in parent_data['child_jobs']
                                if child.get('job_id') != job_id
                            ]
                            
                            # Update parent file
                            with open(parent_file_path, 'w') as f:
                                json.dump(parent_data, f, indent=2)
                    
                    except Exception as e:
                        print(f"Warning: Could not update parent job {parent_job_id}: {e}")
                
                # Delete the child job file
                file_path.unlink()
                deleted_jobs.append(job_id)
                
                return jsonify({
                    "message": f"Successfully deleted child job '{job_id}' and updated parent job '{parent_job_id}'",
                    "deleted_jobs": deleted_jobs,
                    "child_job_id": job_id,
                    "parent_job_id": parent_job_id
                })
            
            else:
                # This is a standalone job
                file_path.unlink()
                deleted_jobs.append(job_id)
                
                return jsonify({
                    "message": f"Successfully deleted standalone job '{job_id}'",
                    "deleted_jobs": deleted_jobs,
                    "job_id": job_id
                })
    
    except FileNotFoundError:
        return jsonify({
            "error": f"Job file not found: {job_id}"
        }), 404
    
    except PermissionError:
        return jsonify({
            "error": f"Permission denied when deleting job: {job_id}"
        }), 403
    
    except json.JSONDecodeError:
        return jsonify({
            "error": f"Invalid JSON in job file: {job_id}"
        }), 500
    
    except Exception as e:
        print(f"❌ Error in delete_job_result: {str(e)}")
        return jsonify({
            "error": f"Error deleting job result: {str(e)}"
        }), 500

@app.route('/api/debug', methods=['GET'])
def debug_routes():
    """Debug route to check registered routes - Flask version"""
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            'endpoint': rule.endpoint,
            'methods': list(rule.methods),
            'rule': rule.rule
        })
    return jsonify({"registered_routes": routes})

@app.route('/api/scrape', methods=['GET'])
@async_route
async def direct_scrape():
    """Direct scraping endpoint (legacy) - Flask + Async version"""
    url = request.args.get('url')
    js_render = request.args.get('js_render', 'true')
    premium_proxy = request.args.get('premium_proxy', 'false')
    
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    
    # Add https:// prefix if not present
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        # Direct async scraping without queueing
        result = await scrape_complete_homepage_async(url, js_render, premium_proxy)
        
        if result.get('success'):
            return jsonify({ 
                "status": "completed",
                "url": url,
                "result": result,
                "credits_used": result.get('credits_used', 0),
                "processing_type": "direct_flask_async"
            })
        else:
            return jsonify({
                "status": "error",
                "url": url,
                "error": result,
                "credits_used": result.get('credits_used', 0)
            }), 500
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "url": url,
            "error": str(e)
        }), 500

@app.route('/', methods=['GET'])
def home():
    """Flask home page"""
    return '''
    <html>
        <head>
            <title>Flask + Asyncio Web Scraper API</title>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }
                .highlight { background-color: #e7f3ff; padding: 10px; border-left: 4px solid #2196F3; margin: 10px 0; }
                .warning { background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; }
                code { background: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
                table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f4f4f4; }
            </style>
        </head>
        <body>
            <h1>🚀 Flask + Asyncio Web Scraper API</h1>
            
            <div class="highlight">
                <strong>⚡ Flask + Async Architecture!</strong><br>
                This API combines Flask's familiar routing with asyncio for efficient concurrent processing.
                <ul>
                    <li>✅ Flask routes for familiar development experience</li>
                    <li>✅ Asyncio background worker for concurrent scraping</li>
                    <li>✅ Non-blocking HTTP requests with aiohttp</li>
                    <li>✅ Thread-safe communication between Flask and async worker</li>
                </ul>
            </div>
            
            <div class="warning">
                <strong>⚠️ Architecture Notes:</strong><br>
                <ul>
                    <li>Flask routes handle HTTP requests synchronously</li>
                    <li>Background thread runs asyncio event loop</li>
                    <li>Jobs are queued and processed asynchronously</li>
                    <li>Results are stored in files for retrieval</li>
                </ul>
            </div>
            
            <h2>📋 API Endpoints</h2>
            
            <h3>1. Submit Single Job</h3>
            <code>GET /api/submit?url=example.com&job_id=my_job&js_render=true</code>
            
            <h3>2. Submit Multiple Domains</h3>
            <code>GET /api/multiple_domains?url=site1.com&url=site2.com&js_render=false</code>
            
            <h3>3. Check Status</h3>
            <code>GET /api/status?job_id=my_job</code>
            
            <h3>4. Queue Status</h3>
            <code>GET /api/queue</code>
            
            <h3>5. Direct Scrape (No Queue)</h3>
            <code>GET /api/scrape?url=example.com&js_render=true</code>
            
            <h2>🏗️ Architecture Comparison</h2>
            <table>
                <tr><th>Aspect</th><th>Pure Flask</th><th>Flask + Asyncio</th><th>Pure Quart</th></tr>
                <tr><td>Route Handling</td><td>Synchronous</td><td>Sync + Async Decorator</td><td>Native Async</td></tr>
                <tr><td>Background Jobs</td><td>Threads/Celery</td><td>Asyncio Worker</td><td>Native Async</td></tr>
                <tr><td>Learning Curve</td><td>Easy</td><td>Medium</td><td>Medium</td></tr>
                <tr><td>Performance</td><td>Limited</td><td>High for I/O</td><td>Highest</td></tr>
                <tr><td>Ecosystem</td><td>Largest</td><td>Flask + Asyncio</td><td>Growing</td></tr>
            </table>
            
            <p><strong>🎯 This approach gives you Flask familiarity with asyncio performance!</strong></p>
        </body>
    </html>
    '''

# Utility functions (keeping sync versions)
def calculate_credits(js_render, premium_proxy):
    """Calculate the credit cost based on the configuration"""
    js = js_render.lower() == 'true'
    premium = premium_proxy.lower() == 'true'
    
    if premium:
        if js:
            return 25
        else:
            return 10
    elif js:
        return 5
    else:
        return 1

def format_to_single_key(data):
    """Format structured data into a single key"""
    page_content = []
    
    # Add page title
    if data.get('page_info', {}).get('title'):
        page_content.append(f"PAGE TITLE: {data['page_info']['title']}")
        page_content.append("")
    
    # Add headings
    headings = data.get('main_content', {}).get('headings', {})
    for level in ['h1', 'h2', 'h3']:
        heading_list = headings.get(level, [])
        if heading_list:
            page_content.append(f"{level.upper()} HEADINGS:")
            for heading in heading_list:
                page_content.append(f"  {heading}")
            page_content.append("")
    
    # Add paragraphs
    paragraphs = data.get('main_content', {}).get('paragraphs', [])
    if paragraphs:
        page_content.append("MAIN CONTENT:")
        for i, paragraph in enumerate(paragraphs[:10], 1):  # Limit to first 10
            para_text = paragraph[:500] + ('...' if len(paragraph) > 500 else '')
            page_content.append(f"  {i}. {para_text}")
        page_content.append("")
    
    # Add images
    images = data.get('images', [])
    if images:
        page_content.append(f"IMAGES: {len(images)} images found")
        for i, img in enumerate(images[:20], 1):  # Limit to first 20
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
    
    complete_content = "\n".join(page_content).strip()
    
    return {
        "page_content": complete_content,
        "metadata": {
            "url": data.get('url', ''),
            "extraction_method": data.get('extraction_method', 'flask_async_parsing'),
            "retry_info": data.get('retry_info', {}),
            "credits_used": data.get('credits_used', 0)
        }
    }

def get_job_result(job_id):
    """Get job result from file"""
    try:
        file_path = RESULTS_DIR / f"{job_id}.json"
        if not file_path.exists():
            return None
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        return data
    except Exception as e:
        print(f"Error reading job result: {str(e)}")
        return None

async def cleanup_session():
    """Cleanup aiohttp session on shutdown"""
    global session
    if session and not session.closed:
        await session.close()
        print("🔗 Closed aiohttp session")

if __name__ == '__main__':
    # Ensure results directory exists
    RESULTS_DIR.mkdir(exist_ok=True)
    
    # Set up graceful shutdown
    def signal_handler(signum, frame):
        global worker_running
        print(f"\n🛑 Received signal {signum}, shutting down...")
        worker_running = False
        
        # Cleanup session in background loop
        if loop and not loop.is_closed():
            asyncio.run_coroutine_threadsafe(cleanup_session(), loop)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Give background thread time to start
    time.sleep(2)
    
    # Run Flask app
    port = int(os.environ.get('PORT', 8080))
    print(f"🌶️ Starting Flask app on port {port}")
    print("🔄 Background asyncio worker should be running...")
    
    try:
        app.run(host='0.0.0.0', port=port, debug=True, threaded=True)
    except KeyboardInterrupt:
        print("\n🛑 Flask shutdown...")
    finally:
        worker_running = False
        if loop and not loop.is_closed():
            # Try to cleanup gracefully
            try:
                asyncio.run_coroutine_threadsafe(cleanup_session(), loop).result(timeout=5)
            except:
                pass
