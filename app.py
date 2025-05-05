from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import json
import re
import os
import threading
import time
import uuid
import redis
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
# ScrapingBee endpoint
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"

# Redis connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL)

# Constants
MAX_CONCURRENT_REQUESTS = 5
QUEUE_KEY = "scraper:queue"
JOB_PREFIX = "scraper:job:"
WORKER_LOCK_KEY = "scraper:worker_lock"
WORKER_LOCK_EXPIRY = 60  # 60 seconds lock expiry

def scrape_complete_homepage(url, use_js_render='true', use_premium_proxy='false'):
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

    try:
        response = requests.get(SCRAPINGBEE_URL, params=params)
        if response.status_code != 200:
            return {"error": f"ScrapingBee failed: {response.status_code}", "details": response.text}

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

        # Meta
        desc = soup.find('meta', {'name': 'description'})
        if desc: data['page_info']['meta_description'] = desc.get('content', '')
        keywords = soup.find('meta', {'name': 'keywords'})
        if keywords: data['page_info']['meta_keywords'] = keywords.get('content', '')

        # Header
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

        # Sections
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

        return data

    except Exception as e:
        return {"error": str(e)}

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

def get_job(job_id):
    """Get job data from Redis"""
    job_key = f"{JOB_PREFIX}{job_id}"
    job_data = redis_client.get(job_key)
    
    if job_data:
        return json.loads(job_data)
    return None

def save_job(job_id, job_data):
    """Save job data to Redis"""
    job_key = f"{JOB_PREFIX}{job_id}"
    redis_client.set(job_key, json.dumps(job_data))

def acquire_worker_lock():
    """Try to acquire the worker lock with expiry"""
    return redis_client.set(WORKER_LOCK_KEY, "1", ex=WORKER_LOCK_EXPIRY, nx=True)

def release_worker_lock():
    """Release the worker lock"""
    redis_client.delete(WORKER_LOCK_KEY)

def worker():
    """Background worker to process queued requests in batches of 5"""
    print("Worker thread started...")
    
    while True:
        try:
            # Try to acquire lock to prevent multiple workers across processes
            if not acquire_worker_lock():
                # Another worker is already running
                time.sleep(5)
                continue
                
            # Get job IDs from queue (up to MAX_CONCURRENT_REQUESTS)
            batch = []
            for _ in range(MAX_CONCURRENT_REQUESTS):
                job_id = redis_client.lpop(QUEUE_KEY)
                if job_id:
                    job_id = job_id.decode('utf-8')
                    job_data = get_job(job_id)
                    if job_data:
                        batch.append(job_id)
                else:
                    break
            
            if not batch:
                # No jobs, release lock and sleep
                release_worker_lock()
                time.sleep(2)
                continue
            
            # Process the batch with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {
                    executor.submit(process_job, job_id): job_id 
                    for job_id in batch
                }
                
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        job_id = futures[future]
                        job_data = get_job(job_id)
                        if job_data:
                            job_data['status'] = 'error'
                            job_data['result'] = {"error": str(e)}
                            job_data['completed_at'] = time.time()
                            save_job(job_id, job_data)
            
            # Release lock when done
            release_worker_lock()
            
        except Exception as e:
            print(f"Worker error: {str(e)}")
            # Make sure to release lock if we hit an error
            release_worker_lock()
            time.sleep(2)

def process_job(job_id):
    """Process a single job"""
    try:
        job_data = get_job(job_id)
        if not job_data:
            return
            
        # Update status to processing
        job_data['status'] = 'processing'
        save_job(job_id, job_data)
        
        # Call the scraping function
        result = scrape_complete_homepage(
            job_data['url'], 
            job_data['js_render'], 
            job_data['premium_proxy']
        )
        
        # Update job with result
        job_data['status'] = 'completed'
        job_data['result'] = result
        job_data['completed_at'] = time.time()
        
        # Calculate credits
        credit_cost = calculate_credits(job_data['js_render'], job_data['premium_proxy'])
        job_data['credits_used'] = credit_cost
        
        # Save updated job
        save_job(job_id, job_data)
        
    except Exception as e:
        try:
            # Try to update job with error
            job_data = get_job(job_id)
            if job_data:
                job_data['status'] = 'error'
                job_data['result'] = {"error": str(e)}
                job_data['completed_at'] = time.time()
                save_job(job_id, job_data)
        except:
            # If we can't even update the job, just pass
            pass

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
        # Generate a UUID-based ID if not provided
        job_id = str(uuid.uuid4())
        
    # Check if job_id already exists
    if get_job(job_id):
        return jsonify({"error": f"Job ID '{job_id}' already exists. Please use a unique job ID"}), 400
        
    # Add http:// prefix if not present
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Create a new job
    job_data = {
        'url': url,
        'js_render': js_render,
        'premium_proxy': premium_proxy,
        'status': 'queued',
        'submitted_at': time.time(),
        'completed_at': None,
        'result': None,
        'credits_used': calculate_credits(js_render, premium_proxy)
    }
    
    # Save job to Redis
    save_job(job_id, job_data)
    
    # Add to queue
    redis_client.rpush(QUEUE_KEY, job_id)
    
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
        
    job_data = get_job(job_id)
    if not job_data:
        return jsonify({"error": f"Job '{job_id}' not found"}), 404
    
    # If job is completed, return the result
    if job_data['status'] == 'completed':
        return jsonify({
            "job_id": job_id,
            "status": "completed",
            "url": job_data['url'],
            "submitted_at": job_data['submitted_at'],
            "completed_at": job_data['completed_at'],
            "credits_used": job_data['credits_used'],
            "result": job_data['result']
        })
    elif job_data['status'] == 'error':
        return jsonify({
            "job_id": job_id,
            "status": "error",
            "url": job_data['url'],
            "submitted_at": job_data['submitted_at'],
            "completed_at": job_data['completed_at'],
            "error": job_data['result']
        })
    else:
        # If job is still queued or processing
        return jsonify({
            "job_id": job_id,
            "status": job_data['status'],
            "url": job_data['url'],
            "submitted_at": job_data['submitted_at'],
            "message": "processing..."
        })

@app.route('/api/queue', methods=['GET'])
def get_queue_status():
    """Get status of all jobs in the queue"""
    # Get queue size
    queue_size = redis_client.llen(QUEUE_KEY)
    
    # Get all job keys
    all_job_keys = redis_client.keys(f"{JOB_PREFIX}*")
    
    # Count jobs by status
    status_counts = {
        'queued': 0,
        'processing': 0,
        'completed': 0,
        'error': 0
    }
    
    jobs_summary = {}
    
    for key in all_job_keys:
        job_id = key.decode('utf-8').replace(JOB_PREFIX, '')
        job_data = get_job(job_id)
        
        if job_data:
            if job_data['status'] in status_counts:
                status_counts[job_data['status']] += 1
                
            jobs_summary[job_id] = {
                "status": job_data["status"],
                "url": job_data["url"],
                "submitted_at": job_data["submitted_at"],
                "credits_used": job_data.get("credits_used", 0)
            }
    
    return jsonify({
        "queue_size": queue_size,
        "active_jobs": len(all_job_keys),
        "status_counts": status_counts,
        "jobs": jobs_summary
    })

@app.route('/api/clear', methods=['GET'])
def clear_completed():
    """Clear completed and error jobs"""
    count = 0
    all_job_keys = redis_client.keys(f"{JOB_PREFIX}*")
    
    for key in all_job_keys:
        job_id = key.decode('utf-8').replace(JOB_PREFIX, '')
        job_data = get_job(job_id)
        
        if job_data and job_data['status'] in ['completed', 'error']:
            redis_client.delete(key)
            count += 1
    
    return jsonify({
        "message": f"Cleared {count} completed/error jobs",
        "remaining_jobs": len(all_job_keys) - count
    })

@app.route('/api/scrape', methods=['GET'])
def scrape_api():
    """Legacy endpoint for immediate scraping (no queuing)"""
    url = request.args.get('url')
    js_render = request.args.get('js_render', 'true')
    premium_proxy = request.args.get('premium_proxy', 'false')
    
    if not url:
        return jsonify({"error": "No URL provided. Use '?url=example.com' parameter"}), 400
    
    # Add http:// prefix if not present
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Calculate estimated credits
    credit_cost = calculate_credits(js_render, premium_proxy)
    
    result = scrape_complete_homepage(url, js_render, premium_proxy)
    
    # Add credit information to the result
    if isinstance(result, dict) and not result.get('error'):
        result['credits_used'] = credit_cost
    
    return jsonify(result)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring"""
    # Check Redis connection
    try:
        redis_client.ping()
        redis_status = "connected"
    except Exception as e:
        redis_status = f"error: {str(e)}"
    
    # Check if worker is running
    worker_status = "running" if redis_client.exists(WORKER_LOCK_KEY) else "idle"
    
    return jsonify({
        "status": "healthy",
        "redis": redis_status,
        "worker": worker_status,
        "queue_size": redis_client.llen(QUEUE_KEY),
        "api_key_configured": bool(API_KEY)
    })

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
                <p>Use the <code>/api/clear</code> endpoint to remove completed and error jobs.</p>
                
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
            
            <div class="endpoint">
                <h2>6. Health Check</h2>
                <p>Use the <code>/api/health</code> endpoint to check the health of the service.</p>
                
                <h3>Example:</h3>
                <pre>GET /api/health</pre>
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

# Start the worker thread
worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()

if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
