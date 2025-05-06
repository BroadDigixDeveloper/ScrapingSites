from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import json
import re
import os
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
# ScrapingBee endpoint
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"

# In-memory job storage with two separate dictionaries
processing_jobs = {}  # Jobs being processed
completed_jobs = {}   # Completed jobs with results
request_queue = queue.Queue()
MAX_CONCURRENT_REQUESTS = 5
worker_running = False

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

def worker():
    """Background worker to process queued requests in batches of 5"""
    global worker_running
    worker_running = True
    
    while worker_running:
        try:
            # Get up to 5 jobs from the queue
            batch = []
            for _ in range(MAX_CONCURRENT_REQUESTS):
                try:
                    job_id = request_queue.get(block=True, timeout=1)  # Block with timeout
                    if job_id in processing_jobs:
                        batch.append(job_id)
                    request_queue.task_done()
                except queue.Empty:
                    break  # No more jobs in queue
            
            if not batch:
                continue  # Skip if no jobs found
            
            # Process the batch with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {
                    executor.submit(
                        process_job, 
                        job_id
                    ): job_id for job_id in batch
                }
                
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        job_id = futures[future]
                        # Move job to completed with error status
                        if job_id in processing_jobs:
                            job = processing_jobs[job_id]
                            completed_jobs[job_id] = {
                                'url': job['url'],
                                'js_render': job['js_render'],
                                'premium_proxy': job['premium_proxy'],
                                'status': 'error',
                                'submitted_at': job['submitted_at'],
                                'completed_at': time.time(),
                                'result': {"error": str(e)},
                                'credits_used': job.get('credits_used', 0)
                            }
                            # Remove from processing
                            del processing_jobs[job_id]
        
        except Exception as e:
            print(f"Worker error: {str(e)}")
            time.sleep(1)

def process_job(job_id):
    """Process a single job"""
    try:
        if job_id not in processing_jobs:
            print(f"Warning: Job {job_id} not found in processing queue")
            return
            
        job = processing_jobs[job_id]
        job['status'] = 'processing'
        
        # Call the scraping function
        result = scrape_complete_homepage(
            job['url'], 
            job['js_render'], 
            job['premium_proxy']
        )
        
        # Calculate credits
        credit_cost = calculate_credits(job['js_render'], job['premium_proxy'])
        
        # Create a completed job entry
        completed_jobs[job_id] = {
            'url': job['url'],
            'js_render': job['js_render'],
            'premium_proxy': job['premium_proxy'],
            'status': 'completed',
            'submitted_at': job['submitted_at'],
            'completed_at': time.time(),
            'result': result,
            'credits_used': credit_cost
        }
        
        # Remove from processing queue
        if job_id in processing_jobs:
            del processing_jobs[job_id]
            
    except Exception as e:
        # Handle errors by moving to completed with error status
        if job_id in processing_jobs:
            job = processing_jobs[job_id]
            completed_jobs[job_id] = {
                'url': job['url'],
                'js_render': job['js_render'],
                'premium_proxy': job['premium_proxy'],
                'status': 'error',
                'submitted_at': job['submitted_at'],
                'completed_at': time.time(),
                'result': {"error": str(e)},
                'credits_used': job.get('credits_used', 0)
            }
            # Remove from processing
            del processing_jobs[job_id]
        else:
            print(f"Error processing job {job_id} that's not in queue: {str(e)}")

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
    
    # Check both queues to ensure job_id is unique
    if job_id in processing_jobs or job_id in completed_jobs:
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
            "url": job['url'],
            "submitted_at": job['submitted_at'],
            "message": "Job is still processing..."
        })
    
    # Then check completed queue
    elif job_id in completed_jobs:
        job = completed_jobs[job_id]
        
        # Create response based on job status
        if job['status'] == 'completed':
            response = {
                "job_id": job_id,
                "status": "completed",
                "url": job['url'],
                "submitted_at": job['submitted_at'],
                "completed_at": job['completed_at'],
                "credits_used": job['credits_used'],
                "result": job['result']
            }
        else:  # Error status
            response = {
                "job_id": job_id,
                "status": "error",
                "url": job['url'],
                "submitted_at": job['submitted_at'],
                "completed_at": job['completed_at'],
                "error": job['result']
            }
        
        # Remove from completed queue after returning results
        del completed_jobs[job_id]
        
        return jsonify(response)
    
    # Job not found in either queue
    else:
        return jsonify({"error": f"Job '{job_id}' not found. It may have already been retrieved or never existed."}), 404

@app.route('/api/queue', methods=['GET'])
def get_queue_status():
    """Get status of all jobs in both queues"""
    # Count jobs by status
    status_counts = {
        'queued': 0,
        'processing': 0,
        'completed': 0,
        'error': 0
    }
    
    # Count processing jobs
    for job in processing_jobs.values():
        if job['status'] in status_counts:
            status_counts[job['status']] += 1
    
    # Count completed jobs
    for job in completed_jobs.values():
        if job['status'] in status_counts:
            status_counts[job['status']] += 1
    
    return jsonify({
        "queue_size": request_queue.qsize(),
        "processing_jobs": len(processing_jobs),
        "completed_jobs": len(completed_jobs),
        "status_counts": status_counts,
        "processing_queue": {
            job_id: {
                "status": job["status"],
                "url": job["url"],
                "submitted_at": job["submitted_at"],
                "credits_used": job.get("credits_used", 0)
            } for job_id, job in processing_jobs.items()
        },
        "completed_queue": {
            job_id: {
                "status": job["status"],
                "url": job["url"],
                "submitted_at": job["submitted_at"],
                "completed_at": job["completed_at"],
                "credits_used": job.get("credits_used", 0)
            } for job_id, job in completed_jobs.items()
        }
    })

@app.route('/api/clear', methods=['GET'])
def clear_completed():
    """Clear completed jobs"""
    count = len(completed_jobs)
    completed_jobs.clear()
    
    return jsonify({
        "message": f"Cleared {count} completed jobs",
        "remaining_processing_jobs": len(processing_jobs),
        "remaining_completed_jobs": len(completed_jobs)
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
                <p>Use the <code>/api/clear</code> endpoint to remove completed jobs from memory.</p>
                
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

# Start the worker thread when the app starts
if __name__ == '__main__':
    # Start the worker thread
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    # Run the Flask app
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
else:
    # For when importing as a module or running with a WSGI server
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()
