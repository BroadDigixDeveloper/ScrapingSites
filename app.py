from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import json
import re
import os

app = Flask(__name__)

API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
# ScrapingBee endpoint
SCRAPINGBEE_URL = "https://app.scrapingbee.com/api/v1/"

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

@app.route('/api/scrape', methods=['GET'])
def scrape_api():
    """
    API endpoint to scrape a website
    
    Query parameters:
    - url: The URL to scrape (required)
    - js_render: Whether to use JavaScript rendering (true/false, default: true)
    - premium_proxy: Whether to use premium proxy (true/false, default: false)
    
    Returns:
    - JSON response with the scraped data or error message
    """
    url = request.args.get('url')
    js_render = request.args.get('js_render', 'true')
    premium_proxy = request.args.get('premium_proxy', 'false')
    
    if not url:
        return jsonify({"error": "No URL provided. Use '?url=example.com' parameter"}), 400
    
    # Add http:// prefix if not present
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Calculate estimated credits
    credit_cost = 1  # Base cost
    if js_render.lower() == 'true':
        credit_cost = 5  # Base cost with JS rendering
    if premium_proxy.lower() == 'true':
        if js_render.lower() == 'true':
            credit_cost = 25  # Premium proxy with JS rendering
        else:
            credit_cost = 10  # Premium proxy without JS rendering
    
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
            <title>Web Scraper API</title>
            <style>
                body { font-family: Arial, sans-serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 20px; }
                code { background: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
                pre { background: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }
                table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #f4f4f4; }
                tr:nth-child(even) { background-color: #f9f9f9; }
            </style>
        </head>
        <body>
            <h1>Web Scraper API</h1>
            <p>Use the API endpoint <code>/api/scrape</code> to scrape a website.</p>
            
            <h2>Parameters:</h2>
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
            
            <h2>Example:</h2>
            <pre>GET /api/scrape?url=example.com&js_render=true&premium_proxy=false</pre>
            <p>This will return a JSON object with the scraped website data.</p>
            
            <h3>Testing Tool:</h3>
            <form action="/api/scrape" method="get">
                <p>
                    <label for="url">URL to scrape:</label>
                    <input type="text" id="url" name="url" placeholder="example.com" required style="width: 300px;">
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
                    <button type="submit">Scrape Website</button>
                </p>
            </form>
        </body>
    </html>
    '''

if __name__ == '__main__':
    # Run the Flask app
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
