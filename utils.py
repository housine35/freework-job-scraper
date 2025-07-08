from bs4 import BeautifulSoup
import re

def clean_html(html_text):
    """Remove HTML tags and extract clean text."""
    if not html_text:
        return 'N/A'
    
    soup = BeautifulSoup(html_text, 'html.parser')
    for script in soup(["script", "style"]):
        script.decompose()
    
    text = soup.get_text(separator=' ')
    return re.sub(r'\s+', ' ', text).strip() or 'N/A'
