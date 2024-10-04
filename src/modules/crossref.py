import requests

def doi_crossref(doi: str) -> dict:
    """
    An Query to CrossRef API to extract information from a DOI,
    """
    url = f'https://api.crossref.org/works/{doi}'
    try:
        # Make the query
        response = requests.get(url)

        # Verify if the query has a reponse
        if response.status_code == 200:
            data = response.json()
            # Extract the information
            info = {
                'title': data['message'].get('title', ['No Title'])[0],
                'author': ', '.join([f"{a['given']} {a['family']}" for a in data['message'].get('author', [])]),
                'journal': data['message'].get('container-title', ['No Journal'])[0],
                'publisher': data['message'].get('publisher', 'No Publisher'),
                'published_date': data['message'].get('issued', {}).get('date-parts', [[None]])[0],
                'DOI': data['message'].get('DOI', 'No DOI'),
                'URL': data['message'].get('URL', 'No URL'),
                'abstract': data['message'].get('abstract', 'No Abstract')
            }
            return info
        else:
            return {"error": f"Error {response.status_code}: Troubleshooting detected."}

    except ValueError as e:
        return {"error": f"DOI isn't a String: {e}"}

