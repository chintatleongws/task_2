import requests
from bs4 import BeautifulSoup


url_list = [
    "https://www.geeksforgeeks.org/dsa/dsa-tutorial-learn-data-structures-and-algorithms/",
    "https://airflow.apache.org/docs/apache-airflow/stable/index.html"
]
url_names = [
    "DSA Tutorial",
    "Airflow Documentation"
]

for i, url in enumerate(url_list):
    # stand in url, can swap a real one
    response = requests.get(url)

    # Add in bs4
    soup = BeautifulSoup(response.text, 'html.parser')
    print(soup.prettify())  # prints well-formatted HTML
    with open(f"./data/bronze/raw_html/{url_names[i]}.html", "w", encoding="utf-8") as f:
        f.write(soup.prettify())