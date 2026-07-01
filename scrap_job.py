import requests
from bs4 import BeautifulSoup


# stand in url, can swap a real one
url = "https://www.geeksforgeeks.org/dsa/dsa-tutorial-learn-data-structures-and-algorithms/"
response = requests.get(url)

# Add in bs4
soup = BeautifulSoup(response.text, 'html.parser')
print(soup.prettify())  # prints well-formatted HTML

with open("sample_parsed_page.html", "w", encoding="utf-8") as f:
    f.write(soup.prettify())