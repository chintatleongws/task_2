import requests
from bs4 import BeautifulSoup


# stand in url, can swap a real one
url = "https://www.geeksforgeeks.org/dsa/dsa-tutorial-learn-data-structures-and-algorithms/"
response = requests.get(url)
print(response.text)