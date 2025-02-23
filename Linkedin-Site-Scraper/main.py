from linkedin_scraper import Person, actions
from selenium import webdriver
import os
from dotenv import load_dotenv
import json
driver = webdriver.Chrome()

load_dotenv()

email = os.getenv("LINKEDIN_EMAIL")
password = os.getenv("LINKEDIN_PASSWORD")
actions.login(driver, email, password)
persons_urls = [
    "https://www.linkedin.com/in/vishal-harihar/"
]

persons_data = []

for url in persons_urls:
    person = Person(url, driver=driver)
    persons_data.append({
    "name": person.get("name", "Unknown"),
    "job_title": person.get("job_title", "Unknown"),
    "company": person.get("company", "Unknown"),
    "education": person.get("education", "Unknown"),
    "location": person.get("location", "Unknown")
    })

with open('persons_data.json', 'w') as json_file:
    json.dump(persons_data, json_file, indent=4)
