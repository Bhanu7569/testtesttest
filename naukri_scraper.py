from playwright.sync_api import sync_playwright
from pymongo import MongoClient
from datetime import datetime
import time


client = MongoClient("mongodb://localhost:27017/")
db = client["job_database"]
collection = db["hirist_jobs"]


def scrape_hirist(keyword, pages=2):
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        keyword_q = keyword.replace(" ", "%20")

        for pg in range(1, pages + 1):
            url = f"https://www.hirist.com/search/?q={keyword_q}&page={pg}"

            print("\nLoading:", url)
            page.goto(url)
            page.wait_for_timeout(4000)

            job_cards = page.locator("div.job-card")
            count = job_cards.count()

            print("Found job cards:", count)

            for i in range(count):
                card = job_cards.nth(i)

                title = card.locator("h3").inner_text() if card.locator("h3").count() > 0 else None
                company = card.locator("div.company-name").inner_text() if card.locator("div.company-name").count() > 0 else None
                exp = card.locator("span.experience").inner_text() if card.locator("span.experience").count() > 0 else None
                loc = card.locator("span.location").inner_text() if card.locator("span.location").count() > 0 else None
                link = card.locator("a").get_attribute("href") if card.locator("a").count() > 0 else None

                if title and link:
                    jobs.append({
                        "keyword": keyword,
                        "title": title,
                        "company": company,
                        "experience": exp,
                        "location": loc,
                        "job_url": "https://www.hirist.com" + link,
                        "source": "Hirist",
                        "scraped_at": datetime.utcnow()
                    })

        browser.close()

    return jobs


def save_to_mongo(jobs):
    for job in jobs:
        collection.update_one(
            {"job_url": job["job_url"]},
            {"$set": job},
            upsert=True
        )


if __name__ == "__main__":
    keywords = [
        "python developer",
        "java developer",
        "react developer",
        "full stack developer",
        "software engineer"
    ]

    for kw in keywords:
        print(f"\nScraping: {kw}")
        data = scrape_hirist(kw, pages=2)
        save_to_mongo(data)
        print(f"Saved {len(data)} jobs for {kw}")

    print("\nDone.")
    print("\nDone.")
    print("\nDone.")
    print("\nDone.")
    print("\nDone.")
