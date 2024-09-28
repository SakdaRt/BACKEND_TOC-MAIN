import os
import random
import cloudscraper
import re
import json
import csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
from movie import Movie
import time

main_start_time = time.time()

urls = {
    'top_250': 'https://www.imdb.com/chart/top'
    # ,'moviemeter': 'https://www.imdb.com/chart/moviemeter/'
}

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',

    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',

    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.1 Safari/605.1.15',
    
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.100.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36 Edg/93.0.961.47',

    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36 OPR/78.0.4093.112',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36 OPR/75.0.3969.218',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36 OPR/73.0.3856.344',

    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:60.0) Gecko/20100101 Firefox/60.0',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
    'Mozilla/5.0 (Linux; Android 11; SM-G950F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 9; SM-G960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36',
]


referers = [
    'https://www.google.com/',
    'https://www.bing.com/',
    'https://duckduckgo.com/',
    'https://search.yahoo.com/',
    'https://www.facebook.com/',
    'https://www.x.com/',
    'https://www.linkedin.com/',
    'https://www.instagram.com/',
    'https://www.reddit.com/',
    'https://www.quora.com/',
    
    'https://www.wikipedia.org/',
    'https://www.youtube.com/',
    'https://www.pinterest.com/',
    'https://www.tumblr.com/',
    'https://www.snapchat.com/',
    'https://www.telegram.org/',
    'https://www.whatsapp.com/',
    'https://www.netflix.com/',
    'https://www.amazon.com/',
    'https://www.ebay.com/'
]

accept_languages = [
    'th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7',
    'en-US,en;q=0.9',
]

cache_controls = [
    'no-cache', 
    'max-age=0', 
    'no-store'
]

headers = {
    'User-Agent': random.choice(user_agents),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': random.choice(accept_languages),
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': random.choice(referers),
    'DNT': '1',
    'TE': 'Trailers',
    'Pragma': 'no-cache',
    'Cache-Control': random.choice(cache_controls),
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'X-Requested-With': 'XMLHttpRequest'
}

scraper = cloudscraper.create_scraper()

def fetch_movie_details(movie, url):
    title = re.sub(r"&(?:apos|quot|amp|lt|gt);", "'", movie.get('name', '')).replace("Ã©", "é").replace("Ã¬", "ì")
    title = re.sub(r'\s+', ' ', title).strip()  # Remove extra spaces

    rating = movie.get('aggregateRating', {}).get('ratingValue')
    if rating and not re.match(r'^\d+(\.\d+)?$', str(rating)):
        rating = 'N/A'
    
    description = re.sub(r'<[^>]*>', '', movie.get('description', 'N/A'))  # Remove any HTML tags
    description = re.sub(r'\s+', ' ', description).strip()  # Remove extra spaces

    duration = 'N/A'
    if movie.get('duration'):
        duration_match = re.search(r'PT(\d+H)?(\d+M)?', movie['duration'])
        if duration_match:
            hours = duration_match.group(1) or ''
            minutes = duration_match.group(2) or ''
            duration = f"{hours.replace('H', ' hours ') if hours else ''}{minutes.replace('M', ' minutes') if minutes else ''}".strip()

    movie_url = urljoin(url, movie['url'])
    movie_response = scraper.get(movie_url, headers=headers)
    movie_response.raise_for_status()

    movie_soup = BeautifulSoup(movie_response.text, 'lxml')
    movie_script_tag = movie_soup.find('script', type='application/ld+json')
    if movie_script_tag:
        movie_json_ld = json.loads(movie_script_tag.string.strip())
        director = ', '.join([person['name'] for person in movie_json_ld.get('director', [])])
        genres = ', '.join(movie_json_ld.get('genre', []))
    else:
        director_tag = movie_soup.find('a', href=re.compile(r'/name/nm\d+/'))
        director = director_tag.text.strip() if director_tag else 'N/A'

        genre_tags = movie_soup.select('div[data-testid="genres"] a')
        genres = ', '.join([genre.text.strip() for genre in genre_tags]) if genre_tags else 'N/A'

    return Movie(
        title=title,
        url_picture=movie.get('image'),
        score=rating,
        duration=duration,
        description=description,
        director=director,
        genre=genres
    )


def fetch_movies(url):
    response = scraper.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'lxml')
    script_tag = soup.find('script', type='application/ld+json')
    
    movies_list = []

    if script_tag:
        json_ld_content = script_tag.string.strip()

        try:
            data = json.loads(json_ld_content)

            with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(lambda movie: fetch_movie_details(movie, url), [item['item'] for item in data['itemListElement']])
                for result in results:
                    movies_list.append(result)

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
    else:
        print("JSON-LD script tag not found.")
    
    return movies_list

def save_to_json(filename, movies):
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump([movie.__dict__ for movie in movies], jsonfile, ensure_ascii=False, indent=4)
    print(f"Data successfully saved to {filename}.")

def save_to_csv(filename, movies):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Title', 'Duration', 'Rating', 'Description', 'Image URL', 'Director', 'Genres'])
        for movie in movies:
            csv_writer.writerow([
                movie.get_title(), 
                movie.get_duration(), 
                movie.get_score(), 
                movie.get_description(), 
                movie.get_url_picture(), 
                movie.get_director(), 
                movie.get_genre()
            ])
    print(f"Data successfully saved to {filename}.")

def save_csv_from_json(json_filename, csv_filename):
    with open(json_filename, 'r', encoding='utf-8') as jsonfile:
        movies = json.load(jsonfile)
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Title', 'Duration', 'Rating', 'Description', 'Image URL', 'Director', 'Genres'])
        
        for movie in movies:
            csv_writer.writerow([
                movie.get('_Movie__title'),
                movie.get('_Movie__duration'),
                movie.get('_Movie__score'),
                movie.get('_Movie__description'),
                movie.get('_Movie__url_picture'),
                movie.get('_Movie__director'),
                movie.get('_Movie__genre')
            ])
    print(f"Data successfully saved to {csv_filename} from {json_filename}.")

def update_movie_data(json_filename, new_movies):
    if os.path.exists(json_filename):
        with open(json_filename, 'r', encoding='utf-8') as jsonfile:
            try:
                old_movies = json.load(jsonfile)
            except json.JSONDecodeError:
                print("Error: Cannot decode JSON file. Starting with an empty list.")
                old_movies = []
    else:
        old_movies = []

    old_movies_dict = {movie['_Movie__title']: movie for movie in old_movies}
    
    updated_movies = []
    for new_movie in new_movies:
        old_movie = old_movies_dict.get(new_movie.get_title())
        
        if old_movie:
            changes = False
            if new_movie.get_score() != old_movie.get('_Movie__score'):
                changes = True
                print(f"Score changed for {new_movie.get_title()}: {old_movie.get('_Movie__score')} -> {new_movie.get_score()}")
            if new_movie.get_duration() != old_movie.get('_Movie__duration'):
                changes = True
                print(f"Duration changed for {new_movie.get_title()}: {old_movie.get('_Movie__duration')} -> {new_movie.get_duration()}")
            if new_movie.get_description() != old_movie.get('_Movie__description'):
                changes = True
                print(f"Description changed for {new_movie.get_title()}")

            if changes:
                updated_movies.append(new_movie.__dict__)
            else:
                updated_movies.append(old_movie)
        else:
            updated_movies.append(new_movie.__dict__)
            print(f"New movie added: {new_movie.get_title()}")

    with open(json_filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(updated_movies, jsonfile, ensure_ascii=False, indent=4)
    print(f"Data successfully updated in {json_filename}.")



def scrap():
    all_movies = []
    with Pool(processes=cpu_count()) as pool:
        result_lists = pool.map(fetch_movies, urls.values())

    for result in result_lists:
        all_movies.extend(result)

    json_filename = 'Top250andBoxoffice.json'
    csv_filename = 'Top250andBoxoffice.csv'

    save_to_json(json_filename, all_movies)
    save_to_csv(csv_filename, all_movies)

    print("All time: --- %s seconds ---" % (time.time() - main_start_time))
    
if __name__ == '__main__':
    pass
