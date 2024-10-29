from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

#Path input and output file 
input_csv_filename = '/Users/saruttayayuenyong/Desktop/s.csv'
output_csv_filename = "spotify_track_DDMMYYYY.csv"

# Function to initialize a WebDriver
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=chrome_options)

# Function to process each URL link in csv file
def process_url(url, song_counter, driver_queue):
    driver = driver_queue.get()
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.encore-text-headline-large[data-encore-id='text']"))
        )
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # Extract the song name
        song_element = soup.select_one("h1.encore-text-headline-large[data-encore-id='text']")
        song_name = song_element.text.strip() if song_element else "0"

        # Extract the play count
        playcount_element = soup.select_one("span[data-testid='playcount']")
        play_count = playcount_element.text.strip() if playcount_element else "0"

        # Extract the artist name
        artist_element = soup.select_one("a[data-testid='creator-link']")
        artist_name = artist_element.text.strip() if artist_element else "0"

        print(f"Song {song_counter}: {song_name}, Artist: {artist_name}, Play count: {play_count}")
        return [song_name, artist_name, play_count]
    
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return ["0", "0", "0"]
    
    finally:
        driver_queue.put(driver)


start_time = time.time()
song_counter = 0

# Read URLs from the input CSV
with open(input_csv_filename, mode='r', newline='') as input_file:
    reader = csv.DictReader(input_file)
    urls = [row['track_href'] for row in reader]

# Open the output CSV file to save the results
with open(output_csv_filename, mode='a', newline='') as output_file:
    writer = csv.writer(output_file)

    # Write the header if the file is empty
    if output_file.tell() == 0:
        writer.writerow(["Song", "Artist", "Play Count"])

    # Create a queue of WebDriver instances
    driver_queue = Queue()
    for _ in range(5):  # Adjust the number of drivers as needed
        driver_queue.put(init_driver())

    # Use threads to speed up processing can modify thread in max_workers
    with ThreadPoolExecutor(max_workers=5) as executor:  
        futures = [executor.submit(process_url, url, i+1, driver_queue) for i, url in enumerate(urls)]
        
        for future in as_completed(futures):
            result = future.result()
            writer.writerow(result)

    # Clean up drivers
    while not driver_queue.empty():
        driver = driver_queue.get()
        driver.quit()

#Calculate time
end_time = time.time()
total_time = end_time - start_time

print(f"Data saved to {output_csv_filename}")
print(f"Total time taken: {total_time:.2f} seconds")
