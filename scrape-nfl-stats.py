import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool
import time
import shutil

BASE_URL = 'https://www.pro-football-reference.com{0}'
PLAYER_LIST_URL = 'https://www.pro-football-reference.com/players/{0}'
PLAYER_PROFILE_URL = 'https://www.pro-football-reference.com/players/{0}/{1}'
PLAYER_GAMELOG_URL = 'https://www.pro-football-reference.com/players/{0}/{1}/gamelog/{2}'

HEADERS = {
    'user-agent': ('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36')
}

DATA_DIR = 'data'

class Scraper():
    """Scraper for pro-football-reference.com to collect NFL player stats"""

    def __init__(self, letters_to_scrape=['A'], num_jobs=1, clear_old_data=True):
        """Initialize the scraper to get player stats

                Args:
                    - letters_to_scrape (str[]): The site sorts players by the first letter of their
                      last name. This array tells the scraper which letters to scrape data for.
                    - num_jobs (int): Number of concurrent jobs the scraper should run. While Python
                      can't multi-thread, it can manage multiple processes at once, which allows it to
                      utilize time spent waiting for the server to respond.
                    - clear_old_data (boolean): Whether or not the data file should be wiped before
                      starting the scrape.

                Returns:
                    None
        """
        self.letters_to_scrape = [letter.upper() for letter in letters_to_scrape]
        self.num_jobs = num_jobs
        self.clear_old_data = clear_old_data
        self.session = requests.Session()
        self.start_time = time.time()
        self.cross_process_player_count = 0

        if num_jobs > 1:
            self.multiprocessing = True
            self.worker_pool = Pool(num_jobs)
        else:
            self.multiprocessing = False

    def scrape_site(self):
        """Pool workers to scrape players by first letter of last name"""
        if self.clear_old_data:
            self.clear_data_dir()
        for letter in self.letters_to_scrape:
            player_profile_links = self.get_players_for_letter(letter)
            print player_profile_links

    def get_players_for_letter(self, letter):
        """Get a list of player links for a letter of the alphabet.
            Site organizes players by first letter of last name.

            Args:
                - letter (str): letter of the alphabet uppercased

            Returns:
                - player_links (str[]): the URLs to get player profiles
        """
        response = self.get_page(PLAYER_LIST_URL.format(letter))
        soup = BeautifulSoup(response.content, 'html.parser')

        players = soup.find('div', {'id': 'div_players'}).find_all('a')
        return [BASE_URL.format(player['href']) for player in players]

    def get_page(self, url, retry_count=0):
        """Use requests to get a page; retry when failures occur

            Args:
                - url (str): The URL of the page to make a GET request to
                - retry_count (int): Number of times the URL has already been requests

            Returns:
                - response (obj): The Requests response object
        """
        try:
            return self.session.get(url, headers=HEADERS)
        except:
            retry_count += 1
            if retry_count <= 3:
                self.session = requests.Session()
                return self.get_page(url, retry_count)
            else:
                raise

    def clear_data_dir(self):
        """Clears the scraped data"""
        try:
            shutil.rmtree(DATA_DIR)
        except FileNotFoundError:
            pass


if __name__ == '__main__':
    letters_to_scrape = ['A']
    nfl_scraper = Scraper(letters_to_scrape=letters_to_scrape, num_jobs=1, clear_old_data=False)

    nfl_scraper.scrape_site()
