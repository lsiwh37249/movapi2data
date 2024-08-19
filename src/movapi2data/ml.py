import requests
import os
import json
from tqdm import tqdm

API_KEY = os.getenv('MOVIE_API_KEY')


def save_j(file_path, data):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf8' ) as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def data2json(year=2015):
    file_path = f'/home/kim1/data/movies/year={year}/data.json'
    base_url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}&openStartDt={year}&openEndDt={year}"
    all_data = [ ]
    for page in tqdm(range(1,11)):
        data = requests.get(f"{base_url}&page={page}")
        j = data.json()
        all_data.append(j)
        print(all_data)
        save_j(file_path, all_data)

    return data, file_path

