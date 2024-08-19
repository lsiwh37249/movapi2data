import requests
import os
import json

API_KEY = os.getenv('MOVIE_API_KEY')


def data2json(year=2015):
    file_path = f'/home/kim1/data/movies/year={year}/data.json'
    base_url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}&openStartDt={year}&openEndDt={year}"
    data = requests.get(base_url)
    j = data.json()
    #print(data)
    print(j)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf8' ) as f:
        json.dump(j, f, indent=4, ensure_ascii=False)


    return data, file_path

