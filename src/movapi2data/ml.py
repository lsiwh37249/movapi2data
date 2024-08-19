import requests
import os

API_KEY = os.getenv('MOVIE_API_KEY')

def data2json(year=2015):
    url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}"
    data = requests.get(url)
    print(data)
    return data
