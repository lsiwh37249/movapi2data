from  movapi2data.ml import data2json
import os
def test_data2json():
    data,file_path = data2json() 
    #print(data)
    assert data
    assert os.path.exists(file_path)
    
