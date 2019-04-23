'''
Created on 19 abr. 2019

@author: Aleix Sancho i Eric Pi
'''
import yaml
import sys
import re
import time
from COS_backend import COS_backend
from ibm_cf_connector import CloudFunctions

if __name__ == '__main__':
    if len(sys.argv) == 3:
        with open('ibm_cloud_config.txt', 'r') as config_file:
            res = yaml.safe_load(config_file)
    
        config_file = res['ibm_cos']
        config_file_cloud = res['ibm_cf']
        invocador = CloudFunctions(config_file_cloud)
        cos_backend = COS_backend(config_file)
        
        ll = cos_backend.list_objects("sanchoericbucket", "count_words")
        
        for elem in ll:
            file = elem["Key"]
            cos_backend.delete_object("sanchoericbucket", file)
            
        ll = cos_backend.list_objects("sanchoericbucket", "reducer")
        
        for elem in ll:
            file = elem["Key"]
            cos_backend.delete_object("sanchoericbucket", file)
        
        ll = cos_backend.list_objects("sanchoericbucket", "word_count_")
        
        for elem in ll:
            file = elem["Key"]
            cos_backend.delete_object("sanchoericbucket", file)
        
        with open('count_words.zip', 'rb') as count_words:
            codi = count_words.read()
            
        invocador.create_action('count_words', codi)
        
        with open('reducer.zip', 'rb') as reducer:
            codi = reducer.read()
            
        invocador.create_action('reducer', codi)
       
        with open('word_count.zip', 'rb') as word_count:
            codi = word_count.read()
            
        invocador.create_action('word_count', codi)        
        num_workers = int(sys.argv[2])
        index = 0
        tamany_totalB = cos_backend.head_object('sanchoericbucket', sys.argv[1])
        mida = tamany_totalB['content-length']
        tamany_particionat = int(mida) / num_workers
        inici_bytes = int(tamany_particionat * index)
        regex = r"(\s)"
        initial_time = time.time()
        
        while index < num_workers:
            ko = True
            offset = 1
            final_bytes = int(tamany_particionat * (index + 1))
            if index != (num_workers - 1):
                while ko:
                    text = cos_backend.get_object('sanchoericbucket', sys.argv[1], extra_get_args = {'Range': 'bytes=' + str(final_bytes) + '-' + str(final_bytes)})
                    if not re.search(regex, str(text)):
                        final_bytes = final_bytes + 1
                        offset = offset + 1
                    else:
                        ko = False
            else:
                final_bytes = int(mida)
            
            text = cos_backend.get_object('sanchoericbucket', sys.argv[1], extra_get_args = {'Range': 'bytes=' + str(inici_bytes) + '-' + str(final_bytes)})
            decoded_text = text.decode("latin1")
            params = {'config_file':config_file, 'text':decoded_text, 'index':index}
            invocador.invoke('word_count', params)
            index = index + 1
            inici_bytes = int((tamany_particionat * index) + offset)
        
        tots_acabats = False
        
        while not tots_acabats:
            llista_objectes = cos_backend.list_objects('sanchoericbucket', 'word_count')
            if len(llista_objectes) == num_workers:
                tots_acabats = True
    
        params = {'config_file':config_file}
        invocador.invoke('reducer', params)
        tots_acabats = False
        
        while not tots_acabats:
            llista_objectes = cos_backend.list_objects('sanchoericbucket', 'reducer')
            if len(llista_objectes) == 1:
                tots_acabats = True
        
        invocador.invoke('count_words', params)
        tots_acabats = False
        
        while not tots_acabats:
            llista_objectes = cos_backend.list_objects('sanchoericbucket', 'count_word')
            if len(llista_objectes) == 1:
                tots_acabats = True
                
        final_time = time.time()
        total_time = final_time - initial_time
        print(total_time)
        objecte_count_words = cos_backend.get_object('sanchoericbucket', 'count_words.txt')
        objecte_decoded_count_words = objecte_count_words.decode('utf-8')
        objecte_reducer = cos_backend.get_object('sanchoericbucket', 'reducer.txt')
        objecte_decoded_reducer = objecte_reducer.decode('utf-8')
        f1 = open('resultat_count_words.txt', 'w+')
        f1.write(objecte_decoded_count_words)
        f2 = open('resultat_reducer.txt', 'w+')
        f2.write(objecte_decoded_reducer)
        f1.close()
        f2.close()
    else:
        print("Necessites introduir tres parametres")