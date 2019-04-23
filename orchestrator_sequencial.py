'''
Created on 19 abr. 2019

@author: Aleix Sancho i Eric Pi
'''
import sys
import re
import string
import time

def count_words(dicc):
    num_words = 0
    words = dicc.keys()
    
    for word in words:
        num_words = num_words + dicc[word]
        
    return {"Number of words": num_words}

def reducer(dicc):
    dictionary = {}
    for word in dicc:
        if word in dictionary:
            dictionary[word] = dictionary.get(word) + words.get(word)
        else:
            dictionary[word] = dicc.get(word)

    return dictionary

def word_count(dicc):
    words = ""
    punctuation2 = "['-]"
    regex2 = r"(\s)"
    
    for value in dicc:
        if re.search(regex2, value):
            value = ' '
            words = words + value
        else:
            if not re.search(punctuation2, value) and re.search('['+string.punctuation+']', value):
                value = ' '
            words = words + value
            
    words = words.split(' ')
    
    dictionary = {}
    for word in words:
        if not word == "":
            if word in dictionary:
                dictionary[word] = dictionary.get(word) + 1
            else:
                dictionary[word] = 1
                
    return dictionary

if __name__ == '__main__':
    if len(sys.argv) == 2:
        file = open(sys.argv[1], 'r')
        initial_time = time.time()
        dictionary = word_count(file.read())
        word_number = count_words(dictionary)
        final_dictionary = reducer(dictionary)
        final_time = time.time()
        total_time = final_time - initial_time
        print(total_time)
        f1 = open('resultat_count_words_sequencial.txt', 'w+')
        f1.write(str(word_number))
        f2 = open('resultat_reducer_sequencial.txt', 'w+')
        f2.write(str(final_dictionary))
        f1.close()
        f2.close()
    else:
        print("Necessites introduir dos parametres")