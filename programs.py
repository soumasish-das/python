# Count of words in paragraph
import re

paragraph = "They had always called it the green river. " \
            "It made sense. The river was green. The river likely had a " \
            "different official name, but to everyone in town, it was and " \
            "had always been the green river. So it was with great surprise " \
            "that on this day the green river was a fluorescent pink."

# Use any one from below for the split operation

# Get words from paragraph by splitting on non-alphanumeric characters
# like space, comma, semicolon etc.
words = re.split("[^0-9a-zA-Z]+", paragraph)

# Below one is also acceptable (split by space only)
words = paragraph.split()

dict = {}
for item in words:
    if item in dict:
        dict[item] = dict[item] + 1
    else:
        dict[item] = 1

for entry in dict:
    if dict[entry] > 2:
        print(entry + " -> " + str(dict[entry]))


        
        
# Vowel swap
string = "hellotia"

list = []
string_result = ""
vowels = ["a", "e", "i", "o", "u"]

for char in string:
    if char in vowels:
        list.append(char)

for i in range(0, len(list), 2):
    if (i+1) < len(list):
        temp = list[i]
        list[i] = list[i+1]
        list[i+1] = temp

j = 0
for char in string:
    if char in vowels:
        string_result += list[j]
        j += 1
    else:
        string_result += char

print(string_result)




# Search for file and read contents (Normal)
import os
from datetime import datetime

date = datetime.today().strftime('%Y%m%d')
num_of_files =  5

for i in range(1, num_of_files+1):
    filename = "file_" + date + "_" + str(i) + ".txt"
    if os.path.isfile(filename):
        content = open(filename, 'r').readlines()
        for line in content:
            if "Value:" in line:
                print(filename + " -> " + line)
                break
    else:
        print(filename + " is not available")




# Search for file and read contents (Regular Expression)
import re
import os
from datetime import datetime

date = datetime.today().strftime('%Y%m%d')
num_of_files =  5

for i in range(1, num_of_files+1):
    filename = "file_" + date + "_" + str(i) + ".txt"
    if os.path.isfile(filename):
        content = open(filename, 'r').read()
        match = re.search("Value:.*", content)
        if match:
            print(filename + " -> " + match.group())
    else:
        print(filename + " is not available")
