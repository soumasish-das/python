from shutil import copyfile
import os
import random
import glob

mylist = [f for f in glob.glob("C:\\Users\\Vicky\\Desktop\\*.txt")]
print(mylist)
print("\n")

output_dir = "C:\\Users\\Vicky\\Minnie\\"

for file in mylist:
    #Generate name of copied file using random numbers
    #random.randint(100,1000) gives a number between 100 and 1000
    dest = output_dir + os.path.basename(file) + "_" + str(random.randint(100,1000))
    
    try:
        #Copy file
        copyfile(file, dest)
         
        #Perform operations on copied file
        test = open(dest, "r")
        data = test.readlines()
        print("---------------------------------------------------------\n"\
              + str(dest)\
              + ":\n---------------------------------------------------------\n"\
              + str(data))
        print()
        test.close()
    except Exception as e:
        print("Exception: " + str(e))
    finally:
        #Delete the copied file if it exists
        if os.path.isfile(dest):
            os.remove(dest)
