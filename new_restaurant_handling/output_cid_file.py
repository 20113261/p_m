import csv
import time

if __name__ == '__main__':
    f = open('jp_36.csv')
    f.readline()
    reader = csv.reader(f)
    res_f = open('cid_file_' + time.strftime('%y_%m_%d', time.localtime(time.time())), 'w')
    for line in reader:
        res_f.write('\t'.join(line) + "\n")
