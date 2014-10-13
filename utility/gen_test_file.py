__author__ = 'kevin'
import os

def get_filename_id():
    f=open('/home/kevin/Desktop/id_name')
    for line in f:
        filename,id = line.split(',')
        yield filename, str(id).strip()


def get_test_files():
    testfiles = os.listdir('/home/kevin/Documents/testfiles')
    return testfiles

def rename():
    testfiles = get_test_files()
    try:
        os.mkdir('/home/kevin/Documents/id_testfiles')
    except OSError as e:
        print "mkdir error"
        print e

    for filename ,id in get_filename_id():
        for f in testfiles:
            if f == filename:
                src ='/home/kevin/Documents/testfiles/'+f
                dst = '{}{}-{}'.format('/home/kevin/Documents/id_testfiles/', id ,f)
                print src
                print dst
                os.link( src, dst)
rename()
