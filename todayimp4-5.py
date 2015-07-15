################################################################################
##  Name: Suyog S Swami
##  ID: 1001119101
##  Course: CSE6331(Cloud Compluting) - Summer(2015)
##  Programming Assignment 4- Introduction to AWS
##  References: http://boto.readthedocs.org/en/latest/s3_tut.html (For inserting and creating bucket)
##              http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_MySQL.html (For MySQL-EC2 pytty connection)
##              http://stackoverflow.com/questions/868690/good-examples-of-python-memcache-memcached-being-used-in-python (Elastic cache implementation)
##              https://docs.python.org/2/library/csv.html (for reading csv)
##              http://catalog.data.gov/dataset/oregon-consumer-complaints-7f511 (csv data-60000 records)
################################################################################
import boto
import uuid
import boto.s3.connection
from boto.s3.key import Key
import csv
import time
import MySQLdb
import datetime
import random
import memcache

s3 = boto.connect_s3()
t1=time.time()
bucket_name = "suyogs-%s" % uuid.uuid4()
print "Creating new bucket with name: " + bucket_name
bucket = s3.create_bucket(bucket_name)
k=Key(bucket)
k.key='Consumer_Complaints.csv'
files=k.set_contents_from_filename('Consumer_Complaints.csv')
t2=time.time()
t3=t2-t1
print 'Time for inserting into the bucket'
print t3

t4=time.time()
db = MySQLdb.connect(host='aa1cljl2dz7ja45.cc1ekq787ymk.us-west-2.rds.amazonaws.com', port=3306, db='ebdb', user='suyogswami',passwd='suyogswami')
t4=time.time()

conn = memcache.Client(['suyog.kvtrnn.cfg.usw2.cache.amazonaws.com:11211'])

cursor=db.cursor()
cursor.execute('create table customer_complaints(Complaint_ID INT,Product VARCHAR(255),Sub_product VARCHAR(255),Issue VARCHAR(255),Sub_issue VARCHAR(255),State VARCHAR(255),ZIP_code INT,Submitted_via VARCHAR(255),Date_received VARCHAR(255),Date_sent_to_company VARCHAR(255),Company VARCHAR(255),Company_response VARCHAR(255),Timely_response VARCHAR(10),Customer_disputed VARCHAR(10))')


with open('Consumer_Complaints.csv', 'rb') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader)
    for row in spamreader:
        cursor.execute('INSERT INTO customer_complaints VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s,%s,%s, %s,%s, %s)', (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13]))
t5=time.time()
print 'Time for inserting data'
print t5-t4
db.commit()


def q1():
    cursor.execute('''select Complaint_ID, Product, Issue, Company ,Zip_code, State from customer_complaints where Zip_code between 90000 and 95000''')
    result1=cursor.fetchall()
    return result1

def q2():
    cursor.execute('''select count(*) , State, Date_received from customer_complaints where State="CA"''')
    result2=cursor.fetchall()
    return result2

def q3():
    cursor.execute('''select count(*),State from customer_complaints where Customer_disputed="Yes"''')
    result3=cursor.fetchall()
    return result3

def q4():
    cursor.execute('''select Complaint_ID, Product, Issue, Company from customer_complaints where Product="Bank account or service" and Company="Navy FCU"''')
    result4=cursor.fetchall()
    return result4


di={'a':q1,'b':q2,'c':q3,'d':q4}

print 'First 1000 Queries without using Memcached'
tim1=time.time()
for k in range(1000):
    p=random.choice(di.items())
    a=p[1]()
tim2=time.time()
print tim2-tim1

print 'First 5000 Queries without using Memcached'
tim3=time.time()
for l in range(5000):
    p=random.choice(di.items())
    a=p[1]()
tim4=time.time()
print tim4-tim3

print 'First 20000 Queries without using Memcached '
tim5=time.time()
for m in range(20000):
    p=random.choice(di.items())
    a=p[1]()
tim6=time.time()
print tim6-tim5

print 'First 1000 Queries using Memcached'
tim7=time.time()
for k in range(1000):
    p=random.choice(di.items())
    memc=conn.get(p[0])
    if not memc:
        a=p[1]()
        conn.set(p[0],a)
tim8=time.time()
print tim8-tim7

print 'First 5000 Queries using Memcached'
tim9=time.time()
for l in range(5000):
    p=random.choice(di.items())
    memc=conn.get(p[0])
    if not memc:
        a=p[1]()
        conn.set(p[0],a)
tim10=time.time()
print tim10-tim9

print 'First 20000 Queries using Memcached '
tim11=time.time()
for m in range(20000):
    p=random.choice(di.items())
    memc=conn.get(p[0])
    if not memc:
        a=p[1]()
        conn.set(p[0],a)
tim12=time.time()
print tim12-tim11


def q5():
    cursor.execute("select * from customer_complaints where Product='Bank account or service' limit 200")
    result1=cursor.fetchall()
    return result1

def q6():
    cursor.execute("select * from customer_complaints where ZIP_code between 90000 and 100000 limit 400")
    result2=cursor.fetchall()
    return result2

def q7():
    cursor.execute("select * from customer_complaints where Submitted_via='Web' limit 600")
    result3=cursor.fetchall()
    return result3

def q8():
    cursor.execute("select * from customer_complaints where Company_response='In Progress' limit 800")
    result4=cursor.fetchall()
    return result4


dim={'a': q5,'b': q6,'c': q7,'d': q8}


print 'First 1000 Queries (200-800 records) without using Memcached'
tim13=time.time()
for k in range(1000):
    p=random.choice(dim.items())
    a=p[1]()
tim14=time.time()
print tim14-tim13

print 'First 5000 Queries (200-800 records) without using Memcached'
tim15=time.time()
for l in range(5000):
    p=random.choice(dim.items())
    a=p[1]()
tim16=time.time()
print tim16-tim15

print 'First 20000 Queries (200-800 records) without using Memcached '
tim17=time.time()
for m in range(20000):
    p=random.choice(dim.items())
    a=p[1]()
tim18=time.time()
print tim18-tim17

print 'First 1000 Queries (200-800 records) using Memcached'
tim19=time.time()
for k in range(1000):
    p=random.choice(dim.items())
    memc=conn.get(p[0])
    if not memc:
        a=p[1]()
        conn.set(p[0],a)
tim20=time.time()
print tim20-tim19

print 'First 5000 Queries (200-800 records) using Memcached'
tim21=time.time()
for l in range(5000):
    p=random.choice(dim.items())
    memc=conn.get(p[0])
    if not memc:
        a=p[1]()
        conn.set(p[0],a)
tim22=time.time()
print tim22-tim21

print 'First 20000 Queries (200-800 records) using Memcached'
tim23=time.time()
for m in range(20000):
    p=random.choice(dim.items())
    memc=conn.get(p[0])
    if not memc:
        a=p[1]()
        conn.set(p[0],a)
tim24=time.time()
print tim24-tim23

db.close()
