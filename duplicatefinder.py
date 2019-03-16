#!/usr/bin/python3.4
# import glob
# import xml.etree.ElementTree as et
# import ntpath
# import datetime
# import operator
import dcrconfig
import config
from datetime import datetime
import utility


def match_percent(job1, job2):
    size1 = len(job1)
    size2 = len(job2)
    intersection = list(val for val in job1 if val in job2)
    if size1 > size2:
        return (len(intersection) / size1)
    else:
        return (len(intersection) / size2)

utility.write_to_file(dcrconfig.ConfigManager().SemanticGraphLogFile, 'a',
        'Semantic graph Generation Step 6..! (duplicatefinder.py) ' + str(datetime.now()))

start = datetime.now()

distinct_file = open(dcrconfig.ConfigManager().DistinctPhraseFile, 'w')
log_file = open(config.ConfigManager().DistinctJdLogFile, 'w')
phrase_file = open(dcrconfig.ConfigManager().NGramFilteredPhraseFile, 'r')

job_count = 1
file_count = 1
jobid = ''
joblist = []
jdcount = 1

# Seperate files and store it in a list
for line in phrase_file:
    line = line.strip()
    if not (line.startswith('--') or len(line.strip()) < 1):
        jdcount += 1
        jd = line.split('|')
        joblist.append((jobid, jd, len(jd)))
    elif (line.startswith('--')):
        jobid = line.strip('-')

distjoblist_orig = sorted(joblist, key=lambda tup: tup[2])
# distjoblist.sort(key=lambda tup: tup[2])
original_count = len(distjoblist_orig)
print('\n Count before duplicate removal %d' % original_count)
dupcount = 0
jc = 0
djc = 0

#   Select 1000 at a time and process
for i in range(0, original_count, 1000):
    distjoblist = distjoblist_orig[i:i + 1000]
    jc = 0

    while True:
        job = distjoblist[jc]

        # job is sorted tuple with element job[2] is the size of job desc
        size = job[2]

        #  select only job descriptions phrase counte +-15. For lower
        #  phrase counts use the percentage.
        lo = size * .80
        if lo > 80:
            lo = size - 15

        up = size * 1.20
        if up > 120:
            up = size + 15

        #  Select only jobs between the ranges lo and up
        dupjoblist = (x for x in distjoblist
                      if x[2] > lo and x[2] < up and x[0] != job[0])

        print('Processing for : %s lo: %d up: %d jc: %d djc: %d'
              % (job[0], lo, up, jc, djc))
        print('Processing for : %s lo: %d up: %d jc: %d djc: %d'
              % (job[0], lo, up, jc, djc), file=log_file)

        #  if duplicate found remove it from distjoblist
        for dj in dupjoblist:
            matchper = match_percent(job[1], dj[1])
            if matchper > 0.80:
                print('Duplicate found removing %s  match per: %f' %
                      (dj[0], matchper))
                print(job[0] + '---' + dj[0] + ' ' +
                      str(matchper), file=log_file)
                distjoblist.remove(dj)
                dupcount += 1
        print('')
        jc += 1
        if jc >= len(distjoblist):
            break

    djc += len(distjoblist)
    for jd in distjoblist:
        print('---%s---' % jd[0], file=distinct_file)
        print('|'.join(jd[1]), file=distinct_file)
        print('Distinct job in written set: %d and i : %d ' % (djc, i))
        print('Distinct job in written set: %d and i : %d ' %
              (djc, i), file=log_file)

print('Duplicate count : %d' % dupcount)
print('Duplicate count : %d' % dupcount, file=log_file)
distinct_file.close()
log_file.close()

end = datetime.now()
print('Time ellapsed : %d seconds' % (end - start).seconds)
