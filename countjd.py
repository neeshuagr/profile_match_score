#!/usr/bin/python3.4
import glob
import xml.etree.ElementTree as et
import dcrnlp
import ntpath
import sys
import datetime
import dcrconfig
# from threading import Thread
# from queue import Queue

files = glob.glob(dcrconfig.ConfigManager().JobDescriptionXMLPath)
# good_job_desc_file = open('/home/neeshu/Data/nlpdata/cleanedjd.txt', 'w')
job_desc_phrases_file = open(dcrconfig.ConfigManager().PhraseFile, 'w')
# job_desc_original_file = open('/home/neeshu/Data/nlpdata/original.txt', 'w')
log_file = open(dcrconfig.ConfigManager().XMLCleanupLogFile, 'w')

allphrases = ''
# q = Queue()

'''
def nounphrase_extractor():
    global allphrases
    while True:
        worker = q.get()
        jobid = worker[0]
        jobdescription = worker[1]

        allphrases += '\n' + jobid + '\n'
        allphrases += dcrnlp.extract_nounphrases_sentences(jobdescription)

        print('.', end='')
        q.task_done()

for i in range(20):
    t = Thread(target=nounphrase_extractor)
    t.daemon = True
    t.start()
'''

job_count = 1
file_count = 1

for file in files:
    try:
        print("%s:processing.. %5d %s" %
              (datetime.datetime.now(), file_count, ntpath.basename(file)))
        print("%s:processing.. %5d %s" %
              (datetime.datetime.now(),
               file_count, ntpath.basename(file)), file=log_file)

        tree = et.parse(file)
        file_count += 1

        allphrases = ''
        root = tree.getroot()
        for child in root.iter('job_info'):
            try:
                jd = child.find('job_description')
                jobid = child.find('job_id')

                if (jd is None):
                    print('?', end='')
                    continue
                elif (jobid is None):
                    print('^', end='')
                    continue

                job_unique_id = '-' * 40 + jobid.text + '_' + str(job_count)
                job_unique_id += '-' * 40

                print('.', end='')
                sys.stdout.flush()

                '''
                print(job_unique_id, file=job_desc_original_file)
                print('%s' % jd.text, file=job_desc_original_file)

                print(job_unique_id, file=good_job_desc_file)
                print('%s' % dcrnlp.remove_whitespaces(jd.text),gi
                    file=good_job_desc_file)

                '''
                # allphrases += '\n' + job_unique_id + '\n'
                # allphrases += dcrnlp.extract_nounphrases_sentences(jd.text)

                # item = ['\n' + job_unique_id + '\n', jd.text]
                # q.put(item)
                # print(job_unique_id, file=job_desc_phrases_file)

                # for word in dcrnlp.extract_nounphrases(jd.text):
                #    allphrases += '%s|' % word

                job_count += 1
                # if (job_count > 15) :
                #    break
            except:
                print("Ignoring the jd  error and continuing")

        # q.join()
        sys.stdout.flush()
        # print('')
        # print(allphrases, file=job_desc_phrases_file)

        # if(file_count > 1):
        #     break

        # print should print the new line
    except:
        print('Ignoring the file error')

# print(allphrases, file=job_desc_phrases_file)

# job_desc_phrases_file.close()
# good_job_desc_file.close()
# job_desc_original_file.close()
# log_file.close()
print("%s" % datetime.datetime.now())
print("job count:%d file_count:%d" % (job_count, file_count))
