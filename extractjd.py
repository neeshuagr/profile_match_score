import glob
import xml.etree.ElementTree as et
import dcrnlp
import ntpath
import sys
import datetime


path = '/media/0C5519460C551946/Temp/XML/XML/*'
files = glob.glob(path)
good_job_desc_file = open('/home/Data/nlpdata/cleanedjd.txt', 'w')
job_desc_phrases_file = open('/home/Data/nlpdata/allphrases.txt', 'w')
job_desc_original_file = open('/home/Data/nlpdata/originaljd.txt', 'w')
log_file = open('/home/Data/nlpdata/log.txt', 'w')

job_count = 1
file_count = 1

for file in files:
    print("%s:processing.. %5d %s" %
          (datetime.datetime.now(), file_count, ntpath.basename(file)))
    print("%s:processing.. %5d %s" %
          (datetime.datetime.now(),
           file_count, ntpath.basename(file)), file=log_file)

    tree = et.parse(file)
    file_count += 1

    root = tree.getroot()
    for child in root.iter('job_info'):
        jd = child.find('job_description')
        jobid = child.find('job_id')

        if (jd is None):
            print('?', end='')
            continue
        elif (jobid is None):
            print('^', end='')
            continue

        job_unique_id = '-' * 40 + jobid.text + '_' + str(job_count) + '-' * 40

        print('.', end='')
        sys.stdout.flush()

        print(job_unique_id, file=job_desc_original_file)
        print('%s' % jd.text, file=job_desc_original_file)

        print(job_unique_id, file=good_job_desc_file)
        print('%s' % dcrnlp.remove_whitespaces(jd.text),
              file=good_job_desc_file)

        print(job_unique_id, file=job_desc_phrases_file)

        for word in dcrnlp.extract_nounphrases(jd.text):
            print('%s' % word, end='|',  file=job_desc_phrases_file)

        print('\n', file=job_desc_phrases_file)
        job_count += 1
        # if (job_count > 15) :
        #    break

    # if (file_count > 5) :
    #    break

    # print should print the new line

    print('')

job_desc_phrases_file.close()
good_job_desc_file.close()
job_desc_original_file.close()
log_file.close()
