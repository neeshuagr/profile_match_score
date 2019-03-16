#!/usr/bin/python3.4
import glob
import xml.etree.ElementTree as et
import dcrnlp
import ntpath
import sys
import datetime
import dcrconfig

files = glob.glob(dcrconfig.ConfigManager().JobDescriptionXMLPath)
job_desc_phrases_file = open(dcrconfig.ConfigManager().PhraseFile, 'w')
log_file = open(dcrconfig.ConfigManager().XMLCleanupLogFile, 'w')

allphrases = ''

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

                # Check for job description validity
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

                allphrases += '\n' + job_unique_id + '\n'
                allphrases += dcrnlp.extract_nounphrases_sentences(jd.text)

                job_count += 1

            except:
                print("Ignoring the jd  error and continuing")

        sys.stdout.flush()
        print('')
        print(allphrases, file=job_desc_phrases_file)

    except:
        print('Ignoring the file error')

job_desc_phrases_file.close()
log_file.close()
print("%s" % datetime.datetime.now())
