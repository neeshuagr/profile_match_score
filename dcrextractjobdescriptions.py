import glob
import xml.etree.ElementTree as et
import dcrnlp

path = '/Temp/XML/XML/*'
files = glob.glob(path)

for file in files:
    print("%s" % file)
    tree = et.parse(file)
    root = tree.getroot()
    for child in root:
        job_descriptions = tree.getroot().iter('job_description')
        for jd in job_descriptions:
            print("%s" % dcrnlp.remove_whitespaces(jd.text))
            break
