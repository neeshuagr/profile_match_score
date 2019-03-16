import zipfile
import xml.parsers.expat

# get content xml data from OpenDocument file
ziparchive = zipfile.ZipFile("/home/lshp/development/dump/resumes-test/_1_stautomationscriptprocess.odt", "r")
xmldata = ziparchive.read("content.xml")
ziparchive.close()


class Element(list):
    def __init__(self, name, attrs):
        self.name = name
        self.attrs = attrs


class TreeBuilder:
    def __init__(self):
        self.root = Element("root", None)
        self.path = [self.root]

    def start_element(self, name, attrs):
        element = Element(name, attrs)
        self.path[-1].append(element)
        self.path.append(element)

    def end_element(self, name):
        assert name == self.path[-1].name
        self.path.pop()

    def char_data(self, data):
        self.path[-1].append(data)

# create parser and parsehandler
parser = xml.parsers.expat.ParserCreate()
treebuilder = TreeBuilder()
# assign the handler functions
parser.StartElementHandler = treebuilder.start_element
parser.EndElementHandler = treebuilder.end_element
parser.CharacterDataHandler = treebuilder.char_data

# parse the data
parser.Parse(xmldata, True)


def showtree(node, prefix=""):
    dataText = ''
    for e in node:
        if isinstance(e, Element):
            showtree(e, prefix + "  ")
        else:
            dataText += e
    print(dataText)


if __name__ == "__main__":
    showtree(treebuilder.root)
