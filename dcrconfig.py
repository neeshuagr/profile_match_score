import configparser


class ConfigManager:
    #  Config Manager reads from application.config file. It exposes accessors
    #  that can be used the application to get config values

    def __init__(self, configfilename='/mnt/nlpdata/application.ini'):
        config = configparser.ConfigParser()
        config.read(configfilename)
        self._allphrasefile = str(config['File Path']['phrase file'])
        self._distinctphrasefile = str(config['File Path']
                                       ['distinct phrase file'])
        self._ngramfilteredphrasefile = str(config['File Path']
                                            ['filtered phrase file'])
        self._semanticgraphlogfile = str(config['File Path']
                                         ['semantic graph log file'])
        self._semanticgraphfile = str(config['File Path']
                                      ['semantic graph all nodes'])
        self._documentsgraphfile = str(config['File Path']['graph documents'])
        self._integernodesfile = str(config['File Path']['integer nodes'])
        self._integeredgesfile = str(config['File Path']['integer edges'])
        self._documentsedgesintegersfile = str(config['File Path']
                                               ['integer document edges'])
        self._nodefile = str(config['File Path']['node dictionary'])
        self._graphedgeweight = int(config['Graph Properties']['edge weight'])
        self._filtergraphedgeweight = int(config['Graph Properties']
                                          ['edge weight filter final graph'])

        self._jobdescriptionxmlpath = str(config['File Path']
                                          ['web crawled xml path'])
        self._xmlcleanuplog = str(config['File Path']
                                  ['xml cleanup log'])
        self._diminitionpercentage = int(config['Graph Properties']
                                         ['neighbor_edge_diminition_percent'])
        self._datadb = str(config['Database']['data db connection'])
        self._smarttrackdirectory = str(config['File Path']
                                        ['smart track requirements doc path'])

        self._stgraphedgeweight = int(config['ST Graph Properties']['st edge weight'])
        self._stfiltergraphedgeweight = int(config['ST Graph Properties']
                                          ['st edge weight filter final graph'])
        self._stdiminitionpercentage = int(config['ST Graph Properties']
                                         ['st neighbor_edge_diminition_percent'])

    @property
    def PhraseFile(self):
        return self._allphrasefile

    @property
    def DistinctPhraseFile(self):
        return self._distinctphrasefile

    @property
    def NGramFilteredPhraseFile(self):
        return self._ngramfilteredphrasefile

    @property
    def SemanticGraphLogFile(self):
        return self._semanticgraphlogfile

    @property
    def SemanticGraphFile(self):
        return self._semanticgraphfile

    @property
    def DocumentsGraphFile(self):
        return self._documentsgraphfile

    @property
    def IntegerNodesFile(self):
        return self._integernodesfile

    @property
    def IntegerEdegesFile(self):
        return self._integeredgesfile

    @property
    def DocumentsEdgesIntegerFile(self):
        return self._documentsedgesintegersfile

    @property
    def NodeFile(self):
        return self._nodefile

    @property
    def GraphEdgeWeight(self):
        return self._graphedgeweight

    @property
    def FilterGraphEdgeWeight(self):
        return self._filtergraphedgeweight

    @property
    def JobDescriptionXMLPath(self):
        return self._jobdescriptionxmlpath

    @property
    def XMLCleanupLogFile(self):
        return self._xmlcleanuplog

    @property
    def DiminitionPercentage(self):
        #  Each far neighbor will be reduced by this percentage from its
        #  previous neighbor'''
        return self._diminitionpercentage

    # Data db connection
    @property
    def Datadb(self):
        return self._datadb

    @property
    def SmartTrackDirectory(self):
        return self._smarttrackdirectory

    @property
    def STGraphEdgeWeight(self):
        return self._stgraphedgeweight

    @property
    def STFilterGraphEdgeWeight(self):
        return self._stfiltergraphedgeweight

    @property
    def STDiminitionPercentage(self):
        #  Each far neighbor will be reduced by this percentage from its
        #  previous neighbor'''
        return self._stdiminitionpercentage
