import configparser


class ConfigManager:
    ''' Config Manager reads from application.config ini. It exposes accessors
    that can be used the application to get config values '''

    def __init__(self, configfilename='/mnt/nlpdata/application.ini'):
        config = configparser.ConfigParser()
        config.read(configfilename)
        self._textfile = str(config['File Path']['text file'])
        self._pdffile = str(config['File Path']['pdf file'])
        self._docxfile = str(config['File Path']['docx file'])
        self._docfile = str(config['File Path']['doc file'])
        self._excelfile = str(config['File Path']['excel file'])
        self._csvfile = str(config['File Path']['csv file'])
        self._odtfile = str(config['File Path']['odt file'])
        self._directorylist = str(config['Directory']['directory list'])
        self._archivedirectory = str(config['Directory']['archive directory'])
        self._resumedirectory = str(config['Directory']['resume directory'])
        self._xchangeresumedirectory = str(config['Directory']['xCHANGEresume directory'])
        self._allphrasefile = str(config['File Path']['phrase file'])
        self._executioncountfile = str(
            config['File Path']['executioncount file'])
        self._logfile = str(config['File Path']['log file'])
        self._xchangelogfile = str(config['File Path']['Xchangelog file'])
        self._analyticsdirectory = str(
            config['Directory']['analytics directory'])
        self._datacollectiondb = str(config['Database']['datacollect db'])
        self._mongodbport = str(config['Database']['mongoDB port'])
        self._datacollectiondbcollection = str(
            config['Database']['collection datacollectiondb'])
        self._stdbcollection = str(
            config['Database']['collection STdatacollectiondb'])
        self._jobportal = str(config['Data Sources']['job Portal'])
        self._configcollection = str(
            config['Database']['collection configuration'])
        self._stconnstr = str(config['Connection Strings']['ST connstr'])
        self._xchangeconnstr = str(
            config['Connection Strings']['Xchange connstr'])
        self._xchangejobqueryid = str(config['Query']['Xchange job query id'])
        self._xchangecandidatequeryid = str(
            config['Query']['Xchange candiate query id'])
        self._stjobqueryid = str(config['Query']['ST job query id'])
        self._stcandidatequeryid = str(
            config['Query']['ST candidate query id'])
        self._querycollection = str(config['Database']['queries collection'])
        self._jobdetails = str(config['Document Type']['job details'])
        self._supplierdetails = str(config['Document Type']['supplier details'])
        self._candidatedetails = str(
            config['Document Type']['candidate details'])
        self._st = str(config['Data Sources']['ST'])
        self._xchange = str(config['Data Sources']['Xchange'])
        self._queryfilesdirectory = str(
            config['Directory']['queryfiles directory'])
        self._misc = str(config['Data Sources']['misc'])
        self._stcandidatesubmissionsqueryid = str(
            config['Query']['ST candidate submissions query id'])
        self._stcandidateresumesqueryid = str(
            config['Query']['ST candidate resumes query id'])
        self._promptcloudurl = str(config['URLs']['PromptCloud URL'])
        self._pccompfolder = str(
            config['Directory']['promptcloud compressed data'])
        self._pcfilefolder = str(
            config['Directory']['promptcloud file data'])
        self._xchangereqcollection = str(
          config['Database']['collection xchangerequirements'])
        self._xchangecandidatecollection = str(
          config['Database']['collection xchangecandidates'])
        self._streqcollection = str(
            config['Database']['collection STreqcollection'])
        self._stcandidatecollection = str(
            config['Database']['collection STcandidatecollection'])
        self._streqdocqueryid = str(
            config['Query']['ST req desc file query id'])
        self._reqdocdirectory = str(config['Directory']['reqdoc directory'])
        self._stbinconnstr = str(config['Connection Strings']['STBin connstr'])
        self._xchangebinconnstr = str(config['Connection Strings']['Xchange connstr'])
        self._stcandstatqueryid = str(
            config['Query']['ST candidate status query id'])
        self._pcdataanalysisresultsfile = str(
            config['File Path']['pc data analysis results'])
        self._xchangecandidateresumesqueryid = str(
            config['Query']['xCHANGE candidate resumes query id'])
        self._intelligencedatacollection = str(
            config['Database']['collection intelligencedata'])
        self._distinctjdlogfile = str(config['File Path']['distinct jd log file'])
        self._stopwords = str(config['Items']['stop words'])
        self._xchangedocumentintegerfile = str(
            config['File Path']['xchange document integer file'])
        self._xchangereqdirectory = str(
            config['Directory']['xchangereq directory'])
        self._xchangedocumentintegerfilebig = str(
            config['File Path']['xchange_big document integer file'])
        self._xchangereqdirectoryinit = str(
            config['Directory']['xchangereq_init directory'])
        self._streqchangesqueryid = str(config['Query']['ST job changes query id'])
        self._stcandidatechangesqueryid = str(config['Query']['ST candidate changes query id'])
        self._xchangecandidatechangesqueryid = str(config['Query']['Xchange candidate changes query id'])
        self._xchangematchindexcoll = str(config['Database']['collection XchangeMatchIndex'])
        self._stmatchindexcoll = str(config['Database']['collection STMatchIndex'])
        self._mongodbhost = str(config['Database']['mongoDB host'])
        self._stsupplierquiryid = str(config['Query']['ST Supplier names query id'])
        self._stsupplierscoll = str(config['Database']['collection STsuppliers'])
        self._resumesdetectdb = str(config['Database']['resumesDetect db'])
        self._filedetections = str(config['Database']['collection filedetections'])
        self._filedetectiondetails = str(config['Database']['collection filedetectiondetails'])
        self._jobserverhost = str(config['Job Server']['JobServerHost'])
        self._jobserverport = str(config['Job Server']['JobServerPort'])
        self._jobserverclasspath = str(config['Job Server']['JobServerClassPath'])
        self._jobserverstclasspath = str(config['Job Server']['JobServerClassPathST'])
        self._jobservermethod = str(config['Job Server']['JobServerMethod'])
        self._filedirectory = str(config['Directory']['file directory'])
        self._enviraonmentprod = str(config['ENV Variables']['EnvProd'])
        self._enviraonmentqa = str(config['ENV Variables']['EnvQA'])
        self._enviraonmentdemo = str(config['ENV Variables']['EnvDemo'])
        self._enviraonmentdemo6 = str(config['ENV Variables']['EnvDemo6'])
        self._enviraonmentsandbox = str(config['ENV Variables']['EnvSandbox'])
        self._intelligencedb = str(config['Database']['intelligence db'])
        self._datadbprodconnection = str(config['Database']['data db prod connection'])
        self._scriptsdirectory = str(config['Directory']['scripts directory'])
        self._transferscriptfile = str(config['File Path']['transfer script file'])
        self._copyscriptfile = str(config['File Path']['copy script file'])
        self._promptcloudlogfile = str(config['File Path']['prompt cloud log file'])
        self._stwebapihost = str(config['ST Resume screening']['STAPIHost'])
        self._stwebapiurl = str(config['ST Resume screening']['STAPIurl'])

        self._stclient = str(config['Query']['ST Client names query id'])
        self._stmsp = str(config['Query']['ST MSP names query id'])
        self._stlaborcatagory = str(config['Query']['ST labor Catagory names query id'])
        self._sttypeofservice = str(config['Query']['ST typeOf Service names query id'])
        self._clientdetails = str(config['Document Type']['client details'])
        self._mspdetails = str(config['Document Type']['msp details'])
        self._laborcatagorydetails = str(config['Document Type']['labor catagory details'])
        self._typeofservicedetails = str(config['Document Type']['type of service details'])
        self._stclientscoll = str(config['Database']['collection Clients'])
        self._stmspcoll = str(config['Database']['collection MSPUsers'])
        self._stlaborcatagorycoll = str(config['Database']['collection laborCatagory'])
        self._sttypeofservicecoll = str(config['Database']['collection typeOf Service'])
        self._ratesdb = str(config['Database']['rates db'])
        self._ratesconfigcollection = str(config['Database']['collection rates config'])
        self._laborcatetypeofservicefile = str(config['File Path']['labor cate and type of service'])
        self._currencydetails = str(config['Document Type']['currency details'])
        self._ratescurrencycoll = str(config['Database']['collection rates currency'])
        self._currencyqueryid = str(config['Query']['currency query id'])
        self._industryqueryid = str(config['Query']['industry query id'])
        self._industrydetails = str(config['Document Type']['industry details'])
        self._ratesindustrycoll = str(config['Database']['collection rates industry'])
        self._suppliernameexceptions = str(config['Database']['collection suppliernameexceptions'])
        self._urlexceptions = str(config['Database']['collection urlexceptions'])
        self._extensionexceptions = str(config['Database']['collection extensionexceptions'])
        self._stwebapisenddata = str(config['ST Resume screening']['STAPIsendData'])
        self._stagingcoll = str(config['Database']['collection staging'])
        self._mastercoll = str(config['Database']['collection master'])
        self._maskingcoll = str(config['Database']['collection masking'])
        self._uiformpost = str(config['Data Sources']['uiFormPost'])
        self._host = str(config['Job Server']['Host'])
        self._port = str(config['Job Server']['Port'])
        self._api = str(config['Job Server']['API'])
        self._geographicaldataconnstr = str(config['Connection Strings']['GeographicalData connstr'])
        self._masterdatauploadpath = str(config['Directory']['uploaded file directory'])
        self._fileupload = str(config['Data Sources']['fileUpload'])
        self._streqcandidateratesqueryid = str(config['Query']['ST Req Candidate rates query id'])
        self._streqratesqueryid = str(config['Query']['ST Req rates query id'])
        self._reqcandidateratesdetails = str(config['Document Type']['req candidate rates details'])
        self._reqratesdetails = str(config['Document Type']['req rates details'])
        self._masterdocumentintegerfile = str(config['File Path']['master integer document file'])
        self._promptcloudratesurl = str(config['URLs']['PromptCloud Rates URL'])
        self._pcratescompfolder = str(config['Directory']['promptcloud rates compressed data'])
        self._pcratesfilefolder = str(config['Directory']['promptcloud rates file data'])
        self._promptcloud = str(config['Data Sources']['Prompt Cloud'])
        self._collpcratesdata = str(config['Database']['collection pcratesdata'])
        self._stpromptcloudconnstr = str(config['Connection Strings']
                                         ['ST PromptCloud connstr'])
        self._stpromptclouddataqueryid = str(config['Query']['ST Promtcloud Data query id'])
        self._promptclouddatadetails = str(config['Document Type']['promtcloud data details'])
        self._pcdataloaddb = str(config['Database']['pcdataloaded_forLucas db'])
        self._collstaging = str(config['Database']['staging collection'])
        self._datadbqa = str(config['Database']['data db qa connection'])
        self._stcandidatecurrencyqueryid = str(config['Query']['ST Candidate Currency query id'])
        self._candidatecurrencydetails = str(config['Document Type']['candidate currency details'])
        self._geoconnstr = str(config['Connection Strings']['Geo connstr'])
        self._geodatafetchqueryid = str(config['Query']['Geo Data Fetch'])
        self._geodatafetchdetails = str(config['Document Type']['Geo Data Fetch details'])
        self._promptclouds3bucketbackup = str(config['Directory']['promptcloud s3 bucket backup'])
        self._promptcloudrecordlimit = str(config['Items']['prompt cloud record limit'])
        self._promptcloudrecordlimitset = str(config['Items']['prompt cloud record limit set'])
        self._transfermasterintgraphfile = str(config['File Path']['transfer master int graph file'])
        self._webserverip = str(config['User Ips']['webserver'])
        self._externalhost = str(config['Database']['external host'])
        self._mongoclient = str(config['Database']['mongo client'])
        self._stagingmastertransferstep = str(config['Items']['staging master transfer step'])
        self._minrandomvalue = str(config['Items']['Min Random Value'])
        self._maxrandomvalue = str(config['Items']['Max Random Value'])
        self._minrandpercentagevalue3m = str(config['Items']['Min Random Percentage Value for 3M Rates'])
        self._maxrandpercentagevalue3m = str(config['Items']['Max Random Percentage Value for 3M Rates'])
        self._minrandpercentagevalue12m = str(config['Items']['Min Random Percentage Value for 12M Rates'])
        self._maxrandpercentagevalue12m = str(config['Items']['Max Random Percentage Value for 12M Rates'])
        self._minrandpercentagevaluemonthly = str(config['Items']['Min Random Percentage Value for Monthly Rates'])
        self._maxrandpercentagevaluemonthly = str(config['Items']['Max Random Percentage Value for Monthly Rates'])
        self._minrandpercentagevalueft = str(config['Items']['Min Random Percentage Value for FullTime Rates'])
        self._maxrandpercentagevalueft = str(config['Items']['Max Random Percentage Value for FullTime Rates'])
        self._pcwrongdatafile = str(config['File Path']['PromptCloud Wrong Data Logs'])
        self._sparkrebootscript = str(config['File Path']['spark reboot script'])
        self._webserverpassword = str(config['ENV Variables']['web server password'])
        self._jobserverpassword = str(config['ENV Variables']['job server password'])
        self._dbserverpassword = str(config['ENV Variables']['db server password'])
        self._mountdirectory = str(config['Directory']['mount directory'])
        self._knowledgefilestransferscript = str(config['File Path']['knowledge files transfer script'])
        self._knowledgefilesbackup = str(config['Directory']['knowledge files backup'])
        self._stclosedreqsapiurl = str(config['ST Resume screening']['STClosedreqsAPIurl'])
        self._knowledgefiless3bucketbackup = str(config['Directory']['knowledge files s3 bucket backup'])
        self._knowledgefiles = str(config['Directory']['knowledge files'])
        self._streqmatchapiurl = str(config['ST Resume screening']['STReqsClosedMatchAPIurl'])
        self._sparkjarfilepath = str(config['Job Server']['spark jar file path'])
        self._sparkcontext = str(config['Job Server']['spark context'])
        self._sparknumcpucores = str(config['Job Server']['spark num-cpu-cores'])
        self._sparkmemorypernode = str(config['Job Server']['spark memory-per-node'])
        self._sparkdrivermemory = str(config['Job Server']['spark driver memory'])

    @property
    def TextFile(self):
        return self._textfile

    @property
    def PdfFile(self):
        return self._pdffile

    @property
    def DocxFile(self):
        return self._docxfile

    @property
    def DocFile(self):
        return self._docfile

    @property
    def ExcelFile(self):
        return self._excelfile

    @property
    def CsvFile(self):
        return self._csvfile

    @property
    def OdtFile(self):
        return self._odtfile

    @property
    def DirectoryList(self):
        return self._directorylist

    @property
    def ArchiveDirectory(self):
        return self._archivedirectory

    @property
    def PhraseFile(self):
        return self._allphrasefile

    @property
    def ExecutioncountFile(self):
        return self._executioncountfile

    @property
    def LogFile(self):
        return self._logfile

    @property
    def XchangeLogFile(self):
        return self._xchangelogfile

    @property
    def AnalyticsDirectory(self):
        return self._analyticsdirectory

    @property
    def DataCollectionDB(self):
        return self._datacollectiondb

    @property
    def MongoDBPort(self):
        return self._mongodbport

    @property
    def DataCollectionDBCollection(self):
        return self._datacollectiondbcollection

    @property
    def JobPortal(self):
        return self._jobportal

    @property
    def ConfigCollection(self):
        return self._configcollection

    @property
    def STConnStr(self):
        return self._stconnstr

    @property
    def XchangeConnStr(self):
        return self._xchangeconnstr

    @property
    def XchangeJobQueryId(self):
        return self._xchangejobqueryid

    @property
    def XchangeCandidateQueryId(self):
        return self._xchangecandidatequeryid

    @property
    def STJobQueryId(self):
        return self._stjobqueryid

    @property
    def STCandidateQueryId(self):
        return self._stcandidatequeryid

    @property
    def QueryCollection(self):
        return self._querycollection

    @property
    def JobDetails(self):
        return self._jobdetails

    @property
    def CandidateDetails(self):
        return self._candidatedetails

    @property
    def ST(self):
        return self._st

    @property
    def Xchange(self):
        return self._xchange

    @property
    def QueryFilesDirectory(self):
        return self._queryfilesdirectory

    @property
    def Misc(self):
        return self._misc

    @property
    def STCandidateSubmissionsQueryId(self):
        return self._stcandidatesubmissionsqueryid

    @property
    def STDBCollection(self):
        return self._stdbcollection

    @property
    def ResumeDirectory(self):
        return self._resumedirectory

    @property
    def xCHANGEResumeDirectory(self):
        return self._xchangeresumedirectory

    @property
    def STCandidateResumesQueryId(self):
        return self._stcandidateresumesqueryid

    @property
    def PromptCloudURL(self):
        return self._promptcloudurl

    @property
    def PCCompFolder(self):
        return self._pccompfolder

    @property
    def PCFileFolder(self):
        return self._pcfilefolder

    @property
    def STReqCollection(self):
        return self._streqcollection

    @property
    def STCandidateCollection(self):
        return self._stcandidatecollection

    @property
    def STReqDocQueryId(self):
        return self._streqdocqueryid

    @property
    def ReqDocDirectory(self):
        return self._reqdocdirectory

    @property
    def STBinConnStr(self):
        return self._stbinconnstr

    @property
    def xCHANGEBinConnStr(self):
        return self._xchangebinconnstr

    @property
    def STCandidateStatusQueryId(self):
        return self._stcandstatqueryid

    @property
    def PCDataAnalysisResultsFile(self):
        return self._pcdataanalysisresultsfile

    @property
    def xCHANGEReqCollection(self):
        return self._xchangereqcollection

    @property
    def xCHANGECandidateCollection(self):
        return self._xchangecandidatecollection

    @property
    def xCHANGECandidateResumesqueryid(self):
        return self._xchangecandidateresumesqueryid

    @property
    def IntelligenceDataCollection(self):
        return self._intelligencedatacollection

    @property
    def DistinctJdLogFile(self):
        return self._distinctjdlogfile

    @property
    def StopWords(self):
        return self._stopwords

    @property
    def xCHANGEDocumentIntegerFile(self):
        return self._xchangedocumentintegerfile

    @property
    def xCHANGEReqDirectory(self):
        return self._xchangereqdirectory

    @property
    def xCHANGEDocumentIntegerFileBig(self):
        return self._xchangedocumentintegerfilebig

    @property
    def xCHANGEReqDirectoryInit(self):
        return self._xchangereqdirectoryinit

    @property
    def STReqChangesQueryId(self):
        return self._streqchangesqueryid

    @property
    def STCandidateChangesQueryId(self):
        return self._stcandidatechangesqueryid

    @property
    def xCHANGECandidateChangesQueryId(self):
        return self._xchangecandidatechangesqueryid

    @property
    def XchangeMatchIndexColl(self):
        return self._xchangematchindexcoll

    @property
    def STMatchIndexColl(self):
        return self._stmatchindexcoll

    @property
    def mongoDBHost(self):
        return self._mongodbhost

    @property
    def STSupplierQueryID(self):
        return self._stsupplierquiryid

    @property
    def STSupplierDetails(self):
        return self._supplierdetails

    @property
    def STSupplierCollection(self):
        return self._stsupplierscoll

    @property
    def JobServerHost(self):
        return self._jobserverhost

    @property
    def JobServerPort(self):
        return self._jobserverport

    @property
    def JobServerClassPath(self):
        return self._jobserverclasspath

    @property
    def JobServerClassPathST(self):
        return self._jobserverstclasspath

    @property
    def JobServerMethod(self):
        return self._jobservermethod

    @property
    def resumesDetectDb(self):
        return self._resumesdetectdb

    @property
    def fileDetectionsColl(self):
        return self._filedetections

    @property
    def fileDirectory(self):
        return self._filedirectory

    @property
    def envProduction(self):
        return self._enviraonmentprod

    @property
    def envQA(self):
        return self._enviraonmentqa

    @property
    def envDemo(self):
        return self._enviraonmentdemo

    @property
    def envDemo6(self):
        return self._enviraonmentdemo6

    @property
    def envSandbox(self):
        return self._enviraonmentsandbox

    @property
    def IntelligenceDb(self):
        return self._intelligencedb

    @property
    def DataDbProdConnection(self):
        return self._datadbprodconnection

    @property
    def ScriptsDirectory(self):
        return self._scriptsdirectory

    @property
    def TransferScriptFile(self):
        return self._transferscriptfile

    @property
    def CopyScriptFile(self):
        return self._copyscriptfile

    @property
    def PromptcloudLogFile(self):
        return self._promptcloudlogfile

    @property
    def STwebApiHost(self):
        return self._stwebapihost

    @property
    def STwebApiUrl(self):
        return self._stwebapiurl

    @property
    def STClientsQueryid(self):
        return self._stclient

    @property
    def STMSPQueryid(self):
        return self._stmsp

    @property
    def STlaborCatagoryQueryid(self):
        return self._stlaborcatagory

    @property
    def STtypeofServiceQueryid(self):
        return self._sttypeofservice

    @property
    def STClientDetails(self):
        return self._clientdetails

    @property
    def STMSPDetails(self):
        return self._mspdetails

    @property
    def STlaborCatagoryDetails(self):
        return self._laborcatagorydetails

    @property
    def STTypeofServiceDetails(self):
        return self._typeofservicedetails

    @property
    def STClientsColl(self):
        return self._stclientscoll

    @property
    def STMSPColl(self):
        return self._stmspcoll

    @property
    def STlaborCateColl(self):
        return self._stlaborcatagorycoll

    @property
    def STtypeofServiceColl(self):
        return self._sttypeofservicecoll

    @property
    def RatesDB(self):
        return self._ratesdb

    @property
    def RatesConfigCollection(self):
        return self._ratesconfigcollection

    @property
    def laborCatetypeOfServicefile(self):
        return self._laborcatetypeofservicefile

    @property
    def currencyDetails(self):
        return self._currencydetails

    @property
    def currecyColl(self):
        return self._ratescurrencycoll

    @property
    def currencyQueryID(self):
        return self._currencyqueryid

    @property
    def IndustryQueryID(self):
        return self._industryqueryid

    @property
    def industryDetails(self):
        return self._industrydetails

    @property
    def ratesIndustryColl(self):
        return self._ratesindustrycoll

    @property
    def fileDetectionDetailsColl(self):
        return self._filedetectiondetails

    @property
    def supplierNameExceptions(self):
        return self._suppliernameexceptions

    @property
    def urlExceptions(self):
        return self._urlexceptions

    @property
    def extensionExceptions(self):
        return self._extensionexceptions

    @property
    def stWebApiSendData(self):
        return self._stwebapisenddata

    @property
    def stagingCollection(self):
        return self._stagingcoll

    @property
    def masterCollection(self):
        return self._mastercoll

    @property
    def MaskingColl(self):
        return self._maskingcoll

    @property
    def uiFormPost(self):
        return self._uiformpost

    @property
    def Host(self):
        return self._host

    @property
    def Port(self):
        return self._port

    @property
    def API(self):
        return self._api

    @property
    def geographicalDataConnstr(self):
        return self._geographicaldataconnstr

    @property
    def masterDataUploadPath(self):
        return self._masterdatauploadpath

    @property
    def fileUpload(self):
        return self._fileupload

    @property
    def STReqCandidateRatesQueryId(self):
        return self._streqcandidateratesqueryid

    @property
    def STReqRatesQueryId(self):
        return self._streqratesqueryid

    @property
    def STReqCandidateRatesDetails(self):
        return self._reqcandidateratesdetails

    @property
    def STReqRatesDetails(self):
        return self._reqratesdetails

    @property
    def masterDocumentIntegerFile(self):
        return self._masterdocumentintegerfile

    @property
    def PromptCloudRatesURL(self):
        return self._promptcloudratesurl

    @property
    def PCRatesCompFolder(self):
        return self._pcratescompfolder

    @property
    def PCRatesFileFolder(self):
        return self._pcratesfilefolder

    @property
    def promptCloud(self):
        return self._promptcloud

    @property
    def PCRatesDataColl(self):
        return self._collpcratesdata

    @property
    def STPromptcloudDataQueryId(self):
        return self._stpromptclouddataqueryid

    @property
    def STPromptCloudConnStr(self):
        return self._stpromptcloudconnstr

    @property
    def STPromptcloudDataDetails(self):
        return self._promptclouddatadetails

    @property
    def PCDataLoaddb(self):
        return self._pcdataloaddb

    @property
    def collStaging(self):
        return self._collstaging

    @property
    def DataDbQA(self):
        return self._datadbqa

    @property
    def STCandidateCurrencyQueryId(self):
        return self._stcandidatecurrencyqueryid

    @property
    def STCandidateCurrencyDetails(self):
        return self._candidatecurrencydetails

    @property
    def GeoDataFetchQueryId(self):
        return self._geodatafetchqueryid

    @property
    def GeoConnstr(self):
        return self._geoconnstr

    @property
    def GeoDataFetchDetails(self):
        return self._geodatafetchdetails

    @property
    def PromptCloudS3BucketBackup(self):
        return self._promptclouds3bucketbackup

    @property
    def PromptCloudRecordLimit(self):
        return self._promptcloudrecordlimit

    @property
    def PromptCloudRecordLimitSet(self):
        return self._promptcloudrecordlimitset

    @property
    def transferMasterIntGraphFile(self):
        return self._transfermasterintgraphfile

    @property
    def webServerIp(self):
        return self._webserverip

    @property
    def ExternalHost(self):
        return self._externalhost

    @property
    def MongoClient(self):
        return self._mongoclient

    @property
    def StagingMasterTransferStep(self):
        return self._stagingmastertransferstep

    @property
    def minRandomValue(self):
        return self._minrandomvalue    

    @property
    def maxRandomValue(self):
        return self._maxrandomvalue
 
    @property
    def minRandPercentageValue3M(self):
        return self._minrandpercentagevalue3m

    @property
    def maxRandPercentageValue3M(self):
        return self._maxrandpercentagevalue3m

    @property
    def minRandPercentageValue12M(self):
        return self._minrandpercentagevalue12m

    @property
    def maxRandPercentageValue12M(self):
        return self._maxrandpercentagevalue12m

    @property
    def minRandPercentageValueMonthly(self):
        return self._minrandpercentagevaluemonthly

    @property
    def maxRandPercentageValueMonthly(self):
        return self._maxrandpercentagevaluemonthly

    @property
    def minRandPercentageValueFT(self):
        return self._minrandpercentagevalueft

    @property
    def maxRandPercentageValueFT(self):
        return self._maxrandpercentagevalueft

    @property
    def pcWrongDatafile(self):
        return self._pcwrongdatafile

    @property
    def sparkRebootScript(self):
        return self._sparkrebootscript

    @property
    def webServerPassword(self):
        return self._webserverpassword

    @property
    def jobServerPassword(self):
        return self._jobserverpassword

    @property
    def dbServerPassword(self):
        return self._dbserverpassword

    @property
    def mountDirectory(self):
        return self._mountdirectory

    @property
    def knowledgeFilesTransferScript(self):
        return self._knowledgefilestransferscript

    @property
    def knowledgeFilesBackup(self):
        return self._knowledgefilesbackup

    @property
    def stClosedreqsApiUrl(self):
        return self._stclosedreqsapiurl

    @property
    def knowledgeFilesS3BucketBackup(self):
        return self._knowledgefiless3bucketbackup

    @property
    def knowledgeFiles(self):
        return self._knowledgefiles

    @property
    def STreqMatchApiUrl(self):
        return self._streqmatchapiurl

    @property
    def sparkJarFilePath(self):
        return self._sparkjarfilepath

    @property
    def sparkContext(self):
        return self._sparkcontext

    @property
    def sparkNumCpuCores(self):
        return self._sparknumcpucores

    @property
    def sparkMemoryPerNode(self):
        return self._sparkmemorypernode

    @property
    def sparkDriverMemory(self):
        return self._sparkdrivermemory
