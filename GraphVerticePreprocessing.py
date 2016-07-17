import pyspark as py
import xml.etree.ElementTree as ET
import random as rn
import hashlib

# Function to parse the XML structure and find the Outlinks for a particluar article
def getvertices(inputLine):
    finalList=[]                                                # List to store vertex and its adjacent elements
    test,vertexName="",""                                       # String Variables to store XML and vertexName

    # Converting the input string to ASCII and split it according to TAB
    inputStringList=inputLine.encode("ascii","ignore").replace("\\n","").split("\t")

    # Loop to traverse throught the input string list
    for elements in inputStringList:
        # condition to check whether the article part contains XML 
        if str(elements).find("<articles")>0:
            test = str(elements)                                # Saving the element as a string
    vertexName=inputStringList[1]                               # Extracting the vertex name from the input string list

    # condition to check XML string exits else extract the XML using substring
    if len(test)==0:
        inputLine=inputLine.encode("ascii","ignore").replace("\\n","")  # this encodes the input string as ASCII replace all "\n"
        name= inputLine[0:inputLine.find("<articles")]                  # find the position of articles in the XML string
        test= inputLine[inputLine.find("<"):inputLine.rfind("</articles>")+11]      # Extracting the XML string between the articles tag
        vertexName = name.split()[1]                                    # get the vertex name which is the firsst element of the remain string

    try:
        root = ET.fromstring(test)                              # Parsing the string to XML format
        vertices=""                                             # initializing a variable to store vertex name
        verticesList=[]                                         # initialize a list element to store list of vertices
        # for value in root[0].findall(".//paragraph/sentence/link/target"):
        #     if vertices.count(str(value.text))==0 and str(value.text).startswith("File:")==False:
        #         vertices=vertices+str(value.text).replace(" ","_")+" "

        # Loop to extract all adjacent nodes
        for value in root[0].findall(".//paragraph/extension/template/param[@name='title']"):
            # Condition to check whether the given element has already been extracted
            if verticesList.count(str(value.text))==0:
                vertices=vertices+str(value.text).replace(" ","_")+" "  # replace all spaces with "_"
                verticesList.append(str(value.text))                    # append the element into the vertex list
        yield str(vertexName).replace(" ","_")+" "+vertices             # returning the adjacency list of vertex along with name
    except Exception as e:
        print ("Vertices "+inputStringList[1]+" has data issue")

# Function to split the adjacency list into vertex and a adjacent element
def getStringEdgeList(input):
    inputList=str(input).split(" ")                                     # Split the space seperate adjacency list
    for i in range(1,len(inputList)-1):                                 # traverse the adjacency list from the second element
        yield [inputList[0], inputList[i]]                              # returning the vertex and adjacent element

# Function to convert string vertex name into Long for Graphx
def stringToLong(vertixString):
    vertixID=[]                                                         # variable to store vertexIDs
    verticeMapping={}                                                   # variables to store the element as tuples
    finalList,mappingList=[],[]
    # Loop to traverse through the vertex string
    for vertices in vertixString:
        # Loop to traverse through vertex list
        for word in list(vertices):
            ranVal=rn.randint(1000,9999999999)                          # Generate a random 10 digit number
            # Condition to check whether the vertex ID is duplicate and vertex is non null
            if (vertixID.count(ranVal)==0) and str(word)!='':
                vertixID.append(ranVal)                                 # saving the generate ID into a list to avoid duplicate
                verticeMapping.update({word:str(ranVal)})               # saving the vertex and its corresponding long ID

        # Condition to remove out dupliacte elements
        if str(list(vertices)[0])!=str(list(vertices)[1]):
            vert1=str(verticeMapping.get(list(vertices)[0]))            # retrieve the long vertex ID from tuple
            vert2=str(verticeMapping.get(list(vertices)[1]))            # retrieve the long vertex ID from tuple
            # condition to check whether the vertex name is not null
            if verticeMapping.get(list(vertices)[1]) !=None:
               finalList.append(vert1+" "+vert2)                        # create an adjacency list with vertex ID

    # Convert tuple into string of elements
    for key,value in verticeMapping.items():
        mappingList.append(key+" "+value)
    return finalList,mappingList

def getLongVertexList(inputLineList):
    return str(int(hashlib.md5(inputLineList[0]).hexdigest(), 16))[0:12]+" "+str(int(hashlib.md5(inputLineList[1]).hexdigest(), 16))[0:12]

def getStringLongList(inputLineList):
    for vertex in inputLineList:
        yield [vertex,str(int(hashlib.md5(vertex).hexdigest(), 16))[0:12]]

def getText(inputLineList):
    return inputLineList[1]+" "+inputLineList[0]

if __name__=="__main__":
    configuartion=py.SparkConf()                                                    # setting the Spark Configuration
    sContext=py.SparkContext(conf=configuartion)                                    # setting the Spark context
    dataSetFile=sContext.textFile("/vyas/BigData/Assignment2/Ref/freebase-wex-2009-01-12-freebase_articles.tsv",use_unicode=True)  #  reading the input file
    #dataSetFile=sContext.textFile("s3n://bigdataproject/Assignment2/Input/",use_unicode=True)
    #dataSetFile=sContext.textFile("/home/vyassu/freebase-wex-2009-01-12-freebase_articles.tsv",use_unicode=True)  # reading the input file
    print ("Total no of Elements:",dataSetFile.count())
    vertexList = dataSetFile.flatMap(getvertices).cache()                                         # Convert the input file into vertexRDD
    verticeString = vertexList.flatMap(getStringEdgeList).cache()                                 # convert the vertexRDD into adjacency matrix
    #verticeString.flatMap(getStringLongList).map(getText).distinct().saveAsTextFile("/vyas/BigData/Assignment2/stringlongfolder")
    lonVertList = verticeString.map(getLongVertexList)
    #lonVertList.saveAsTextFile("/vyas/BigData/Assignment2/longvertex")                   # Saving the Long adjacency matrix into file for Graphx
    vertexList.saveAsTextFile("/vyas/BigData/Assignment2/interimOutput1")                 # Saving the adjacency matrix into file for Spark Implementation