import pyspark as py
import re
import datetime


# function to split the input and assign default of 1
def setdefaultrank(inputLine):
    for everyWord in inputLine.split():                         # Splits the given input line into list of words
        yield (everyWord,1)                                     # Assigning default weight of 1 to each word in the input

# Function to split the input into list of words
def mapper(inputLine):
    inputLine=inputLine.encode("ascii","ignore")		# Converting string from UTF-8 to ASCII
    wordList=""							# String variable to store the adjacent nodes of a vertex 
    inputLineList = inputLine.split()				# Splitting the input into List of words
    for i in range(1,len(inputLineList)):			# Looping from the second word in the list to create the adjacency matrix
        wordList=inputLineList[i]+" "+wordList;			# Creating the adjacency matrix
    yield (inputLineList[0],str (wordList).rstrip(" "))		# emitting the vertex and adjacent elements
                                                            

# Function to calculate the difference between old rank and new rank of a vertex
def delta(param):
    value=list(param[1])                                        # Spliting the input into list
    deltaValue=abs(value[0]-value[1])                           # taking the absolute value of the difference between the old and new rank
    return deltaValue                                           # returning the delta value

# Function to calculate the new Rank for the outlinks of a vertex
def calculateRank(input):
    vertexName=input[0]						# extracting the vertex name
    elementRank=float(input[1][0])				# extracting the vertex rank
    elementList = str(input[1][1]).split()			# Splitting the adjacent elements into List
    if len(elementList)!=0:					# Condition to check adjacent element exists
        newElementRank=elementRank/len(elementList)		# Calculating the new Rank from the old rank and no of outlinks
    else:
        newElementRank=elementRank/1				# Calculating the new rank from the old rank
    for element in elementList:					# Loop to traverse throught a list
        if elementList.count(element)==1 and element!='None':	# Condition to check if duplicate elements exists and removing NUll elements
            yield(element,newElementRank)			# Emitting the new rank with the vertex name

# Function to filter out vertices/pages corresponding to universities
def getUniversityRanking(input):
    pattern1 = re.compile("university", re.IGNORECASE)		# Regular expression to extract all the elements with University in their name
    pattern2 = re.compile("of", re.IGNORECASE)			
    vertexDetails=list(input)					# converting the input into list of elements
    patternList1 = pattern1.findall(vertexDetails[0])		# finding the vertex which contain the expression
    patternList2 = pattern2.findall(vertexDetails[0])	
    if len(patternList1)!=0 and len(patternList2)!=0:		# Checking to see if the variable contains both University and of
        yield input


if __name__=="__main__":
    configuartion=py.SparkConf()                                                     # setting the Spark Configuration
    sContext=py.SparkContext(conf=configuartion)                                     # setting the Spark context
    dataSetFile=sContext.textFile("/vyas/BigData/Assignment2/interimOutput").cache() # Fetching the data file from HDFS

    noOfIterations=1                                                                 # Counter to calculate the number of iterations
    vertexRank=dataSetFile.flatMap(setdefaultrank).distinct().cache()                # setting default value to the vertex
    totalElement= vertexRank.count()                                                 # calculating the number of distinct elements
    weightAggregate = float(totalElement)                                            # setting the default value for rank convergence
    verticesList=dataSetFile.flatMap(mapper).distinct()                              # Converting the RDD into list for faster processing
    vertexDetails=sContext.parallelize(vertexRank.join(verticesList).collect())	     # Joining the default Rank and Adjacent elements of node
    vertexnewRank= vertexDetails.flatMap(calculateRank).reduceByKey(lambda a,b: a+b) # Calculating new Rank from the adjacency list
    iterationTime={}								     # Dictionary to store Iteration count and Time taken for each iteration
    #while(noOfIterations<3):							     # Loop to calcuate Graph based on No of iterations
    while(weightAggregate>(0.001*totalElement)):				     # Loop to calculate Graph on the basis of convergence
        startTime=datetime.datetime.now().microsecond				     # Startime of the iteration
        weightAggregate = vertexnewRank.join(vertexRank).map(delta).sum()/float(totalElement)  # Calculate the change in rank from the previous value
        vertexRank1=vertexnewRank						     # Copying old rank into new rank
        vertexDetails=sContext.parallelize(vertexRank1.join(verticesList).collect()) # Joining the default Rank and Adjacent elements of node
        vertexnewRank=vertexDetails.flatMap(calculateRank).reduceByKey(lambda a,b: a+b) # Calculating new Rank from the adjacency list
        endTime=datetime.datetime.now().microsecond				     # End time of the iteration
        iterationTime.update({noOfIterations:(endTime-startTime)})		     # Storing the iteration count and time
        noOfIterations=noOfIterations+1						     # incrementing the iteration counter
							     
    finalRankRDD=sContext.parallelize(vertexnewRank.sortBy(lambda x: x[1],ascending=False).take(100))  # RDD to store Top 100 vertices
    print ("The top 100 vertices:"+str(vertexnewRank.sortBy(lambda x: x[1],ascending=False).take(100))) # Printing the Top 100 vertices
    finalRankRDD.saveAsTextFile("/vyas/BigData/Assignment2/SparkOutput")	     # saving the Top 100 vertices into a file
    print("Time for Iterations:"+str(iterationTime))				     # Printing iteration time for Performance analysis
