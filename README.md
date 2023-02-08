# Analyzing-Reddit-and-Netflix-Datasets-using-Spark

NetflixGraphGenerate.java:



• The code creates a Spark Session.
• The next step is to read in the input file from where we want to start reading data from and
converts it into a Java RDD.
• The movieID is then used to create a new tuple with an Integer value for movieID and
customerID.
• The rating is then parsed from the next line of input, which creates a new tuple with an Integer
value for rating.
• The code parses the movieID, ratings and makes new tuple.
• Then movieID and ratings are stored into a tuple.
• The another tuple is created that contains the first tuple along with customerID.
• The purpose of this code is to create a new tuple with three elements: movieID, rating, and
customerID.
• The these there values are printed to the screen.
NetflixMovieAverage.java:
• The code starts by reading the input file and creating a SparkSession.
• Then iterates through each line of text in the file, splitting it into an array of strings using a
regular expression that matches commas.
• Next, it parses out the first string from each array to get the movie ID.
• The next step is to create an RDD called lines which contains all of the lines in our input file as
Java objects.
• Then we map this RDD to a new RDD called temp which will contain pairs with two elements:
one being an integer representing the movie: ID and another being a float representing how
many times that movie was streamed on Netflix during its lifetime (in millions).
• After that a text file is read and transformed it into a JavaPairRDD.
• The function mapToPair takes in a String and splits it into multiple strings.
• Then iterates over the lines of text and creates a JavaPairRDD.
• It groups the RDD by splitting on commas in order to create an RDD with two elements: one
for each line of text. The mapToPair method then takes each element from the RDD and returns
a new pair consisting of the movie ID and rating.
• The resulting output corresponds to the movie ID and the rating corresponding to that movie
ID.
• Then it creates a new RDD called cleanSlate.
• It then uses the reduceByKey method to create an RDD of JavaPairRDD objects.
• After that creates a counter that counts how many times each movieID appears in the data set.
• The next line creates an object called temp which is initialized with a list of tuples containing
two integers and one float value for each movieID in the data set.
• Next, it iterates over all pairs of values (i1, i2) where i1 is greater than or equal to 0 and less
than or equal to count-1 (the total number of movies).
• It then calls reduceByKey on both input RDDs: counter and counts.
• This results in two new RDDs: totalOccurences and finalOutput respectively.
• Finally, it maps over each tuple from finalOutput using mapToPair to produce a list containing
two integers representing the movie ID followed by its average rating.
• The code will produce a new RDD with the following shape: (movieID, Σ Rating) The code
above will first collect all the movie IDs from the input data and then compute the average
rating for each movie.
• The output of this code is a new RDD with two values: (movieID, Σ Rating).
RedditHourImpact.java:
• The code creates a SparkSession object, which is used to interact with Spark's API.
• Next, it defines a function that takes two arguments: an input string and a format string that
specifies how the date should be formatted.
• It uses this function to parse the input string into a Date object using SimpleDateFormat's
format method and returns it as a Tuple2 value (a tuple containing two values).
• Next, it creates an RDD from the parsed date data using JavaPairRDD's rdd method on String
type.
• Then it filters out any nulls from this RDD using filterByNullsOn() method on String type of
data because all its elements are strings too.
• Finally, it prints out some statistics about this RDD using collect().
• The code creates an instance of the JavaPairRDD class, which is used for reading and writing
data in RDD format. The code creates an instance of the JavaRDD class, which is used for
reading and writing data in DataFrame format.
• Next, it checks to see if there is a comma after the first argument (the input path).
• If so, then it uses that as the delimiter for parsing out all of the arguments from that point on.
• Next, it creates a pattern using COMMA and parses out all of the arguments into individual
fields with commas between them. Then they are put into variables for later use.
• The code will read the file specified and print out a list of all the rows.
• The following code is what is called an "in-line" function.
• This means that this function can be placed within other functions in order to execute them with
one line of code.
• The mapToPair method is used to convert each line in the JavaRDD into a Tuple2.
• The reduceByKey method is used to group all of the Tuple2 pairs together by their key value.
• The sortByKey method sorts all of these grouped pairs by their key values.
• The collect method iterates over all of these grouped pairs and prints them out on screen
RedditPhotoImpact.java:
• The code first creates a SparkSession object.
• InputPath of the csv reddit data file that is to be read.
• ID, upvotes, downvotes and comments are stored in variable from the file.
• The code then calls the JavaRDD method to convert Input file and store it in “lines” variable.
• Then from the data read from text file the impact is calculated using following formula:
impact = upvote + downvote + comments
• The resulting impact is printed to the screen.
Based on the result of RedditPhotoImpact, the most impactful photo of the whole dataset is “1437” which has an impact of 192896.
Photo ID Impact
1437 192896
Based on the result of RedditHourImpact, the most impactful hour of the whole dataset is “20:00” which has an impact /reach of 15057971.
Hour(EST) Impact
20 15057971

Based on the result of NetflixMovieAverage, the highest average rated movies are
Movie ID
7485
7230
14961
Average Rating
4.74 4.72 4.72
Difficulties in Project:
Setting up spark on the local computer was difficult as due to 32 bit version libraries of hadoop that were causing issues in running of code. Also incompatible java version also caused issues as spark can only run on java 8 and java 11. Java 11 was installed instead of previously installed java 17 to resolve the second issue while 64 bit hadoop libraries were installed to resolve the first issue.
