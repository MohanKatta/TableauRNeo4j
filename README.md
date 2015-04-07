# libraries
require(data.table)
require(reshape)
require(reshape2)
require(bitops)
require(RCurl)
require(RJSONIO)
require(plyr)
require(XLConnect) 

# other definitions
# R workhorse to communicate with Neo4J using the REST interface
query <- function(querystring, verbose = FALSE) {
  h = basicTextGatherer()
  curlPerform(url="http://localhost:7474/db/data/cypher",
              postfields=paste('query',curlEscape(querystring), sep='='),
              writefunction = h$update,
              verbose = verbose
  )           
  result <- fromJSON(h$value())
  data <- data.frame(t(sapply(result$data, unlist)))
  names(data) <- result$columns
  return(data)
}

###################################
# data preparation
###################################
fileName <- "DATA/KOELN_VENUES_DATASET.csv"

# read data and transform features 
dataset <- read.csv2(fileName, colClasses = c(rep("character", 6), 
                                              rep("factor", 1), 
                                              rep("character", 2)), 
                     dec = ",", encoding = "UTF-8")
dataset$CHECKIN_DATE <- as.POSIXct(dataset$CHECKIN_DATE, 
                                   format = "%Y-%m-%d %H:%M:%S")
dataset$LAT <- sub(",", ".", dataset$LAT)
dataset$LNG <- sub(",", ".", dataset$LNG)
dataset$LAT <- as.numeric(dataset$LAT)
dataset$LNG <- as.numeric(dataset$LNG)

# Aggregation using data.table
# COUNT_CHECKINS is our "score" the user gives to a venue
datasetDT <- data.table(dataset)
venueUserDataset <- datasetDT[,
                              list(COUNT_CHECKINS = length(unique(CHECKIN_ID))),
                              by = list(VENUEID, USERID)]
venueUserDataset <- data.frame(venueUserDataset)

venueDataset <- unique(dataset[, c("VENUEID", "LNG", "LAT","VENUE_NAME","CATEGORY_NAME")])

# save as Excel file => input for Tableau
wb = loadWorkbook(paste("DATA/VENUE_LIST_",
                        format(Sys.time(),"%y%m%d_%H%M%S"),".xlsx",sep=""),
                  create = TRUE)
createSheet(wb, "VENUES")
writeWorksheet(wb, venueDataset, "VENUES")
saveWorkbook(wb)

#####################################
# upload data to Neo4j
#####################################

# insert venues
for(i in 1:nrow(venueDataset)){
  q <-paste('CREATE (venue {name:"',venueDataset[i,"VENUEID"],'",txt:"',venueDataset[i,"VENUE_NAME"],
            '",categoryname:"',venueDataset[i,"CATEGORY_NAME"],'",type:"venue",
            lng:',venueDataset[i,"LNG"],', lat:',venueDataset[i,"LAT"],'}) RETURN venue;',sep='')
  data <- query(q)
  }

# insert user
for(i in unique(venueUserDataset$USERID)){
  q <-paste('CREATE (user {name:"',i,'",type:"user"}) RETURN user;',sep='')
  data <- query(q)
}

# add relationships
for(i in 1:nrow(venueUserDataset)){
  q <- paste('START user=node:node_auto_index(name="',venueUserDataset[i,"USERID"],
             '"), venue=node:node_auto_index(name="',venueUserDataset[i,"VENUEID"],
             '") CREATE user-[:RATED {stars : ',venueUserDataset[i,"COUNT_CHECKINS"],'}]->venue;',sep="")
  data <- query(q)
}

# add precalculated magnitude
q <- paste('MATCH (venue)<-[r1]-(user)
  	   WITH venue, sqrt(sum(r1.stars*r1.stars)) as mag
		   SET venue.mag = mag',sep="")
data <- query(q)
