# Title: Elora TD Processing
# Site: ELR
# SiteID: ELR1-QB, ELR1-QA
# Author: Isabella Bowman
# Created: May 04 2025
# Last updated: May 05 2025
# Description: Processing mls port data

# https://github.com/bowmanii
# https://github.com/jkennel

# activate applicable packages
library(data.table)
library(ggplot2)
library(plotly)
library(remotes)
library(readxl)
library(viridis)
library(dplyr)
library(lubridate)

# pull in Kennel's packages if don't already have
# make sure to clear workspace and restart R first
#remotes::install_github("jkennel/rsk")
#remotes::install_github("jkennel/transducer")
#remotes::install_github("jkennel/hydrorecipes")

library(rsk)
library(transducer)
library(hydrorecipes)

###############################################################################
# Q for Kennel:
## 

###############################################################################
#### Constants ####

cm_to_m <- 0.01 # divers record in cm H20, convert to m H20

#well1 <- ELR1-QB (ports 1-5) (1 = deepest)
#well2 <- ELR1-QA (ports 6-10) (10 = shallowest)
#well3 <- ELR2-QA (ports 1-9) (1 = deepest, 9 = shallowest)

# well elevations (m amsl)
#elev1 <- 377.225 # gs elev. not gs elev = 378.421
#elev2 <- 377.446 # gs elev. not gs elev = 378.405
#elev3 <- 402.523 # concrete pad elev.

# mls port elevations
#elev1_1 <- elev1 + 0.987 # port 1 ELR1-QB
#elev1_2 <- elev1 + 0.981 # port 2 ELR1-QB
#elev1_3 <- elev1 + 0.980 # port 3 ELR1-QB
#elev1_4 <- elev1 + 0.978 # port 4 ELR1-QB
#elev1_5 <- elev1 + 0.995 # port 5 ELR1-QB
#elev2_6 <- elev2 + 1.099 # port 6 ELR1-QA
#elev2_7 <- elev2 + 1.116 # port 7 ELR1-QA
#elev2_8 <- elev2 + 1.080 # port 8 ELR1-QA
#elev2_9 <- elev2 + 1.106 # port 9 ELR1-QA
#elev2_10 <- elev2 + 1.106 # port 10 ELR1-QA
#elev3_1 <- elev3 + 0.862 # port 1 ELR2-QA
#elev3_5 <- elev3 + 0.861 # port 5 ELR2-QA
#elev3_9 <- elev3 + 0.861 # port 9 ELR2-QA

# time period
#start <- as.POSIXct("2024-03-31 12:00:00", tz = "UTC") # same as TD
#end <- as.POSIXct("2024-10-22 12:00:00", tz = "UTC") 

###############################################################################
#### Data Manipulation ####

# set where data files are located
file_dir <- "data/"

# assign loc variable to excel file that has port depths, file names, s/n's, etc
# ensure NA in file is read as na in R
loc <- read_xlsx("./metadata/transducer_locations_mls.xlsx", na = "NA")
# create a data.table using the metadata file we just read in
setDT(loc)

# redefine DT to only include for ELR1-QB
#loc <- loc[well == "ELR1-QB"]
#loc <- loc[well %in% c("ELR1-QB", "ELR1-R1")]
#loc <- loc[well == "ELR1-QB" | serial == "R9455"]
#loc <- loc[well == "ELR1-QB" | serial == "R9455" | well == "ELR1-QA"]
loc <- loc[well == "ELR2-QA" | serial == "R9455"]

# list all file names from "data" folder, return full file path
fn <- list.files(file_dir, full.names = TRUE)
# cross reference the data files to the data table file_name column
fn <- fn[basename(fn) %in% loc$file_name]

# create user defined function to read all csv files
read_csv <- function(x) {
  transducer_data <- fread(x, 
                           sep = ",",     # needed?
                           header = TRUE, # does data have a header?
                           skip = 51)     # exclude rows w/out data
  return(transducer_data)
}

# use sapply to create a list, but with the file names (lapply wont keep file names)
# simplify must be false to keep it as list or else it returns as a matrix
#list <- sapply(fn, read_csv, simplify = FALSE, USE.NAMES = TRUE) 

# bind all lists, keeping col of which file it came from
#pr <- rbindlist(list, idcol = TRUE)

# optional choice to combine the above 2 lines of code into 1
pr <- rbindlist(sapply(fn, read_csv, simplify = FALSE, USE.NAMES = TRUE), idcol = TRUE)

# rename columns
colnames(pr) <- c("file_name", "datetime", "value_cm", "temp")

# clean up the id col by taking away "data/"
pr[, file_name := basename(file_name)]

# fix datetime
pr[, datetime := format(strptime(datetime, "%Y/%m/%d %H:%M:%S"), format = "%Y-%m-%d %H:%M:%S")] # corrects format
pr[, datetime := as.POSIXct(datetime, tz = "America/Toronto")] # changes type, sets timezone
pr[, datetime := with_tz(datetime, tzone = "UTC")] # converts to UTC

# remove unneeded col in loc before merging
loc[, c("site", "is_baro", "use") := NULL]
# merge loc and pr dt by the col "file_name"
pr <- loc[pr, on = "file_name"]

# create baro dt
#baro <- pr[serial == "R1111111"]
baro <- pr[port == "baro"]
# discard unneeded cols
baro[, c("well", "serial", "port", "screen_top", "screen_bottom", "monitoring_location") := NULL]
# create wl dt
wl <- pr[!port %in% c("baro")]

# merge baro as a col in wl dt
wl <- baro[, .(datetime, baro_cm = value_cm)][wl, on = "datetime", nomatch = NA]

# convert all pressures to m H20
wl[, baro := baro_cm * cm_to_m]
wl[, value := value_cm * cm_to_m]

# calculate elevation of transducer monitoring point
#wl[, sensor_elev2 := (elev1 + stickup) - monitoring_location]
wl[, sensor_elev := (elev + stickup) - monitoring_location]
# calculate head
wl[, head_masl := sensor_elev + (value - baro)]

# sort dt by date time (ascending order)
setkey(wl, datetime)

###############################################################################
#### Data Subsets ####

wl_sub <- wl

###############################################################################
#### Plots ####

p_wl <- plot_ly(wl_sub,
                x = ~datetime,
                y = ~head_masl, #or head_masl, or value_m, value_adj, 
                #head_masl_cf_air, head_masl_cf_man, etc
                color = ~port,
                colors = plasma(9),
                name = ~port,
                type = "scatter", mode = "lines")

# plot baro
p_baro <- plot_ly(wl_sub,
                  x = ~datetime,
                  y = ~baro,
                  line = list(color = "#ee8326"),
                  name = "Baro",
                  type = "scatter", mode = "lines")

###############################################################################
#### Subplots ####

s0 <- subplot(p_wl, p_baro, shareX = TRUE, nrows = 2, heights = c(0.7, 0.3))%>%
  layout(
    title = list(text = "ELR2-QA: MLS Ports", # ELR1-QA, ELR1-QB, ELR1-QA/QB, ELR2-QA
                 y = 0.98,
                 font = list(size = 18)), 
    xaxis = list(title = "Date and time",
                 nticks = 20,
                 tickangle = -45),
    yaxis = list(title = "Head (m asl)"), 
                 #range = c(367, 373.5)), 
    yaxis2 = list(title = "Pressure (m H20)"), # Δ Pressure (m H20)
    legend = list(traceorder = "reversed")
  )


#interpolate <- approx(wl_sub$datetime, wl_sub$baro)


