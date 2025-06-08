# Title: Elora TD Processing
# Site: ELR
# SiteID: ELR1-QB, ELR1-QA
# Author: Isabella Bowman
# Created: June 08 2025
# Last updated: June 08 2025
# Description: Barometric Efficiency

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
library(collapse)

# pull in Kennel's packages if don't already have
# make sure to clear workspace and restart R first
#remotes::install_github("jkennel/rsk")
#remotes::install_github("jkennel/transducer")
#remotes::install_github("jkennel/hydrorecipes")

library(rsk)
library(transducer)
library(hydrorecipes)

#### read in data ####

# set where data files are located
file_dir <- "data/"

# assign loc variable to excel file that has port depths, file names, s/n's, etc
# ensure NA in file is read as na in R
loc <- read_xlsx("./metadata/transducer_locations_mls.xlsx", na = "NA")
# create a data.table using the metadata file we just read in
setDT(loc)

# redefine DT to only include for ELR1-QB
loc <- loc[well == "ELR1-QB" | serial == "213655" | well == "ELR1-QA"]
#loc <- loc[well == "ELR2-QA" | serial == "213655"]

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

# read data
wl <- rbindlist(sapply(fn[c(6)], read_csv, simplify = FALSE, USE.NAMES = TRUE), idcol = TRUE) #QA/QB
# rename columns
colnames(wl) <- c("file_name", "datetime", "value_cm", "temp")

# read transducer files using Kennel's package
baro <- rsk::read_rsk(fn[c(13)],
                      return_data_table = TRUE,
                      include_params = c('file_name'),
                      simplify_names = TRUE,
                      keep_raw = FALSE,
                      raw = TRUE)[variable == "pressure"]

# clean up filename col by removing file paths
baro[, file_name := basename(file_name)]
wl[, file_name := basename(file_name)]
# fix datetime
wl[, datetime := format(strptime(datetime, "%Y/%m/%d %H:%M:%S"), format = "%Y-%m-%d %H:%M:%S")] # corrects format
wl[, datetime := as.POSIXct(datetime, tz = "America/Toronto")] # changes type, sets timezone
wl[, datetime := with_tz(datetime, tzone = "UTC")] # converts to UTC
# convert all pressures to m H20
cm_to_m <- 0.01 # divers record in cm H20, convert to m H20
wl[, value := value_cm * cm_to_m]
dbar_to_m <- 1.0199773339984 # rbr data reads pressure in dbar, convert to m of H20
baro[, value_m := value * dbar_to_m]

# combine dt's while also calculating the mean value of each
wl_baro <- baro[,.(datetime, baro = value_m - mean(value_m))][wl[, .(datetime, value = value - mean(value))], on = "datetime", nomatch = 0]

# subset date range
# goal = times where there is no pumping, its "stable"
wl_baro <- wl_baro[between(datetime, as.POSIXct("2024-04-17", tz = "UTC"), as.POSIXct("2024-04-24", tz = "UTC"))] #R1-R1_1, R1-R2_1
#wl_baro <- wl_baro[between(datetime, as.POSIXct("2024-07-14", tz = "UTC"), as.POSIXct("2024-07-19", tz = "UTC"))] #R1-R1_2, R1-R2_2

# visualize the data
# dependent variable = wl pressure
# independent variable = baro pressure
# range is 0 to 1, with increments of 0.05, 0.01, etc
hydrorecipes::be_visual(wl_baro,
                        dep = "value", ind = "baro", time = "datetime",
                        be_tests = seq(0, 1.0, 0.01), inverse = FALSE)











