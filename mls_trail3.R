# Title: Elora TD Processing
# Site: ELR
# SiteID: ELR1-QB, ELR1-QA
# Author: Isabella Bowman
# Created: May 17 2025
# Last updated: May 22 2025
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
dbar_to_m <- 1.0199773339984 # rbr data reads pressure in dbar, convert to m of H20

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

# pumping data for sealed holes
# 2023 data. Waiting for 2024 data
cw_pump_start1 <- as.POSIXct("2024-03-29 21:00:00", tz = "UTC")
cw_pump_end1 <- as.POSIXct("2024-10-22 18:00:00", tz = "UTC")

# climate data
cw_rain_start1 <- as.POSIXct("2024-03-29 21:00:00", tz = "UTC")
cw_rain_end1 <- as.POSIXct("2024-10-22 18:00:00", tz = "UTC")

# for blended calibration
# manual wl last recorded data for ports 1-5
manual_well1 <- as.POSIXct("2024-10-22 16:40:00", tz = "UTC")
manual_wl1 <- 368.832
manual_wl2 <- 368.860
manual_wl3 <- 369.112
manual_wl4 <- 369.799
manual_wl5 <- 370.066
manual_well2 <- as.POSIXct("2024-10-22 16:10:00", tz = "UTC")
manual_wl6 <- 370.494
manual_wl7 <- 371.060
manual_wl8 <- 370.652
manual_wl9 <- 373.851
manual_wl10 <- 374.528

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
#loc <- loc[well == "ELR2-QA" | serial == "R9455"]
#loc <- loc[well == "ELR1-QA" | serial == "213655"]
loc <- loc[well == "ELR1-QB" | serial == "213655" | well == "ELR1-QA"]

# read in centre wellington data (9 sheets), specify sheet, rows to skip
cw_e4 <- read_xlsx("./data/cw_wells.xlsx", sheet = "Well E4", skip = 2)
setDT(cw_e4)
# assign column headers to dt
colnames(cw_e4) <- c("time", "flow", "drawdown", "waterlevel", "comments")
# take difference between hourly flow rate (they are cumulative daily), reset every 24hrs
cw_e4[, flow_hrly_avg := ifelse((as.numeric(time) %% 86400) == 0, flow, flow - lag(flow))]
# overwrite timezone to EST/EDT (read_xlsx auto assumes UTC, this is wrong in this case)
cw_e4[, datetime := force_tz(time, tzone = "America/Toronto")]
# convert EDT/EST time zones to UTC, will auto adjust for time shifts
cw_e4[, datetime_utc := with_tz(datetime, tzone = "UTC")]
# clean up dt - remove unnecessary columns
cw_e4[, c("comments", "datetime") := NULL]
# subset data (for memory and performance), keep desired cols and pump data by desired times
cw_e4_sub <- cw_e4[, .(datetime_utc, flow_hrly_avg)][datetime_utc %between% c(cw_pump_start1, cw_pump_end1)]
# remove larger dt
#cw_e4 <- NULL # clean up memory

# precipitation data - monthly files - Elora RCS
# read in files (file paths) using the data dir and subsetting by csv files only
fp <- list.files(file_dir, full.names = TRUE, pattern = "*.csv")
# can also subset by characters in file name (if other csv's present)
#fp2 <- list.files(file_dir, full.names = TRUE, pattern = "Elora_RCS")
# read data (individually)
rd <- lapply(fp, fread, fill = TRUE)
# combine data into one data table
rcs <- rbindlist(rd)
# convert datetime column from char to POSIxct class type
rcs[, datetime := as.POSIXct(`Date/Time (UTC)`, format = "%Y-%m-%d %H:%M", tz = "UTC")]
# clean up dt - remove empty cols
rcs[, c("Temp Flag", "Dew Point Temp Flag", "Rel Hum Flag", "Precip. Amount Flag", 
        "Wind Dir Flag", "Wind Spd Flag", "Visibility (km)", "Visibility Flag", 
        "Stn Press Flag", "Hmdx Flag", "Wind Chill Flag") := NULL]
# subset data by cols and times
rcs_sub <- rcs[, .(datetime, `Precip. Amount (mm)`)][datetime %between% c(cw_rain_start1, cw_rain_end1)]
# clean up memory by setting rd, rcs to null
#rd <- NULL
#rcs <- NULL

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
#pr <- rbindlist(sapply(fn[c(1:7, 10:19)], read_csv, simplify = FALSE, USE.NAMES = TRUE), idcol = TRUE) #QB
#pr <- rbindlist(sapply(fn[c(1:5, 8:16)], read_csv, simplify = FALSE, USE.NAMES = TRUE), idcol = TRUE) #QA
pr <- rbindlist(sapply(fn[c(1:12, 15:33)], read_csv, simplify = FALSE, USE.NAMES = TRUE), idcol = TRUE) #QA/QB

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

# read RBR
pr_b <- rsk::read_rsk(fn[c(13:14)], # 8:9, 6:7, 13:14
                      return_data_table = TRUE,
                      include_params = c('file_name'),
                      simplify_names = TRUE,
                      keep_raw = FALSE,
                      raw = TRUE)
# only have pressure variable
pr_b <- pr_b[variable %in% c("pressure")]
# clean up dt: file names, cols
pr_b[, file_name := basename(file_name)]
pr_b[, c("variable") := NULL]
# convert to m H20
pr_b[, value_m := value * dbar_to_m]

# create wl dt
wl <- pr[!port %in% c("baro")]

# merge baro as a col in wl dt
wl <- pr_b[, .(datetime, baro = value_m)][wl, on = "datetime", nomatch = NA]

# convert all pressures to m H20
wl[, value := value_cm * cm_to_m]

# calculate elevation of transducer monitoring point
#wl[, sensor_elev2 := (elev1 + stickup) - monitoring_location]
wl[, sensor_elev := (elev + stickup) - monitoring_location]
# calculate head
wl[, head_masl := sensor_elev + (value - baro)]

# sort dt by date time (ascending order)
setkey(wl, datetime)

# get correction factors 
#wl_cf1 <- wl[datetime %in% c(manual_well1, manual_well2)]
wl_cf1 <- wl[datetime == manual_well1]
wl_cf2 <- wl[datetime == manual_well2]
wl_cf2 <- wl_cf2[port %in% c("06", "07", "08", "09", "10")]
wl_cf <- rbind(wl_cf1, wl_cf2)
dtw <- read_xlsx("./metadata/manual_dtw.xlsx")
setDT(dtw)
wl_cf <- dtw[, .(port, manual_wl = wl)][wl_cf, on = "port"]
wl_cf[, cf := manual_wl - head_masl]
# bring cf's into wl dt
wl <- wl_cf[, .(serial, cf = cf)][wl, on = "serial"]

# wl corrected
wl[, head_masl_cf := head_masl + cf]

#manual_well3 <- as.POSIXct("2024-10-22 15:00:00", tz = "UTC")
#wl_cf3 <- wl[datetime == manual_well3]

###############################################################################
#### Data Subsets ####

wl_sub <- wl
wl_sub1 <- wl[port == "01"]
wl_sub2 <- wl[port == "02"]
wl_sub3 <- wl[port == "03"]
wl_sub4 <- wl[port == "04"]
wl_sub5 <- wl[port == "05"]
wl_sub6 <- wl[port == "06"]
wl_sub7 <- wl[port == "07"]
wl_sub8 <- wl[port == "08"]
wl_sub9 <- wl[port == "09"]
wl_sub10 <- wl[port == "10"]

# find outliers
boxplot(wl_sub$head_masl_cf)
# z-score method: how many standard deviations away from the mean
# anyting +3/-3 away from mean exclude
z_score <- scale(wl_sub$head_masl_cf) # standardizing the data
outliers <- wl_sub$head_masl_cf[abs(z_score) > 3]
# this shows that there are 464 outliers
# out of 464: 24 are #s, 440 are NA's

# subset the data to exclude this
upper_limit <- 375.0000
lower_limit <- 368.000
# Assuming `data` is your dataset and `outlier_column` is where the outliers are.
outliers_removed <- subset(wl_sub, head_masl_cf <= upper_limit & head_masl_cf >= lower_limit)

## do it to every port..
boxplot(wl_sub4$head_masl_cf)
z_score <- scale(wl_sub4$head_masl_cf) # standardizing the data
outliers <- wl_sub4$head_masl_cf[abs(z_score) > 3]
upper_limit <- 371.0000
lower_limit <- 370.100
outliers_removed4 <- subset(wl_sub4, head_masl_cf <= upper_limit & head_masl_cf >= lower_limit)

###############################################################################
#### Plots ####

p_wl <- plot_ly(wl_sub,
                x = ~datetime,
                y = ~head_masl_cf, #or head_masl, or value_m, value_adj, 
                #head_masl_cf, head_masl_cf_man, etc
                color = ~port,
                colors = plasma(10), # 5, 10, 9
                name = ~port,
                type = "scatter", mode = "lines")

p_wl1 <- plot_ly(outliers_removed4,
                x = ~datetime,
                y = ~head_masl_cf, #or head_masl, or value_m, value_adj, 
                #head_masl_cf, head_masl_cf_man, etc
                color = ~port,
                colors = plasma(10), # 5, 10, 9
                name = ~port,
                type = "scatter", mode = "lines")

# plot baro
#p_baro <- plot_ly(wl_sub,
#                  x = ~datetime,
#                  y = ~baro,
#                  line = list(color = "#ee8326"),
#                  name = "Baro",
#                  type = "scatter", mode = "lines")
# this one shows weird line at t-profile

# plot baro
p_baro <- plot_ly(pr_b[as.numeric(datetime) %% 300 == 0],
                  x = ~datetime,
                  y = ~value_m,
                  line = list(color = "#ee8326"),
                  name = "Baro",
                  type = "scatter", mode = "lines")

# plot precipitation
p_rain <- plot_ly(rcs_sub,
                  x = ~datetime,
                  y = ~`Precip. Amount (mm)`,
                  marker = list(color = "#cc72b0"),
                  name = "Precipitation",
                  type = "bar")

# plot E4 flow rate
p_cw <- plot_ly(cw_e4_sub,
                x = ~datetime_utc,
                y = ~flow_hrly_avg,
                line = list(color = "#37bac8"),
                name = "E4 Pumping",
                type = "scatter", mode = "lines")

###############################################################################
#### Subplots ####

s0 <- subplot(p_wl, p_baro, shareX = TRUE, nrows = 2, heights = c(0.7, 0.3))%>%
  layout(
    title = list(text = "ELR1-QA/QB: MLS Ports", # ELR1-QA, ELR1-QB, ELR1-QA/QB, ELR2-QA
                 y = 0.98,
                 font = list(size = 18)), 
    xaxis = list(title = "Date and time",
                 nticks = 20,
                 tickangle = -45),
    yaxis = list(title = "Head (m asl)", 
                 range = c(360, 390)), 
    yaxis2 = list(title = "Pressure (m H20)"), # Δ Pressure (m H20)
    legend = list(traceorder = "reversed")
  )

s1 <- subplot(p_wl, p_baro, p_rain, p_cw, shareX = TRUE, nrows = 4, heights = c(0.6, 0.1, 0.15, 0.15))%>%
  layout(
    title = list(text = "ELR1-QA/QB: MLS Ports", # ELR1-QA, ELR1-QB, ELR1-QA/QB, ELR2-QA
                 y = 0.98,
                 font = list(size = 18)), 
    xaxis = list(title = "Date and time",
                 nticks = 20,
                 tickangle = -45),
    yaxis = list(title = "Head (m asl)"), 
                # range = c(368, 375)), 
    yaxis2 = list(title = "Pressure (m H20)"), # Δ Pressure (m H20)
    legend = list(traceorder = "reversed")
  )

#interpolate <- approx(wl_sub$datetime, wl_sub$baro)

###############################################################################
#### Extract Processed Data ####

# create DT for vertical head profiles
#vhp <- wl[datetime %in% as.POSIXct(c("2024-05-12 8:45:00", "2024-05-18 12:25:00"), tz = "UTC")] #old choice
vhp <- wl_sub[datetime %in% as.POSIXct(c("2024-04-05 18:35:00", "2024-04-05 15:50:00",
                                         "2024-05-12 12:45:00", "2024-05-18 18:35:00", 
                                         "2024-05-31 18:15:00", "2024-06-03 18:05:00",
                                         "2024-06-25 21:50:00", "2024-07-09 17:35:00", 
                                         "2024-07-25 14:00:00", "2024-07-25 19:00:00",
                                         "2024-07-25 21:45:00", "2024-09-15 22:15:00",
                                         "2024-09-22 21:00:00"), tz = "UTC")]

# shorten table
vhp <- vhp[, list(datetime, port, head_masl_cf, head_masl)]
write.csv(vhp, "out/ELR1-QA_QB_vhp_v1.csv")

vhp <- wl_sub[datetime %in% as.POSIXct(c("2024-04-05 18:30:00", "2024-04-05 15:50:00",
                                         "2024-05-12 12:40:00", "2024-05-18 18:30:00", 
                                         "2024-05-31 18:10:00", "2024-06-03 18:00:00",
                                         "2024-06-25 21:50:00", "2024-07-09 17:30:00", 
                                         "2024-07-25 14:00:00", "2024-07-25 19:00:00",
                                         "2024-07-25 21:40:00", "2024-09-15 22:10:00",
                                         "2024-09-22 21:00:00"), tz = "UTC")]

# shorten table
vhp <- vhp[, list(datetime, port, head_masl_cf, head_masl)]
write.csv(vhp, "out/ELR1-QA_QB_vhp_v2.csv")

