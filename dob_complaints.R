---
title: "Pull Open Data Sets"
output: html_notebook
---

## **Load Your Assets**
### Libraries
```{r}

library(RSocrata)
library(dplyr)
library(soql)
library(lubridate)
library(readxl)
library(rBES)

```

### Reference Assets
```{r}
complaint_cats <- read.delim("~/Data Enhancements/DOB_Complaint_Type_Codes1.txt", header = TRUE, sep = "\t")

boro_x_zip <- read_excel("~/Data Enhancements/MWBE/NYCZips.xlsx", sheet = "zip_boro_r")
```

## **Get the Data**
### Build the main URL components
```{r}
resource_id <- "ebb7-mvp5"
nyc_main_url <- paste0("https://data.cityofnewyork.us/resource/",resource_id,".json") 
app_token <- "NxA3gCD1g81xcussE7S2VX3l5"
```

### Build the Queries
#### DOB Complaints
```{r}
query <- soql() %>%
  soql_add_endpoint(nyc_main_url) %>%
  soql_limit(10000) %>%
  soql_select(paste(
    "complaint_number",
    "status",
    "date_entered",
    "house_number",
    "zip_code",
    "house_street",
    "bin",
    "community_board",
    "special_district",
    "complaint_category",
    "unit",
    "disposition_date",
    "disposition_code",
    "inspection_date",
    "dobrundate",
    sep = ","
    )) %>%
  soql_order("date_entered", desc=TRUE) %>%
  soql_where(paste0("date_entered like ","'%252019%25'")) %>%
  as.character()

#DOB dates are, for some reason, stored as text, so sys.date() - 365 not an option to get last year
```

#### DSNY Tonnage
```{r}
query <- soql() %>%
  soql_add_endpoint(nyc_main_url) %>%
  soql_limit(10000) %>%
  soql_select(paste(
    "month",
    "borough",
    "refusetonscollected",
    "papertonscollected"
  ))
```


### Make the Call to Socrata
```{r}
df <- read.socrata(query, app_token = app_token)
```

## **Transform the Data**
### Deduplicate if Needed
```{r}
df1 <- distinct(.data = df, complaint_number, .keep_all = TRUE)
```

### Make Subs
```{r}
df1 <- merge(df1, complaint_cats, by = 'complaint_category', all.x = TRUE)
df1 <- merge(df1, boro_x_zip, by = 'zip_code', all.x = TRUE)
```

### Merge Columns if Needed
```{r}
df1$full_street_address <- paste(df1$house_number, df1$house_street, sep = " ")
```

### Geocode
```{r}
geocode_fields <- c('F1E.longitude','F1E.latitude','F1E.com_schl_dist','F1E.city_council','F1E.output.hse_nbr_disp',
                         'F1A.addr_range_1ax1.st_name','F1E.USPS_city_name','F1E.zip_code','F1E.output.boro_name',
                         'F1E.output.bin','F1E.output.bbl','F1E.com_dist','F1E.nta','F1A.RPAD','F1A.mh_ri_flag',
                         'F1E.output.ret_code','F1E.output.msg','GC.version')

source_fields <- c('complaint_number','status','date_entered','complaint_category','unit','disposition_date','disposition_code','inspection_date','dobrundate')

df2 <- NYC.CleanGeoZip(in_df = df1, id_colname="complaint_number", addr1_colname="full_street_address", addr2_colname=NULL,city_colname = "boro",
                                  zip_colname="zip_code", source_cols=source_fields, geocode_fields=geocode_fields,GBAT_name="18A")
```

## **Write the Data Out**
### Write to Excel for Now; SQL Later
```{r}
writexl::write_xlsx(df2, "dob_complaints.xlsx")
```

