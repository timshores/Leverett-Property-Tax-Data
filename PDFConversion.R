
# Section 1 infrastructure ------------

my_packages <- c("tidyverse", 
                 "broom", 
                 "fs", 
                 "pdftools", 
                 "tidytext",
                 "writexl") # create vector of packages
invisible(lapply(my_packages, require, character.only = TRUE)) # load multiple packages

  # creates fs_path vector of each file in that directory
projectFiles <- dir_ls('./') 

# Section 2, preparing FY25 report --------------

  # Fetch FY25 PDF files from vector.
projectFilesFY25 <- projectFiles[grepl('\\FY25.pdf$', projectFiles)]

projectFilesFY25_list <- projectFilesFY25 %>%
  map(pdf_data) 
  # purrr function that applies pdf_data reading function 
  # to each file element of fs_path vector
  # ... map creates a list of lists of tibbles 
  # (a tibble for each page of each pdf)
  # https://www.r-bloggers.com/2018/12/pdftools-2-0-powerful-pdf-text-extraction-tools/
  # the 'space' column indicates a space separator between text boxes
  # a likely clue that a single column has been separated into multiple df values

  # now that I've consumed my files 
  # I can str_replace to get town and year info from each filename.

names(projectFilesFY25_list) <- projectFilesFY25

  # projectFilesFY25_list structure is:
    # a list 
      # of 183 lists (one for each page)
        # 1 dataframe per page 
  # list[[list]][[tibble]]

# Section 3, flatten list (might be reusable) --------------

  # flatten list: stamps each row with source page number and filename
  # (note that filename is the same for all rows of 
  # a data set that comes from one file)

flatList <- list()
for (i in seq_along(projectFilesFY25_list)) { 
    # seq_along(projectFilesFY25_list) creates 
    # vector with a number value for each list element
  for (j in seq_along(projectFilesFY25_list[[i]])) {
    projectFilesFY25_list[[i]][[j]] <- mutate(projectFilesFY25_list[[i]][[j]], page_num = j)
  }
  flatList <- append(flatList, map2(projectFilesFY25_list[[i]], 
                                    names(projectFilesFY25_list[i]), 
                                    ~.x %>% 
                                      mutate(filename = .y)
                                    )
                     )
  }

# this bind_rows takes all of the df observations 
# from all of the list elements and binds them together in a single df.
 
fullDF <- flatList %>%
  bind_rows() %>% 
  filter(y >= 89 & y < 742)   # omit header and footer

# Section 4, FY25 coordinate fixes --------------
  
  # When the parcel info field on the original report
  # gets too close to the right edge, map(pdf_data) seems to interpret 
  # a space at the end of the string. In the concatDF mutation below, 
  # this results in several parcel address strings jammed together with 
  # assessment type strings from the next column.
  # this preempts that problem by changing the value of space from TRUE to
  # FALSE where the end of the parcel address string hits x == 283, a
  # threshhold coordinate on this report.
fullDF$space[fullDF$x + fullDF$width == 283] <- FALSE
  # Fix two specific cases where slight misalignment in the source pdf
  # caused separation of Book/Page values
fullDF$space[fullDF$page_num == 89 & fullDF$x == 186 & fullDF$y == 429] <- TRUE
fullDF$space[fullDF$page_num == 103 & fullDF$x == 186 & fullDF$y == 434] <- TRUE

# Section 5, grouping and widening (might be reusable) --------------

concatDF <- fullDF %>%
  mutate(group = cumsum(lag(!space, default = TRUE))) %>% # Increment group when space is FALSE
  group_by(group) %>%
  summarise(
    text = paste(text, collapse = " "),
    width = sum(width),
    height = first(height),
    x = first(x),
    y = first(y),
    eof = x + width,
    page_num = first(page_num),
    filename = first(filename),
    .groups = "drop"
  ) %>%
  select(-group) %>%  # Remove the grouping column
  #select(as.character(sort(as.numeric(names(.)))))
  arrange(page_num, y, x) # Sort rows by coordinates

# widen by turning x coordinates into column names
# this replicates the format of the original report, 
# albeit with staggered lines that we need to realign further down.

wideDF <- concatDF %>% 
  pivot_wider(
    id_cols = c(filename, page_num, y), # ensures one row per y coordinate per page
    names_from = x, 
    values_from = text) %>% 
  select(page_num, ######### sort x coordinate columns
         y, 
         as.character(
           sort(as.numeric(
               setdiff(names(.), c("page_num", "y", "filename"))))), 
         filename)

  # now we have a df that is simply x:y coordinates of the page
  # time to smush cols together, yay smush!

# Section 6, more FY25 coordinate fixes --------------

  # Remove the y = 89 row from original data
headingDF <- wideDF %>% filter(y != 89)

ownerCols <- c("78", "85", "86", "113")
parcelCols <- c("186", "187", "208", "232")
typeCols <- c("292", "337")
assessedCols <- c("338", "344", "348", "352", "358", "362", "365", "392")
taxableCols <- c("396", "401", "405", "409", "415", "419", "422")
assessmentCols <- c("439", "470")
committedCols <- c("519", "523", "530", "534", "539")

smushDF <- headingDF %>%
  unite("owner", 
        all_of(ownerCols),
        sep = " ",
        na.rm = TRUE,   # remove NAs
        remove = TRUE)  %>% # remove original columns
  unite("parcel",   # rinse, repeat
        all_of(parcelCols),
        sep = " ",
        na.rm = TRUE,
        remove = TRUE) %>%
  unite("type",   # rinse, repeat
        all_of(typeCols),
        sep = " ",
        na.rm = TRUE,
        remove = TRUE) %>%
  unite("assessed",   # rinse, repeat
        all_of(assessedCols),
        sep = " ",
        na.rm = TRUE,
        remove = TRUE) %>%
  unite("taxable",   # rinse, repeat
        all_of(taxableCols),
        sep = " ",
        na.rm = TRUE,
        remove = TRUE) %>%
  unite("assessment",   # rinse, repeat
        all_of(assessmentCols),
        sep = " ",
        na.rm = TRUE,
        remove = TRUE) %>%
  unite("committed",   # rinse, repeat
        all_of(committedCols),
        sep = " ",
        na.rm = TRUE,
        remove = TRUE)

# Section 7, FY25 Brute fix functions --------------

  # parcel ID and assessment = LAND are sometimes misaligned by a hair
  # causing two df rows instead of one
  # this identifies those pairs by matching two patterns, A and B
  # and writes parcel ID found in B to the NA parcel field in A
  # 
  # step1: Create mapping for ABAB pattern rows
create_parcel_mapping <- function(df) {
  # Identify rows matching the ABAB pattern
  pattern_rows <- which(
    (!is.na(df$parcel) & df$type == "") | 
      (is.na(df$parcel) & df$type == "LAND")
  )
  
  # Create a subset of the data for the pattern rows
  pairs_df <- data.frame(
    row_index = pattern_rows,
    parcel = df$parcel[pattern_rows],
    type = df$type[pattern_rows]
  )
  
  # Create mapping from B to A
  mapping <- data.frame(
    row_a = pairs_df$row_index[seq(1, nrow(pairs_df), 2)],
    parcel_value = pairs_df$parcel[seq(2, nrow(pairs_df), 2)]
  ) 
  
  return(mapping)

}

  # Update the NA parcel values in row A using the mapping
update_parcels <- function(df, mapping) {
  
  df_updated <- df
  
    # Update the parcel values in row A

  df_updated$parcel[mapping$row_a] <- mapping$parcel_value
  
  return(df_updated)

}

  # Validate that the parcel values in each pair match
validate_updates <- function(df, mapping) {
    # Extract row numbers for A and B from the mapping
    row_a <- mapping$row_a
    row_b <- row_a + 1  # Assuming B rows are always directly after A rows
    
    # Check if parcel values in row A and row B are identical
    all(df$parcel[row_a] == df$parcel[row_b])
}


# Section 8, Parse FY25 fields (might be reusable) --------------

  # Owner part 1
  # 
  # Step1: Pull that numeral, which may or may not be a report record number
  # (some numbers in the sequence are skipped in the source report)
  # regex for field with nothing but numerals
  # not beginning with 0 (to avoid zips)
  # It aligns reliably with the parcel ID field
parseOwnerDF <- smushDF %>%
  mutate(
    report_row = if_else(   # new field with just the report row numbers
      str_detect(owner, "^[1-9]\\d*$") & !is.na(parcel) & trimws(parcel) != "",
      owner,
      NA_character_
      ),
    owner = if_else(     # remove the report row numbers from the owner field
      str_detect(owner, "^[1-9]\\d*$") & !is.na(parcel) & trimws(parcel) != "",
      NA_character_,
      owner
    ))

  # step2: parse parcel info by reorganizing columns
parseOwnerDF <- parseOwnerDF %>%
  relocate(report_row, .after = 2)

  # Parcel
  #   # note for parcel parsing,
      # Parcel ID patterns
      # #-##
      # #-#
      # #A-##
      # #A-#
      # #A-##A
      # #A-###
      # #-###
      # #-###A  
  # step1: parse parcel info into separate cols
parseParcelDF <- parseOwnerDF %>%
  mutate(parcel_address = str_remove(parcel, "^[0-9][A-Z]?-.+")) %>%
  mutate(book_page = str_remove(parcel, "Book/Page:")) %>%
  mutate(deed_date = str_remove(parcel, "Deed Date:")) %>%
  mutate(acres = str_remove(parcel, "Acres"))

  # step2: parse parcel info by reorganizing columns
parseParcelDF <- parseParcelDF %>%
  relocate(parcel_address, book_page, deed_date, acres, .after = 5)

  # step3: parse parcel info by removing unnecessary values
parseParcelDF <- parseParcelDF %>%
  mutate(parcel_address =
           if_else(str_detect(parcel, "Book/Page:|Deed Date:|Acres"),
                   NA_character_,
                   parcel_address)) %>%
  mutate(book_page = 
           if_else(str_detect(parcel, "Book/Page:"), 
                   book_page, 
                   NA_character_)) %>%
  mutate(deed_date = 
           if_else(str_detect(parcel, "Deed Date:"),
                   deed_date,
                   NA_character_)) %>%
  mutate(acres = 
           if_else(str_detect(parcel, "Acres"), 
                   acres, 
                   NA_character_))

  # step4: parse parcel info by tidying parcel column
parseParcelDF <- parseParcelDF %>%
  mutate(parcel = 
           if_else(str_detect(parcel, "^[0-9][A-Z]?-.+"),
                   parcel,
                   NA_character_)) %>%
  mutate(parcel = 
           if_else(str_detect(parcel, "Book/Page:"), 
                   NA_character_,
                   parcel)) %>%
  mutate(parcel = 
           if_else(str_detect(parcel, "Deed Date:"), 
                   NA_character_,
                   parcel)) %>%
  mutate(parcel = 
           if_else(str_detect(parcel, "Acres"), 
                   NA_character_,
                   parcel))

  # step5: Apply the brute fix to mismatched parcel/type pairs
mapping <- create_parcel_mapping(parseParcelDF)
parseParcelDF <- update_parcels(parseParcelDF, mapping)

  # Brute fix validation
if (validate_updates(parseParcelDF, mapping)) {
  print("Updates have been applied correctly. All A/B pairs match.")
} else {
  print("Validation failed. Some A/B pairs do not match.")
}

  #step6 fill parcel IDs and turn white space into NAs
parseParcelDF <- parseParcelDF %>%
  fill(parcel, .direction = "down") %>%
  mutate(across(where(is.character), # turn all blank fields into NA
                ~ifelse(trimws(.) == "", NA, .)))

  # Type
  # step1: parse type info into separate cols
parseTypeDF <- parseParcelDF %>%
  mutate(taxable_land_val = 
           ifelse(type == "LAND", as.numeric(gsub(",", "", taxable)), NA),
         taxable_bldg_val = 
           ifelse(type == "BLDG", as.numeric(gsub(",", "", taxable)), NA),
         taxable_oth_val = 
           ifelse(type == "OTH", as.numeric(gsub(",", "", taxable)), NA))

  # Assessment
  # parse assessment info into separate cols
parseAssessmentDF <- parseTypeDF %>%
  mutate(cpa_due = 
           ifelse(assessment == "CPA", as.numeric(gsub("[$,]", "", committed)), NA),
         tax_due =
           ifelse(assessment == "Tax", as.numeric(gsub("[$,]", "", committed)), NA))

# Section 9, Clean up final FY25 DF --------------

# report_row has some ";"
# 
  # 'owner' and 'parcel_address' fields use collapse = '\n', 
  # and all other fields use collapse = ';' (which is just for QA)
combinedDF <- parseAssessmentDF %>%
  group_by(parcel) %>%
  summarise(across(everything(), 
                   ~paste(unique(.[!is.na(.) & nchar(trimws(.)) > 0]), 
                          collapse = if_else(
                            cur_column() %in% 
                              c("owner", "parcel_address"), "\n", ";")))) %>%
  ungroup() %>%
  mutate(across(c(page_num, report_row, acres, 
                  taxable_land_val, taxable_bldg_val, taxable_oth_val,
                  cpa_due, tax_due), 
                ~ as.numeric(replace_na(as.numeric(.), 0))),
         taxable_total = taxable_land_val + taxable_bldg_val + taxable_oth_val,
         total_due = cpa_due + tax_due)

finalDF <- select(combinedDF,
                  parcel, owner, parcel_address, book_page, deed_date, acres,
                  cpa_due, tax_due, 
                  total_due, 
                  taxable_land_val, taxable_bldg_val, taxable_oth_val, 
                  taxable_total,
                  filename, page_num, report_row)


# summfinalDF <- data.frame(do.call(rbind, 
#                                   lapply(finalDF[, c("acres", 
#                                                      "cpa_due", 
#                                                      "tax_due", 
#                                                      "total_due", 
#                                                      "taxable_land_val",
#                                                      "taxable_bldg_val",
#                                                      "taxable_oth_val",
#                                                      "taxable_total")], 
#                                          summary)))


# Section 10, Create FY25 summaries and exports -------------- 

calculate_summary_df <- function(dataframe, cols) {
  
  # Calculate summary statistics for specified columns
  summary_df <- data.frame(do.call(rbind, 
                                   lapply(dataframe[, cols], 
                                          summary)))
  
  # Calculate totals for specified numeric fields
  totals <- colSums(dataframe[, cols], 
                    na.rm = TRUE)
  
  # Add the totals as a new column
  summary_df$total <- totals

  # calculate standard deviation for specified numeric fields
  std_devs <- sapply(dataframe[, cols], 
                     sd, na.rm = TRUE)
  
  # Add standard deviation as a new column
  summary_df$stdev <- std_devs  

  
  # Perform Shapiro-Wilk test for each numeric field
  normality_tests <- sapply(dataframe[, cols], 
                            function(x) shapiro.test(x)$p.value)
  
  # Add normality results as a new column to summfinalDF
  summary_df$norm_distribution <- normality_tests > 0.05    
  
  # Add numeric column names as a new column
  summary_df <- summary_df %>% 
    rownames_to_column(var = "variable") %>%
    relocate(variable, .before = 1)

  # Return the modified dataframe
  return(summary_df)
}

numeric_cols <- c("acres", 
                  "cpa_due", 
                  "tax_due", 
                  "total_due", 
                  "taxable_land_val",
                  "taxable_bldg_val",
                  "taxable_oth_val",
                  "taxable_total")

summfinalDF <- calculate_summary_df(finalDF, numeric_cols)

# subset specific owner information
npDF <- finalDF %>% filter(str_detect(owner, "NIXON PEABODY"))
summnpDF <- calculate_summary_df(npDF, numeric_cols)

# export
write_xlsx(finalDF, "../export_files/leverett_tax_commitments_fy25.xlsx")
write_xlsx(npDF, "../export_files/nixon_peabody_fy25.xlsx")
write_xlsx(summfinalDF, "../export_files/summary_fy25.xlsx")
write_xlsx(summnpDF, "../export_files/summaryNP_fy25.xlsx")

# Section 11, Prep FY23 and FY24 -------------- 

# Fetch and consume FY23 and FY24 PDF files from vector.
projectFilesFY2324 <- projectFiles[grepl('\\FY2[3-4].pdf$', projectFiles)]

projectFilesFY2324_list <- projectFilesFY2324 %>%
  map(pdf_data) 

names(projectFilesFY2324_list) <- projectFilesFY2324

# projectFilesFY2324_list structure is:
# a list of two lists
# of a large number of lists (one for each page)
# 1 dataframe per page 
# list[[list]][[list]][[tibble]]

# Section 12, flatten FY23 and FY24 -------------- 

# flatten list: stamps each row with source page number and filename
# (note that filename is the same for all rows of 
# a data set that comes from one file)

flatList <- list()
for (i in seq_along(projectFilesFY2324_list)) { 
  # seq_along(projectFilesFY2324_list) creates 
  # vector with a number value for each list element
  for (j in seq_along(projectFilesFY2324_list[[i]])) {
    projectFilesFY2324_list[[i]][[j]] <- mutate(projectFilesFY2324_list[[i]][[j]], page_num = j)
  }
  flatList <- append(flatList, map2(projectFilesFY2324_list[[i]], 
                                    names(projectFilesFY2324_list[i]), 
                                    ~.x %>% 
                                      mutate(filename = .y)
  )
  )
}

# this bind_rows takes all of the df observations 
# from all of the list elements and binds them together in a single df.

fullDF_FY2324 <- flatList %>%
  bind_rows() %>% 
  filter(y >= 89 & y < 742)   # omit header and footer
