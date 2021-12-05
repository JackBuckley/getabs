# -------------------------------------------------------------------------
# 
# Title: Functions 
# Purpose: Read ABS data 
# Author: Jack Buckley
# Date: 02/12/2021
#
# -------------------------------------------------------------------------

if(!require("pacman", character.only = T)) install.packages("pacman")

pacman::p_load(
  tidyverse,
  xml2,
  XML,
  httr,
  magrittr, 
  janitor,
  roxygen2
)

# remove old files
rm(list=ls())
gc()



# Get the list of all ABS dataflows -----------------------------------

#' Get Dataflows
#'
#' Returns dataflows currently available from the ABS data API which match the supplied search term.
#' @param search Search term (regular expression). Default returns all dataflows.
#' @param case_sensitive Should the search term be treated as case sensitive, default set to FALSE.
#' @param exact Return only exact matches of the search term, default set to FALSE
#' @return A tibble containing information on the matched dataflows, including their dataset_id, name and description.
#' @examples 
#' # Return all available dataflows
#' all_data <- get_dataflows();
#' 
#' # Return all dataflows which mention the term WPI
#' wage_price_index <- get_dataflows(search = "wpi");
#' 
#' # Return all dataflows which begin with WPI
#' wage_price_index <- get_dataflows(search = "^WPI", case_sensitive = TRUE);
#' 
#' # Return all datasets related to indigenous/aboriginal/torres strait islander Australians.
#' indigenous_data <- get_dataflows(search = "indigenous|aboriginal|torres strait");
#' @export
get_dataflows <- function(search = ".", case_sensitive = FALSE, exact = FALSE){
  
  # download the dataflows from the ABS developer API
  # note - requires saving a temp file in the current working directory
  dataflow_url <- "https://api.data.abs.gov.au/dataflow"
  
  xml2::download_xml(url = dataflow_url, file = "temp.curltmp")
  
  dataflows <- xml2::read_xml("temp.curltmp")
  
  file.remove("temp.curltmp")
  
  dataflows <- XML::xmlParse(dataflows) %>% XML::xmlToList()
  
  num_dataflows <- length(dataflows$Structures$Dataflows)
  
  data <- tibble::tibble()
  
  for (i in 1:num_dataflows){
    
    # read in each codelist and convert to a tidy dataframe
    temp <- dataflows$Structures$Dataflows[[i]]
    
    # get the dataset name
    name <- temp$Name$text
    
    if(is.null(name)) name <- NA_character_
    
    # get the dataset description
    desc <- temp$Description$text
    
    if(is.null(desc)) desc <- NA_character_
    
    # get variable attributes and check that they exist
    attr <- temp$.attrs %>% tibble::enframe() 
    
    if(!is.null(attr)){
      
      attr <- attr %>% 
        tidyr::pivot_wider(names_from = name, values_from = value) %>% 
        janitor::clean_names()
      
      if(!is.null(attr$id)){
        
        attr <- attr %>% dplyr::rename(dataset_id = id)
      }
      
    } else {
      
      attr <-
        tibble::tibble(
          dataset_id = NA_character_,
          agency_id = NA_character_,
          version = NA_character_,
          is_final = NA_character_
        )
    }
    
    # combine data and add it to the main database
    temp <- tibble::tibble(dataset_name = name, description = desc) %>% 
      dplyr::bind_cols(attr)
    
    data <- data %>% dplyr::bind_rows(temp)
  }
  
  data <- data %>% dplyr::bind_rows()
  
  data <- data %>% dplyr::select(dataset_id, dataset_name, description)

  # filter dataflows so that only dataflows which match the regular expression are returned
  data <- data %>% 
    dplyr::filter(
      stringr::str_detect(dataset_id, stringr::regex(search, ignore_case = !case_sensitive)) |
      stringr::str_detect(dataset_name, stringr::regex(search, ignore_case = !case_sensitive)) |
      stringr::str_detect(description, stringr::regex(search, ignore_case = !case_sensitive))
    )
  
  return(data)
}



# Get the data structure for a given dataflow -------------------------

# function returns the structure of a given dataflow - mainly used as a helper function later
get_structure <- function(dataset_id){
  
  temp_url <- 
    stringr::str_c(
      "https://api.data.abs.gov.au/datastructure/ABS/", 
      dataset_id, 
      "?references=children")
  
  xml2::download_xml(url = temp_url, file = "temp.curltmp")
  
  data_struc <- xml2::read_xml("temp.curltmp")
  
  file.remove("temp.curltmp")
  
  data_struc <- XML::xmlParse(data_struc) %>% XML::xmlToList()
  
  data_struc <- 
    data_struc$Structures$DataStructures$DataStructure$DataStructureComponents
  
  # save the number of dimensions, attributes and measures (minus one to avoid the attributes, which we don't need)
  num_dimensions <- length(data_struc[["DimensionList"]]) - 1
  
  num_attr <- length(data_struc[["AttributeList"]]) - 1
  
  num_measures <- length(data_struc[["MeasureList"]]) - 1
  
  data_dim <- tibble::tibble()
  
  for (i in 1:num_dimensions){
    
    # read in each codelist and convert to a tidy dataframe
    temp <- data_struc$DimensionList[[i]]
    
    # get the dataset name
    attr <- temp$.attrs %>% tibble::enframe() %>% tidyr::pivot_wider()
    
    if(!is.null(attr$id)) {
      
      attr <- attr %>% dplyr::rename(attr_id = id) %>% dplyr::select(attr_id, position)
      
    } else {
      
      attr <- tibble::tibble(attr_id = NA_character_, position = NA_real_)
    }
    
    # get the components lists
    local <- temp$LocalRepresentation$Enumeration$Ref %>% 
      tibble::enframe() %>% 
      tidyr::pivot_wider()
    
    if(!is.null(local$id)) {
      
      local <- local %>% dplyr::rename(local_id = id) %>% dplyr::select(local_id)
      
    } else {
      
      local <- tibble::tibble(local_id = NA_character_)
    }
    
    concept <- temp$ConceptIdentity$Ref %>% 
      tibble::enframe() %>% 
      tidyr::pivot_wider()
    
    if(!is.null(concept$id)) {
      
      concept <- concept %>% dplyr::rename(concept_id = id) %>% dplyr::select(concept_id)
      
    } else {
      
      concept <- tibble::tibble(concept_id = NA_character_)
    }
    
    # combine data and add it to the main database
    temp <- dplyr::bind_cols(attr, local) %>% dplyr::bind_cols(concept)
    
    temp <- temp %>% dplyr::mutate(type = "Dimension")
    
    data_dim <- data_dim %>% dplyr::bind_rows(temp)
  }
  
  data_attr <- tibble::tibble()
  
  for (i in 1:num_attr){
    
    # read in each codelist and convert to a tidy dataframe
    temp <- data_struc$AttributeList[[i]]
    
    # get the dataset name
    attr <- temp$.attrs %>% tibble::enframe() %>% tidyr::pivot_wider()
    
    if(!is.null(attr$id)) {
      
      attr <- attr %>% dplyr::rename(attr_id = id) %>% dplyr::select(attr_id)
      
    } else {
      
      attr <- tibble::tibble(attr_id = NA_character_)
    }
    
    # get the components lists
    local <- temp$LocalRepresentation$Enumeration$Ref %>% 
      tibble::enframe() %>% 
      tidyr::pivot_wider() 
    
    if(!is.null(local$id)) {
      
      local <- local %>% 
        dplyr::rename(local_id = id) %>% 
        dplyr::select(local_id)
      
    } else {
      
      local <- tibble::tibble(local_id = NA_character_)
    }
    
    concept <- temp$ConceptIdentity$Ref %>% 
      tibble::enframe() %>% 
      tidyr::pivot_wider()
    
    if(!is.null(concept$id)) {
      
      concept <- concept %>% 
        dplyr::rename(concept_id = id) %>% 
        dplyr::select(concept_id)
      
    } else {
      
      concept <- tibble::tibble(concept_id = NA_character_)
    }
    
    # combine data and add it to the main database
    temp <- dplyr::bind_cols(attr, local) %>% dplyr::bind_cols(concept)
    
    temp <- temp %>% dplyr::mutate(type = "Attribute")
    
    data_attr <- data_attr %>% dplyr::bind_rows(temp)
  }
  
  data_attr <- data_attr %>% dplyr::distinct()
  
  data_measure <- tibble::tibble()
  
  for (i in 1:num_measures){
    
    # read in each codelist and convert to a tidy dataframe
    temp <- data_struc$MeasureList[[i]]
    
    # get the dataset name
    attr <- temp$.attrs %>% tibble::enframe() %>% tidyr::pivot_wider()
    
    if(!is.null(attr$id)) {
      
      attr <- attr %>% dplyr::rename(attr_id = id) %>% dplyr::select(attr_id)
      
    } else {
      
      attr <- tibble::tibble(attr_id = NA_character_)
    }
    
    # get the components lists
    local <- temp$LocalRepresentation$Enumeration$Ref %>% tibble::enframe() %>% tidyr::pivot_wider() 
    
    if(!is.null(local$id)) {
      
      local <- local %>% dplyr::rename(local_id = id) %>% dplyr::select(local_id)
      
    } else {
      
      local <- tibble::tibble(local_id = NA_character_)
    }
    
    concept <- temp$ConceptIdentity$Ref %>% tibble::enframe() %>% tidyr::pivot_wider()
    
    if(!is.null(concept$id)) {
      
      concept <- concept %>% dplyr::rename(concept_id = id) %>% dplyr::select(concept_id)
      
    } else {
      
      concept <- tibble::tibble(concept_id = NA_character_)
    }
    
    # combine data and add it to the main database
    temp <- dplyr::bind_cols(attr, local) %>% dplyr::bind_cols(concept)
    
    temp <- temp %>% dplyr::mutate(type = "Measure")
    
    data_measure <- data_measure %>% dplyr::bind_rows(temp)
  }
  
  data <- dplyr::bind_rows(data_dim, data_attr, data_measure)
  
  return(data)
}



# Get the code list for a given dataflow ------------------------------

#' Get Codelist
#'
#' Returns the codelist of variables and variable levels for the selected dataflow.
#' @param dataset_id The dataset ID of the dataflow you wish to get a codelist for. 
#' Dataset IDs can be found using get_dataflows(). 
#' @param values Should the returned codelist include only variables ("vars"), 
#' or variables and variable levels ("levels"). Default is set to return both variables and levels.
#' @return A tibble with the available variables names, types and levels. 
#' @examples 
#' # Return the Codelist for the Apparent Consumption of Alcohol dataflow (dataset_id = "ALC")
#' codes <- get_codelist(dataset_id = "ALC");
#' 
#' # Only retrieve variable names
#' var_codes <- get_codelist(values = "vars");
#' @export
get_codelist <- function(dataset_id, values = "levels"){
  
  # test input
  # if(is.null(dataset_id))
  
  # download the codelist XML file
  codelist_url <-
    stringr::str_c(
      "https://api.data.abs.gov.au/datastructure/ABS/",
      dataset_id,
      "?references=codelist"
    )
  
  xml2::download_xml(url = codelist_url, file = "temp.curltmp")
  
  data_struc <- xml2::read_xml("temp.curltmp")
  
  file.remove("temp.curltmp")
  
  codelist <- XML::xmlParse(data_struc) %>% XML::xmlToList()
  
  # read in the number of codelists (variables) we have
  num_codelist <- length(codelist$Structures$Codelists)
  
  var_codes <-
    tibble::tibble(
      var_id = character(),
      var_name = character(),
      level_id = character(),
      level_name = character(),
      agency_id = character(),
      version = character(),
      is_final = character()
    )
  
  for (i in 1:num_codelist){
    
    # read in each codelist and convert to a tidy dataframe
    temp_codelist <- codelist$Structures$Codelists[[i]]
    
    # get the variable names
    temp_var_name <- temp_codelist$Name$text
    
    # get variable attributes
    var_attr <- temp_codelist$.attrs %>% tibble::enframe() 
    
    var_attr <- var_attr %>% 
      tidyr::pivot_wider(names_from = name, values_from = value) %>% 
      janitor::clean_names() %>% 
      dplyr::rename(var_id = id) %>% 
      dplyr::mutate(var_name = temp_var_name)
    
    # get the number of levels for the variable
    num_codes <- length(temp_codelist) - 2
    
    levels <-
      tibble::tibble(var_name = character(),
             level_name = character(),
             level_id = character())
    
    # get the codes for each variable level
    for(j in 1:num_codes){
      
      temp_code <- temp_codelist[[j + 1]]
      
      lvl_name <- temp_code$Name$text
      
      lvl_id <- temp_code$.attrs
      
      temp_data <- tibble::tibble(level_name = lvl_name, level_id = lvl_id, var_name = temp_var_name)
      
      levels <- levels %>% dplyr::bind_rows(temp_data)
    }
    
    levels <- levels %>% dplyr::left_join(var_attr, by = "var_name")
    
    var_codes <- var_codes %>% dplyr::bind_rows(levels)
  }
  
  if(values == "vars"){
    
    var_codes <- var_codes %>% 
      dplyr::distinct(var_id, var_name) %>% 
      dplyr::mutate(dataset_id = dataset_id)
    
  } else if(values == "levels"){
    
    var_codes <- var_codes %>% 
      dplyr::distinct(var_id, var_name, level_id, level_name) %>% 
      dplyr::mutate(dataset_id = dataset_id)
  }
  
  var_codes <- var_codes %>% dplyr::select(dataset_id, dplyr::everything())
  
  # add variable types
  structure <- get_structure(dataset_id)
  
  structure <- structure %>% 
    dplyr::mutate(local_id = dplyr::if_else(is.na(local_id), attr_id, local_id)) %>% 
    dplyr::select(local_id, type)
  
  var_codes <- var_codes %>% dplyr::left_join(structure, by = c("var_id" = "local_id"))
  
  return(var_codes)
}



# Get the data for a given dataflow -----------------------------------

# helper function to returns a valid ABS data API request url
get_url <- function(dataset_id, start_date, end_date, filters){
  
  data_url <-
    stringr::str_c("https://api.data.abs.gov.au/data/",
          dataset_id,
          "/?format=csv")
  
  # # testing of the whether the inputs are valid
  # 
  # # get the data structure
  # 
  # 
  # # check if 
  # if(is.null(filters)){
  #   
  # 
  #   
  # } else {
  #   
  #   
  # }
  
  return(data_url)
}

#' Get Data
#'
#' Returns the dataset for the selected dataflow.
#' @param dataset_id The dataset ID of the dataflow you wish to get a codelist for. 
#' Dataset IDs can be found using get_dataflows(). 
#' @param start_date Optional parameter, refines the search to only include data after the provided start date (to be implemented).
#' @param end_date Optional parameter, refines the search to only include data before the provided end date (to be implemented).
#' @param add_labels Should additional variable level label columns be included in the output. Default is TRUE.
#' @param filters Additional filter for other dimensions of the data (to be implemented).
#' @return A tibble with the data from the dataset. 
#' @examples 
#' # Return the Codelist for the Apparent Consumption of Alcohol dataflow (dataset_id = "ALC")
#' codes <- get_codelist(dataset_id = "ALC");
#' 
#' # Only retrieve variable names
#' var_codes <- get_codelist(values = "vars");
#' @export
get_data <-
  function(
    dataset_id,
    start_date = NA_real_,
    end_date = NA_real_,
    add_labels = TRUE,
    filters = NULL
  ) {
    
    data <- httr::GET(url = get_url(dataset_id, start_date, end_date, filters))
    
    data <- readr::read_csv(httr::content(data))
    
    if (!add_labels) {
      
      data <- data %>% janitor::clean_names()
      
    } else {
      
      # get data structure
      structure <- get_structure(dataset_id)
      
      structure <- structure %>% 
        dplyr::select(attr_id, local_id) %>% 
        dplyr::mutate(local_id = dplyr::if_else(is.na(local_id), attr_id, local_id))  
      
      # convert data to long form for ease of updating level names
      data <- data %>%
        dplyr::mutate(
          dplyr::across(-c(DATAFLOW), ~ as.character(.)),
          row_id = dplyr::row_number()
        ) %>%
        tidyr::pivot_longer(
          cols = -c(DATAFLOW, row_id),
          names_to = "var",
          values_to = "value"
        )

      # add codes
      codes <- get_codelist(dataset_id, values = "levels")
      
      codes <- codes %>% dplyr::left_join(structure, by = c("var_id" = "local_id"))
      
      # add data variable names back to the codes
      codes <- codes %>% dplyr::select(attr_id, level_id, level_name)
      
      # add the codes to the data
      data <- data %>% 
        dplyr::left_join(codes, by = c("var" = "attr_id", "value" = "level_id"))
      
      data_val <- data %>% dplyr::select(DATAFLOW, row_id, var, value)
      data_label <- data %>% dplyr::select(DATAFLOW, row_id, var, level_name)
      
      data_val <- data_val %>% tidyr::pivot_wider(names_from = var, values_from = value)
      data_label <- data_label %>% tidyr::pivot_wider(names_from = var, values_from = level_name)
      
      data_label <- data_label %>% dplyr::select(-dplyr::any_of(c("OBS_VALUE", "OBS_COMMENT")))
      
      data_label <- data_label %>%
        janitor::clean_names() %>%
        dplyr::rename_at(dplyr::vars(-c(dataflow, row_id)), ~ stringr::str_c(., "_label"))
      
      data <- data_val %>%
        janitor::clean_names() %>%
        dplyr::left_join(data_label, by = c("dataflow", "row_id")) %>%
        dplyr::select(-row_id)
    }
    
    return(data)
  }

