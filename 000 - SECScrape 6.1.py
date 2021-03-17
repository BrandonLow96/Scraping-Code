#########

# Project: Data Scraping
# Author: Ben Rodman
# SMD: Meloria Meschi
# Brief: Write a function or functions to scrape every annual and quarterly report from every company from the SEC website for data
#        All data must be in a format in which data analysis can be applied
# Note: This code is in no way finished. There are several sections that can be defined as individual functions and called upon where needed
#       Creating these smaller functions will help improve legibility, but will probably not improve performance speed, as the limiting factors are bandwidth and data restructuring speed (RAM)

#########


# import libraries
print("Importing Libraries")
import math
import os
import re
import string
import urllib
import collections

import pandas as pd
import requests
import xlsxwriter
from bs4 import BeautifulSoup
import numpy as np

##########

# Basic variables defined

# List to store files that errored out during the conversion stage
doc_check = []

# Directory into which data will be saved and manipulated
data_directory = os.getcwd() + "\\" + "Data Directory"


# Functions initiaised for use in the main function
print("Initialising functions")


def main():
    print("Main Program Initialised")

    # This is the base of the URL that will be used to look through the quarters
    base_url = r"https://www.sec.gov/Archives/edgar/daily-index"
    # Year being searched for filings
    year = "2020"

    # See if data is already gathered, else gather data
    try:
        com_files = pd.read_csv(
            f"com_files_{year}.csv",
            converters={
                "10Ks": lambda x: eval(x),
                "KDates": lambda x: eval(x),
                "10Qs": lambda x: eval(x),
                "QDates": lambda x: eval(x),
                "SIC": lambda x: eval(x),
            },
        )

    except:
        # The SEC daily index files are requested through the SEC master data navigator
        year_links = get_year_links(year, base_url)

        # Find 'master' files for each year. SEC provides three types of .idx files, sorted by 'Company', 'form types' and 'CIK number'.
        # The 'master file for each year sorts by CIK number and is the only file which has any sort of delimiter, allowing us to parse it.
        # A single variable of master_dictionary is created, which is a dictionary of every filing for each company for a year
        master_dictionary = get_master_files(year_links, year)

        # Retrieves the 10-K and 10-Q URLs along with the associated company names and CIK codes
        # The data is stored as a dataframe with the 10K and 10Qs, along with their respective filing dates, stored as lists
        com_files = retrieve_filings(master_dictionary, year)

    base_url = r"https://www.sec.gov"

    input_filing_path = os.getcwd() + "\\" + "Filing Names"

    # Retrieves the filing names and headers found in the files titled 'Default Filing Terms', 'Filing Document Names', 'Scraped Filing Document Names'
    # stores them in a variable titled 'Lists'

    Lists = load_filing_names(input_filing_path)

    terms_list = Lists[0]
    scraped_list = Lists[1]
    headers = Lists[2]
    default_terms = Lists[3]

    scraped_list = parse_filings(
        "10Ks",
        terms_list,
        com_files,
        "KDates",
        base_url,
        scraped_list,
        default_terms,
        headers,
    )
    # scraped_list = parse_filings(
    #     "10Qs",
    #     terms_list,
    #     com_files,
    #     "QDates",
    #     base_url,
    #     scraped_list,
    #     default_terms,
    #     headers,
    # )

    df = pd.DataFrame(scraped_list).transpose()
    df.columns = headers
    df.to_excel(input_filing_path + r"\Scraped Filing Document Names.xlsx")


# Function creating SEC URL from base URL defined
def make_url(base_url, comp):
    url = base_url

    for r in comp:
        url = "{}/{}".format(url, r)

    return url


######

# The SEC daily index files are requested through the SEC master data navigator

######
def get_year_links(year, base_url):
    # This line sets up a link to https://www.sec.gov/Archives/edgar/daily-index/###YEAR VARIABLE###/
    # This is the ###YEAR VARIABLE### page of all the quarterly filing indexes
    year_url = make_url(base_url, [year, "index.json"])

    # Requesting the content for ###YEAR VARIABLE###
    content = requests.get(year_url)
    decoded_content = content.json()

    year_links = []
    # Iterating through a list of quarters
    for item in decoded_content["directory"]["item"]:

        # The daily-index filings are searched
        qtr_url = make_url(base_url, [year, item["name"], "index.json"])

        # New URL requested as json structure
        file_content = requests.get(qtr_url)
        decoded_content = file_content.json()

        # For each file retrieved, the type and URL is stored
        for file in decoded_content["directory"]["item"]:

            file_url = make_url(base_url, [year, item["name"], file["name"]])
            year_links.append(file_url)
    return year_links


######

# Find 'master' files for each year
# A single variable of master_dictionary is created, which is a dictionary of every filing for each company for a year

######


def get_master_files(year_links, year):

    # Creates a directory to hold the master files

    master_file_path = "master_files" + "_" + year

    if not os.path.exists(master_file_path):
        os.makedirs(master_file_path)

    # Links to master files found for each year
    matching = [link for link in year_links if "master" in link]

    # Master dictionary for each year initialised
    master_dictionary = []

    for master in matching:

        print("This is the master file: " + str(master))

        file_url = master
        content = requests.get(file_url).content

        # Master file name created for each master file
        result = re.search("master.(.*).idx", file_url)
        file_name = result.group(1)

        ###########################
        # This section saves a copy of the master file in a more accesible format and in a document
        ###########################

        # Data saved to reduce RAM requirements and increase code speed

        with open(f"{master_file_path}\\{file_name}", "wb") as f:
            f.write(content)

        with open(f"{master_file_path}\\{file_name}", "rb") as f:
            byte_data = f.read()

        # Byte steam decoded, split by '--' which is the header and the rest of the data (the useful data)
        data = byte_data.decode("utf-8", "ignore").split("--")

        data_format = data[-1]

        #############################
        # This section cleans the master file
        #############################

        master_data = []

        clean_item_data = data_format.replace("\n", "|").split("|")

        # Loop through the data list
        for index, row in enumerate(clean_item_data):

            # Loop for when the next txt file is found
            if ".txt" in row:

                # Values for the row retrieved, and indexed specifically to a standard SEC format
                mini_list = clean_item_data[(index - 4) : index + 1]

                if len(mini_list) != 0:
                    mini_list[4] = "https://www.sec.gov/Archives/" + mini_list[4]
                    master_data.append(mini_list)

        ########

        # A dictionary list is created and updated containing all terms relating to the master file

        ########

        for index, document in enumerate(master_data):

            # A dictionary is created for each item in the master list
            document_dict = {}
            document_dict["cik_number"] = document[0]
            document_dict["company_name"] = document[1]
            document_dict["form_id"] = document[2]
            document_dict["date"] = document[3]
            document_dict["file_url"] = document[4]

            master_data[index] = document_dict

            master_dictionary.append(document_dict)
    return master_dictionary


######

# This section retrieves the 10-K and 10-Q URLs along with the associated company names and CIK codes
# The data is stored as a dataframe with the 10K and 10Qs, along with their respective filing dates, stored as lists

######


def retrieve_filings(master_dictionary, year):

    # Initialise the master dataframe
    com_files = pd.DataFrame(
        {
            "Name": [],
            "CIK": [],
            "10Ks": [],
            "KDates": [],
            "10Qs": [],
            "QDates": [],
            "SIC": [],
        }
    )

    for document_dict in master_dictionary:

        # If the document is a 10K or 10Q, a series of checks are then performed
        if document_dict["form_id"] == "10-Q" or document_dict["form_id"] == "10-K":

            # In the event the company isn't listed in the dataframe, a new row is added with the CIK no. and company name
            if document_dict["cik_number"] not in set(com_files["CIK"]):
                com_row = pd.DataFrame(
                    {
                        "Name": [document_dict["company_name"]],
                        "CIK": [document_dict["cik_number"]],
                        "10Ks": [[]],
                        "KDates": [[]],
                        "10Qs": [[]],
                        "QDates": [[]],
                        "SIC": [[]],
                    }
                )
                com_files = pd.concat([com_files, com_row])

                # Index reset each time a company is added to the dataframe
                com_files = com_files.reset_index(drop=True)

            # The 10Q or 10K document URLs are added to the company row along with the corresponding date
            if document_dict["form_id"] == "10-Q":
                ComIndex = com_files.index[
                    com_files["CIK"] == document_dict["cik_number"]
                ].tolist()[0]
                com_files.at[ComIndex, "10Qs"].append(
                    document_dict["file_url"]
                    .replace("-", "")
                    .replace(".txt", "/index.json")
                )
                com_files.at[ComIndex, "QDates"].append(document_dict["date"])

                if len(com_files.at[ComIndex, "SIC"]) == 0:

                    # This gets the SIC webpage
                    sic_url = document_dict["file_url"].replace(".txt", "")
                    sic_url1 = sic_url.split("/")
                    sic_url = (
                        sic_url.replace("-", "")
                        + "/"
                        + sic_url1[-1]
                        + "-index-headers.html"
                    )

                    sic_webpage = requests.get(sic_url).content
                    soup = BeautifulSoup(sic_webpage).get_text()

                    try:
                        match = re.findall(
                            "(?<=STANDARD INDUSTRIAL CLASSIFICATION:\t)[^\n]*", soup
                        )
                        sic_number = re.findall("[\d]+", match[0])
                        sic_number = sic_number[0]
                        com_files.at[ComIndex, "SIC"].append(sic_number)

                    except:
                        com_files.at[ComIndex, "SIC"].append("")

            else:
                print(document_dict["file_url"])
                ComIndex = com_files.index[
                    com_files["CIK"] == document_dict["cik_number"]
                ].tolist()[0]
                com_files.at[ComIndex, "10Ks"].append(
                    document_dict["file_url"]
                    .replace("-", "")
                    .replace(".txt", "/index.json")
                )
                com_files.at[ComIndex, "KDates"].append(document_dict["date"])

                if len(com_files.at[ComIndex, "SIC"]) == 0:

                    # This gets the SIC webpage
                    sic_url = document_dict["file_url"].replace(".txt", "")
                    sic_url1 = sic_url.split("/")
                    sic_url = (
                        sic_url.replace("-", "")
                        + "/"
                        + sic_url1[-1]
                        + "-index-headers.html"
                    )

                    sic_webpage = requests.get(sic_url).content
                    soup = BeautifulSoup(sic_webpage).get_text()

                    # This gets the SIC number and appends it to com_files
                    try:
                        match = re.findall(
                            "(?<=STANDARD INDUSTRIAL CLASSIFICATION:\t)[^\n]*", soup
                        )
                        sic_number = re.findall("[\d]+", match[0])
                        sic_number = sic_number[0]
                        com_files.at[ComIndex, "SIC"].append(sic_number)

                    except:
                        com_files.at[ComIndex, "SIC"].append("")

            # The URL of each filing is adjusted for future indexing to be in the .json format
            document_dict["file_url"] = (
                document_dict["file_url"]
                .replace("-", "")
                .replace(".txt", "/index.json")
            )

    com_files.to_csv(f"com_files_{year}.csv", index=False)
    return com_files


######

# This section iterates through the 10K and 10Q filing URLs and creates corresponding CSV files for four main tables
# The retrieved tables are iterated through to identify whether they fall into the main categories. If true, they are parsed

######


def load_filing_names(filing_path):
    # File loading of the confirmed filing names, and the scraped file names
    # This section hasn't been split into multiple functions to increase legibilty, however, it could be split to reduce number of lines written

    # Filing names retrieved, cleaned and sorted
    File_Doc_names = pd.read_excel(filing_path + r"\Filing Document Names.xlsx")

    headers = File_Doc_names.columns.to_list()

    terms_list = []
    for i in headers:
        terms_list.append(File_Doc_names[i].to_list())

    # nan values removed that occur due to different string lengths
    terms_list = [[x for x in y if str(x) != "nan"] for y in terms_list]
    terms_list = [
        ["".join(c.lower() for c in s if c not in string.punctuation) for s in y]
        for y in terms_list
    ]

    # Scraped filing names retrieved, cleaned and sorted
    Scraped_File_Doc_names = pd.read_excel(
        filing_path + r"\Scraped Filing Document Names.xlsx", index_col=0
    )

    scraped_list = []
    for i in headers:
        scraped_list.append(Scraped_File_Doc_names[i].to_list())

    scraped_list = [[x for x in y if str(x) != "nan"] for y in scraped_list]
    scraped_list = [
        ["".join(c.lower() for c in s if c not in string.punctuation) for s in y]
        for y in scraped_list
    ]

    # Default filing row names and keys
    Default_Doc_Terms = pd.read_excel(filing_path + r"\Default Filing Terms.xlsx")

    default_terms = []
    for i in headers:
        default_terms.append(Default_Doc_Terms[i].to_list())

    default_terms = [[x for x in y if str(x) != "nan"] for y in default_terms]
    default_terms = [
        ["".join(c.lower() for c in s if c not in string.punctuation) for s in y]
        for y in default_terms
    ]

    return terms_list, scraped_list, headers, default_terms


def parse_filings(
    filing_name,
    term_list,
    com_files,
    term_date,
    base_url,
    scraped_list,
    default_terms,
    headers,
):
    print(len(com_files))
    for company in range(0, len(com_files)):
        print("Company " + str(company))
        # Iterate through a companies 10Ks

        # index to restart code if errors out
        if company < 2861:
            continue

        for filing in com_files.at[company, filing_name]:
            print("Filing " + filing)

            # URL requested and json format retrieved
            content = requests.get(filing).json()

            xml_summary = ""

            for file in content["directory"]["item"]:

                # The filing summary url can be used to add terms to the dictionary in the event that a document cannot be found
                if file["name"] == "FilingSummary.xml":

                    xml_summary = (
                        base_url + content["directory"]["name"] + "/" + file["name"]
                    )

            base_url_hold = xml_summary.replace("FilingSummary.xml", "")

            try:

                # Content requested
                content = requests.get(xml_summary).content

            except:
                print(f"{filing} does not contain a FilingSummary.xml page.")
                continue

            # Content parsed
            soup = BeautifulSoup(content, "lxml")

            # The 'myreports' tag contains all the individual reports submitted
            reports = soup.find("myreports")

            # A master list of components is created
            master_reports = []

            # Each 'myreports' tag reports iterated through
            # Based on the SEC website structure, the last item is avoided
            for report in reports.find_all("report")[:-1]:

                # Dictionary for relevant parts created
                report_dict = {}
                report_dict["name_short"] = report.shortname.text
                report_dict["name_long"] = report.longname.text
                report_dict["position"] = report.position.text
                report_dict["category"] = report.menucategory.text
                report_dict["url"] = base_url_hold + report.htmlfilename.text

                # Each dictionary is appended to the master list
                master_reports.append(report_dict)

            # List to hold URLs initialsed
            statements_url = []

            for names in headers:
                best_match_url = []
                # print(names)
                for report_dict in master_reports:

                    # This section lists the key terms and attempts to identify the type of report it is based on the presence of key terms

                    x1 = default_terms[headers.index(names)]
                    url_hold = best_fit_url(
                        master_reports, default_terms[headers.index(names)]
                    )

                    if url_hold == "No match found":
                        statements_url.append(url_hold)
                        break
                    else:
                        print("Best fit algorithm found match for " + str(names))
                        statements_url.append(url_hold["url"])
                        break

            print(statements_url)
            # statements_url.append(best_match_url[0])

            ########################

            # All the statements are assembled into a single dataset

            ########################

            for i in range(0, len(statements_url)):
                if type(statements_url[i]) == list:
                    statements_url[i] = statements_url[i][0]

            statements_data = []

            # Loop through each statement url
            for statement in statements_url:

                if statement != "No match found":
                    # A dictionary is defined that will store the different parts of the statement
                    statement_data = {}
                    statement_data["headers"] = []
                    statement_data["sections"] = []
                    statement_data["data"] = []

                    # Statement file content requested
                    content = requests.get(statement).content
                    report_soup = BeautifulSoup(content, "html")

                    # All rows found and parsed
                    for index, row in enumerate(report_soup.table.find_all("tr")):

                        cols = row.find_all("td")

                        # Statement for a regular row and not section or table header
                        if (
                            len(row.find_all("th")) == 0
                            and len(row.find_all("strong")) == 0
                        ):
                            reg_row = [ele.text.strip() for ele in cols]
                            statement_data["data"].append(reg_row)

                        # Statement for a regular row and a section but not a table header
                        elif (
                            len(row.find_all("th")) == 0
                            and len(row.find_all("strong")) != 0
                        ):
                            sec_row = cols[0].text.strip()
                            statement_data["sections"].append(sec_row)

                        # Statement if none of the above are recognised, therefore it's a table header
                        elif len(row.find_all("th")) != 0:
                            hed_row = [ele.text.strip() for ele in row.find_all("th")]
                            statement_data["headers"].append(hed_row)

                        else:
                            print("We encountered an error.")

                    # Appended to master file
                    statements_data.append(statement_data)
                else:
                    statements_data.append(statement)

                # Data saved in common file

            print("saving data")
            save_data(
                filing_name,
                statements_data,
                com_files,
                term_date,
                company,
                filing,
                headers,
            )
    return scraped_list


def save_data(
    filing_name, statements_data, com_files, term_date, company, filing, headers
):

    for stat_num in range(0, len(statements_data)):
        if statements_data[stat_num] != "No match found":
            # Grab the proper components

            if len(statements_data[stat_num]["headers"]) == 1:
                index_num = 1
            else:
                index_num = 0

            doc_header = statements_data[stat_num]["headers"][
                (len(statements_data[stat_num]["headers"]) - 1)
            ]
            doc_data = statements_data[stat_num]["data"]

            # Data is converted into a dataframe
            doc_df = pd.DataFrame(doc_data)

            # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
            doc_df.index = doc_df[0]
            doc_df.index.name = "Category"
            doc_df = doc_df.drop(0, axis=1)

            # Get rid of the '$', '(', ')', and convert the '' to NaNs.
            doc_df = (
                doc_df.replace("[\$,)%]", "", regex=True)
                .replace("[(]", "-", regex=True)
                .replace("", "NaN", regex=True)
                .replace("\[", "", regex=True)
                .replace("\]", "", regex=True)
            )

            # Strings are converted into floats
            # If there is an error, the document check is skipped
            try:
                doc_df = doc_df.astype(float)
            except:
                break

            # Column header names inserted
            doc_df.columns = doc_header[index_num:]

            #####################################################################
            # Regular expression removes special characters from company name. This helps if the company name contains a "\\" which may break code.
            # Some special characters are also not permitted in folder names.

            if not os.path.exists(
                data_directory
                + "\\"
                + re.sub("[^a-zA-Z0-9 \n\.]", "", com_files["Name"][company]).strip()
            ):
                os.makedirs(
                    data_directory
                    + "\\"
                    + re.sub(
                        "[^a-zA-Z0-9 \n\.]", "", com_files["Name"][company]
                    ).strip()
                )

            # A file is created in each company's folder with a name structured: Filing type + Filing date + Table type
            new_file_dir = (
                data_directory
                + "\\"
                + re.sub("[^a-zA-Z0-9 \n\.]", "", com_files["Name"][company]).strip()
                + "\\"
                + filing_name[0:3]
                + "_"
                + com_files.at[company, term_date][
                    com_files.at[company, filing_name].index(filing)
                ]
                + "_"
                + str(com_files.at[company, "CIK"])
                + "_"
                + str(com_files.at[company, "SIC"][0])
                + "_"
                + headers[stat_num]
            )
            print("This is the new_file_dir: " + new_file_dir)
            if not os.path.exists(new_file_dir):
                doc_df.to_csv(new_file_dir)
                print("data saved")


def best_fit_url(master_reports, default_list):

    print("Best fit algorithm initiated")
    # Hold values initialised
    match_values = []

    # print("These are the master reports")
    # print(master_reports)
    # print(len(master_reports))

    # Loop through each statement url
    for statement in master_reports:
        # print("This is the statement")
        # print(statement)

        # A dictionary is defined that will store the different parts of the statement
        statement_data = {}
        statement_data["headers"] = []
        statement_data["sections"] = []
        statement_data["data"] = []

        hold_term = 0
        while hold_term == 0:

            # Statement file content requested
            # Hold term is added in because there is a strange error in BeautifulSoup which causes it to error out randomly
            # This hold term ensures that if an error out occurs, the code will run again until it works
            try:
                content1 = requests.get(statement["url"]).content
                hold_term = 1
            except:
                hold_term = 0

        hold_term = 0
        while hold_term == 0:
            try:
                report_soup = BeautifulSoup(content1, "html")
                hold_term = 1
            except:
                hold_term = 0

        # All rows found and parsed
        try:
            for index, row in enumerate(report_soup.table.find_all("tr")):

                cols = row.find_all("td")

                # Statement for a regular row and not section or table header
                if len(row.find_all("th")) == 0 and len(row.find_all("strong")) == 0:
                    reg_row = [ele.text.strip() for ele in cols]
                    statement_data["data"].append(reg_row)

                # Statement for a regular row and a section but not a table header
                elif len(row.find_all("th")) == 0 and len(row.find_all("strong")) != 0:
                    sec_row = cols[0].text.strip()
                    statement_data["sections"].append(sec_row)

                # Statement if none of the above are recognised, therefore it's a table header
                elif len(row.find_all("th")) != 0:
                    hed_row = [ele.text.strip() for ele in row.find_all("th")]
                    statement_data["headers"].append(hed_row)

                else:
                    print("We encountered an error.")

            # Grab the proper components

            if len(statement_data["headers"]) == 1:
                index_num = 1
            elif len(statement_data["headers"]) == 2:
                index_num = 0
            else:
                pass

            doc_header = statement_data["headers"][(len(statement_data["headers"]) - 1)]
            doc_data = statement_data["data"]

            # Data is converted into a dataframe
            doc_df = pd.DataFrame(doc_data)

            # Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
            doc_df.index = doc_df[0]
            doc_df.index.name = "Category"
            doc_df = doc_df.drop(0, axis=1)

            # Get rid of the '$', '(', ')', and convert the '' to NaNs.
            doc_df = (
                doc_df.replace("[\$,)%]", "", regex=True)
                .replace("[(]", "-", regex=True)
                .replace("", "NaN", regex=True)
                .replace("\[", "", regex=True)
                .replace("\]", "", regex=True)
            )
        except:
            print("We have encountered an error while looking for 'tr' in code")
        # Strings are converted into floats
        # If there is an error, the document check is skipped

        try:
            doc_df = doc_df.astype(float)
            exception = 0
        except:
            exception = 1
            # print("Exception")

        # Column header names inserted
        if len(doc_df.columns) == len(doc_header[index_num:]):
            doc_df.columns = doc_header[index_num:]

        # This loop is to convert the pandas index format (all the terms found in that report) into a list.
        # The loop also cleans the list, ensuring that no punctuation is in the final list and all the terms are in lower case.

        category_hold = []
        for i in range(0, len(doc_df.index)):
            category_hold.append(doc_df.index[i])

        category_hold = [x.lower() for x in category_hold]
        category_hold = [
            "".join(c for c in s if c not in string.punctuation) for s in category_hold
        ]

        # TODO is this function the function that slows the program down? should minimise using it
        # TODO maybe x = list_average(category_hold, default_list) then reference x throughout?
        list_average_calc = list_average(category_hold, default_list)
        match_values.append(list_average_calc)
        # print(
        #    "Exception was: "
        #    + str(exception)
        #    + "\nValue was: "
        #    + str(list_average_calc)
        # )

        # This new sections returns the first match that meets the threshold of > 2
        if list_average_calc > 2:
            print(list_average_calc)
            output = master_reports[match_values.index(list_average_calc)]
            return output

    # print("Values for each master report given: ")
    # print(default_list[0])
    # print(match_values)

    if sum(match_values) == 0 or max(match_values) < 2:
        output = "No match found"
    else:
        output = master_reports[match_values.index(max(match_values))]

    return output


# def word2vec(word):
#     from collections import Counter
#     from math import sqrt

#     # count the characters in word
#     cw = Counter(word)
#     # precomputes a set of the different characters
#     sw = set(cw)
#     # precomputes the "length" of the word vector
#     lw = sqrt(sum(c * c for c in cw.values()))

#     # return a tuple
#     return cw, sw, lw


def modified_word2vec(x):
    # TODO why do you import it here, move to the top

    hold_count = []
    for i in range(0, len(x) - 1):
        hold_count.append(x[i] + x[i + 1])

    cw = collections.Counter(hold_count)
    sw = set(cw)
    lw = math.sqrt(sum(c * c for c in cw.values()))

    return cw, sw, lw


def cosdis(v1, v2):
    # which characters are common to the two words?
    common = v1[1].intersection(v2[1])
    # by definition of cosine distance we have
    return sum(v1[0][ch] * v2[0][ch] for ch in common) / v1[2] / v2[2]


def list_average(list_A, list_B):
    threshold = 0.80
    cumulative_res = 0

    list_A = [x for x in list_A if x != ""]
    list_B = [x for x in list_B if x != ""]
    list_A = [x for x in list_A if len(x) > 1]
    list_B = [x for x in list_B if len(x) > 1]

    for key in list_A:
        for word in list_B:
            try:
                res = cosdis(modified_word2vec(word), modified_word2vec(key))

                if res > threshold:
                    cumulative_res += res

            except IndexError:
                pass

    return cumulative_res


if __name__ == "__main__":
    main()
