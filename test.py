from html import getHTMLReport

#domain_list = ["Activity", "Party", "Contract", "ContractOption", "ContractOptionFund", "ContractPayout"]

header_dict = {"SourcetoIngestion" : "['Domain', 'Pass', 'Fail', 'No Run']"}

details_dict = {"SourcetoIngestion" : "['Activity', 7, 3, 1], ['Party', 17, 13, 11], ['Contract', 8, 9, 10], ['ContractOption', 15, 16, 17], ['ContractOptionFund', 3, 4, 2], ['ContractPayout', 25, 5, 0]"}

output_file_path = "report.html"

getHTMLReport("Test Report", header_dict, details_dict, output_file_path)
