from html import getHTMLReport

#domain_list = ["Activity", "Party", "Contract", "ContractOption", "ContractOptionFund", "ContractPayout"]

header_dict = {"Activity" : "['Status', 'Number of test cases']",
"Party" : "['Status', 'Number of test cases']",
"Contract" : "['Status', 'Number of test cases']",
"ContractOption" : "['Status', 'Number of test cases']",
"ContractOptionFund" : "['Status', 'Number of test cases']",
"ContractPayout" : "['Status', 'Number of test cases']"
}

details_dict = {"Activity" : "['Pass', 7], ['Fail', 3], ['No Run', 1]",
"Party" : "['Pass', 17], ['Fail', 13], ['No Run', 11]",
"Contract" : "['Pass', 8], ['Fail', 9], ['No Run', 10]",
"ContractOption" : "['Pass', 15], ['Fail', 16], ['No Run', 17]",
"ContractOptionFund" : "['Pass', 3], ['Fail', 4], ['No Run', 2]",
"ContractPayout" : "['Pass', 25], ['Fail', 5], ['No Run', 0]"
}

output_file_path = "report.html"

getHTMLReport("Test Report", header_dict, details_dict, output_file_path)
