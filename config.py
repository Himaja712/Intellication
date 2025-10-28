# Authorization
X_Authorization = "Yh7og2vjmznpuh8MpKbHrrvL9UMsQxAHpZYt"
DRIVE_CREDENTIALS = './informedk12-reimbursement-aac169d07799.json'

SCOPES = ['https://www.googleapis.com/auth/drive']

# Google Drive Folder IDs
# GOOGLE_DRIVE_FOLDER_ID_COMPLETED = "1sUzyfZw7wwA0lwBGHovIGRLfnRh9kj0z"
GOOGLE_DRIVE_FOLDER_ID_COMPLETED = "138Gy6v8UaRAgDuexrkbkJQt3Rhd7Npf0"
GOOGLE_DRIVE_FOLDER_ID_ARCHIVED = "138Gy6v8UaRAgDuexrkbkJQt3Rhd7Npf0"
# GOOGLE_DRIVE_FOLDER_ID_ARCHIVED = "1q3m7DPrUIBhwSQsaebfxoP7rjp7vnBYM"

# Escape File Name 
FILE_NAME_TO_DOWNLOAD = "PUSD Employee Escape Data.xlsx" 
# Escape File Folder ID
GOOGLE_DRIVE_FOLDER_ID1 = "1o-kOlAHUNHMzDakkCakMjA2Swg0QtH3v"

# Informed K12 URLs
URL = "https://app.informedk12.com/api/v1/campaigns/"
STATUS_COMPLETED = "/responses?statusGroup=pending"
STATUS_ARCHIVED = "archived"

# New Campaign Ids
campaign_id_expense = 173260
campaign_id_mileage = 173261
campaign_id_conference = 173262

# Old Campaign Ids
old_campaign_id_expense = 153257
old_campaign_id_mileage = 159935
old_campaign_id_conference = 150715

CAMPAIGN_IDS = {
    "expense": {"old": old_campaign_id_expense, "new": campaign_id_expense},
    "mileage": {"old": old_campaign_id_mileage, "new": campaign_id_mileage},
    "conference": {"old": old_campaign_id_conference, "new": campaign_id_conference},
}

# CSV file headers
template_columns = [
        "Transfer Date", "Tran Date", "Org ID", "Bank", "Vendor", "Invoice Date", "Amount", "Account",
        "Invoice #", "Comment", "Local Field", "City", "Country", "Payee Name", "State", "Street",
        "ZIP", "PymtType", "EmpId", "Ref#", "VendorAddrId", "FinalPymt", "OnHold"
    ]

# Expense, Mileage and Conference sheet headers
COMMON_COLUMNS = [
    "Invoice Date", "Employee #", "First", "Last",
    "Account Code 1", "Account Code 1 Total", "Account Code 2", "Account Code 2 Total",
    "Account Code 3", "Account Code 3 Total", "Total Reimbursement"
]

# Merged sheet headers
Merged_column_order = [
        "Org Id", "Invoice #", "Invoice Date", "Employee #", "First", "Last", "Email_Escape", "Account Code 1", 
        "Account Code 1 Total", "Account Code 2", "Account Code 2 Total", "Account Code 3", "Account Code 3 Total", 
        "Total Reimbursement", "Emp_Status", "Form" ]

required_cols = ["Org Id", "Last", "First", "Employee #", "Emp_Status", "Email_Escape", "Match_Status"]

# Expense header mappings
RENAME_MAPPING_EXPENSE = {
    74: "Invoice Date",
    4: "Employee #",
    3: "First",
    2: "Last",
    60: "Account Code 1",
    61: "Account Code 1 Total",
    62: "Account Code 2",
    63: "Account Code 2 Total",
    83: "Total Reimbursement",
    9: "Email_API",
}

# Expense excel headers
EXPENSE_COLUMNS = [
    "Invoice Date", "Employee #", "First", "Last",
    "Account Code 1", "Account Code 1 Total", "Account Code 2", "Account Code 2 Total",
    "Account Code 3", "Account Code 3 Total", "Total Reimbursement", "Email_API"
]

# Mileage header mappings
RENAME_MAPPING_MILEAGE = {
    368: "Invoice Date",
    1: "Employee #",        
    489: "First",
    490: "Last",
    415: "Account Code 1",
    144: "Account Code 1 Total",
    699: "Total Reimbursement",
    416: "Account Code 2",
    148: "Account Code 2 Total"
}

# Conference header mappings
COLUMN_MAPPING_CONFERENCE = {
    118: "Invoice Date",
    1: "Employee #",
    2: "First",
    3: "Last",
    86: "Account Code 1 Total",
    75: "Total Reimbursement",
    97: "Account Code 2 Total",
    108: "Account Code 3 Total"
}

# Headers from Informed K12
EXTRA_FIELDS = [
    "Invoice #",
    "Invoice Date",
    "Account Code 1",
    "Account Code 1 Total",
    "Account Code 2",
    "Account Code 2 Total",
    "Total Reimbursement"
]

# Timezone
est = "America/New_York"

# Target field numbers from Informed K12 forms for New Campaign Ids
target_fields_expense = {74, 4, 3, 2, 60, 61, 62, 63, 83, 9, 84, 85, 86, 87, 88, 89, 90 , 91, 92, 93, 94, 95, 96, 97, 98, 99}
target_fields_mileage = {368, 1, 489, 490, 415, 144,  699, 416, 148, 374, 389, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411}
target_fields_conference = {3, 2, 1, 118, 14, 15, 16, 17, 18, 76, 77, 78, 79, 80, 81, 82, 83, 84, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 108, 75}

# Account code columns from Conference form 
account_code_1_fields = {76, 77, 78, 79, 80, 81, 82, 83, 84}
account_code_2_fields = {87, 88, 89, 90, 91, 92, 93, 94, 95}
account_code_3_fields = {98, 99, 100, 101, 102, 103, 104, 105, 106}

# Target field numbers from Informed K12 forms for Old Campaign Ids
target_fields_expense_old = {80, 74, 4, 3, 2, 60, 61, 62, 63, 83, 9}
target_fields_mileage_old = {488, 368, 1, 489, 490, 415, 144,  699, 416, 148}
target_fields_conference_old = {137, 3, 2, 1, 118, 76, 77, 78, 79, 80, 81, 82, 83, 84, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 108, 75}

# Invoice # field numbers - Old Campaign Ids
invoice_field_old = {
    "expense": 80,
    "mileage": 488,
    "conference": 137
}

# Invoice # field numbers - New Campaign Ids
date_fields_new = {
    "expense": [84, 85, 86, 87, 88, 89, 90 , 91, 92, 93, 94, 95, 96, 97, 98, 99],
    "mileage": [374, 389, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411],
    "conference": [14, 15, 16, 17, 18]
}

# Define expected headers
escape_headers = ["Org Id", "Last", "First", "Employee #", "Emp_Status", "Email_Escape"]  

# Global variables
not_found = "Not Found"
matched = "Matched"
org_id = 33
bank = "COUNTY"
paymt = "E"
comment = "REIMB EXPENSE"
account_code_1 = "Account Code 1"
account_code_2 = "Account Code 2"
account_code_3 = "Account Code 3"
account_code_1_total = "Account Code 1 Total"
account_code_2_total = "Account Code 2 Total"
account_code_3_total = "Account Code 3 Total"
invoice_number = "Invoice #"
last = "Last"
first = "First"
emp_id = "Employee #"
match_col = "Match Status"
form = "Form"
sheet1 = "Expense"
sheet2 = "Mileage"
sheet3 = "Conference"
sheet4 = "Escape"
sheet5 = "Merged"
sheet6 = "Compared"

# Upload excel and csv in the Google drive folder
upload_excel_true=True
upload_excel_false=False