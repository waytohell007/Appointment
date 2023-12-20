import pymongo
from pymongo import MongoClient
import smtplib
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# Calculate the timestamp of 2 hours ago
two_hours_ago = datetime.utcnow() - timedelta(hours=2)

# MongoDB connection details
client = MongoClient("mongodb://tech_readdb:tech_readdb" + "@10.27.15.136:37018/fluprod_minervadb")
db = client.fluprod_minervadb

# Email configuration
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
SMTP_USERNAME = "abc@xyz.com"
SMTP_PASSWORD = "H31nl31n@2023"
SENDER_EMAIL = "abc@xyz.com"
RECIPIENT_EMAILS = []
EMAIL_SUBJECT = "Fleury - Processing Rate of hl7Messages"
# MongoDB query
mongo_query = [
    {"$match": {"processedDate": {"$gte": two_hours_ago}, "processed": {"$nin": ["PENDING"]}}},
    {"$group": {"_id": {"messageType": "$messageType", "jobInstanceNumber": "$jobInstanceNumber", "status": "$processed"}, "Count": {"$sum": 1}}},
    {"$match": {"_id.messageType": {"$ne": "SIU"}}},
    {"$sort": {"_id.jobInstanceNumber": 1}}
]

mongo_query2 = [
        {"$match":{"processed":"PENDING"}},
        {"$group":{"_id":{"messageType":"$messageType","jobInstanceNumber":"$jobInstanceNumber","status":"$processed"},"Count":{"$sum":1}}},
        {"$sort":{"_id.jobInstanceNumber":1}}
]

siu_processed_mongo_query = [
        {"$match":{"processed": "PROCESSED", "messageType":"SIU", "lockDate": {"$gte": two_hours_ago}}}, 
        {"$group": {"_id": {"messageType": "$messageType", "jobInstanceNumber": "$jobInstanceNumber"}, "Count": {"$sum":1}}}, 
        {"$sort":{"_id.jobInstanceNumber":1}}
]

mongo_query_failed = [
        {"$match":{"processed": "FAILED",  "lockDate": {"$gte": two_hours_ago}}},
        {"$group": {"_id": {"messageType": "$messageType", "jobInstanceNumber": "$jobInstanceNumber"}, "Count": {"$sum":1}}},
        {"$sort":{"_id.jobInstanceNumber":1}}
]

job_config = [
        {"$group": {"_id":{"serviceName": "$serviceName", "modInstance": "$modInstance", "clusterId": "$clusterId"} }},
        {"$sort": {"_id.serviceName":1, "_id.modInstance":1}}
]


# Connect to MongoDB



# Execute the MongoDB query
processed_results = list(db.hl7Message.aggregate(mongo_query))

pending_results = list(db.hl7Message.aggregate(mongo_query2))

processed_siu_results = list(db.hl7Message.aggregate(siu_processed_mongo_query))

failed_results = list(db.hl7Message.aggregate(mongo_query_failed))

jobConfig_result = list(db.jobConfiguration.aggregate(job_config))

merged_results = {}

for result in processed_siu_results:
    key = (result["_id"]["messageType"], result["_id"]["jobInstanceNumber"])
    if key not in merged_results:
        merged_results[key] = {}
    merged_results[key]["PROCESSED"] = result["Count"]

for result in processed_results:
    key = (result["_id"]["messageType"], result["_id"]["jobInstanceNumber"])
    if key not in merged_results:
        merged_results[key] = {}
    merged_results[key]["PROCESSED"] = result["Count"]


for result in pending_results:
    key = (result["_id"]["messageType"], result["_id"]["jobInstanceNumber"])
    if key not in merged_results:
        merged_results[key] = {}
    merged_results[key]["PENDING"] = result["Count"]

for result in failed_results:
    key = (result["_id"]["messageType"], result["_id"]["jobInstanceNumber"])
    if key not in merged_results:
        merged_results[key] = {}
    merged_results[key]["FAILED"] = result["Count"]

for items in jobConfig_result:
    serviceName = items["_id"]["serviceName"]
    if serviceName == "amiADT":
        messageType = "ADT"
    elif serviceName == "amiOrm":
        messageType = "ORM"
    elif serviceName == "amiOru":
        messageType = "ORU"
    elif serviceName == "labOrmOru":
        messageType = "ORM_LAB"
    elif serviceName == "SIU":
        messageType = "SIU"
    else:
        messageType = ""
    instance = items["_id"]["modInstance"]
    key = (messageType, instance)
    if key in merged_results:
        merged_results[key]["ClusterID"] = items["_id"]["clusterId"]
    
    if serviceName == "labOrmOru":
        messageType = "ORU_LAB"
        key = (messageType, instance)
        if key in merged_results:
            merged_results[key]["ClusterID"] = items["_id"]["clusterId"]


# Format query result as HTML
#html_content = "<h4>Dear Team, <p></p> Please find the processing rate in last 2 hours</h4>"
#html_content += "<p></p>Please note, the query is running on processedDate and hence cannot capture failure status<p></p>"

#for result in query_result:
#    html_content += f"<p>{result['_id']} - Count: {result['Count']}</p>"
#html_content += "Regards<p></p> Technical Services Team<p></p><p></p>"

html_table = """<table style="border-collapse: collapse; width: 100%;">
                <tr>
                    <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">MessageType</th>
                    <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">JobInstanceNumber</th>
                    <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">PROCESSED</th>
                    <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">PENDING</th>
                    <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">FAILED</th>
                    <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">ClusterID</th>
                </tr>
            """
for key, value in merged_results.items():
    message_type, job_instance_number = key
    cluster = value.get("ClusterID",0)
    processed_count = value.get("PROCESSED", 0)
    pending_count = value.get("PENDING", 0)
    failed_count = value.get("FAILED", 0)

    html_table += f"""<tr>
                        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;">{message_type}</td>
                        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;">{job_instance_number}</td>
                        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;">{processed_count}</td>
                        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;">{pending_count}</td>
                        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;">{failed_count}</td>
                        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;">{cluster}</td>
                    </tr>
                    """

html_table += "</table>"

email_content = f"""Dear Team,

Please see the processing rate for hl7Message in the last 2 hours. Please note that PENDING count is the total count on the instance.<p></p><p></p>

{html_table}

Thanks & Regards,<p><p/>
Technical Services Team<p></p><p></p><p></p>
"""
#print(html_table)

# Send email
msg = MIMEMultipart()
msg['From'] = SENDER_EMAIL
msg['To'] = ", ".join(RECIPIENT_EMAILS)
msg['Subject'] = EMAIL_SUBJECT
msg.attach(MIMEText(email_content, 'html'))

smtp = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
smtp.starttls()
smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
smtp.sendmail(SENDER_EMAIL, RECIPIENT_EMAILS, msg.as_string())
smtp.quit()
