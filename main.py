import os
import xml.etree.ElementTree as ET
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List,Optional
import uvicorn
import datetime
import cx_Oracle
import sys
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
    oracle_client_path = os.path.join(application_path, 'Client')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)


class JBRequest(BaseModel):
    packageNumbers: List[str]


class AddValueResponse(BaseModel):
    result: bool
    Msg: str
    added_value: float = 0.0

def fetch_job_order_status(jde_search: str) -> str:
    soap_settings_path = os.path.join("D:\\Inditech\\Integrations\\JDE Job Order", "SoapSettings.xml")
    soap_input_path = os.path.join("D:\\Inditech\\Integrations\\JDE Job Order", "SoapOrdInput.xml")
    soap_response_path = os.path.join("D:\\Inditech\\Integrations\\JDE Job Order", "SoapOrdResponse.xml")



    settings_tree = ET.parse(soap_settings_path)
    soap_uri = settings_tree.find(".//SoapOrdURI").text


    with open(soap_input_path, "r") as file:
        soap_request_template = file.read()



    soap_request = soap_request_template.replace(
        "[SEARCHDATA]", 
        f"<inputArray><vendorReference>{jde_search}</vendorReference></inputArray>"
    )



    headers = {"Content-Type": "text/xml; charset=utf-8"}
    try:
        response = requests.post(soap_uri, data=soap_request, headers=headers, verify=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to make SOAP request")


    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to fetch job order status")

    with open(soap_response_path, "w", encoding='utf-8', errors='replace') as file:
        file.write(response.text)

    try:
        response_tree = ET.ElementTree(ET.fromstring(response.content))
    except ET.ParseError as parse_err:
        raise HTTPException(status_code=500, detail="Error parsing SOAP response XML")


    messages = response_tree.findall(".//message")


    if not messages:
        message = "Order not found in JDE"
    else:

        message = messages[0].text

        if message:
            message = message.encode('utf-8', errors='replace').decode('utf-8')  
            message = message.replace("\x92", "'")  

    return message




@app.post("/api/log/joborderstatus", response_model=AddValueResponse)
async def job_order_status(request: JBRequest):
    status_messages = []
    
    for package_number in request.packageNumbers:
        try:
            status_message = fetch_job_order_status(package_number)
            status_messages.append(status_message)
        except Exception as e:
            return AddValueResponse(result=False, Msg=f"Error fetching status: {str(e)}", added_value=0.0)
    
    return AddValueResponse(result=True, Msg=", ".join(status_messages), added_value=0.0)


class OrderRequest(BaseModel):
    JDEsearch: Optional[str] = None
    cust: Optional[str] = None
    emp: Optional[str] = None
    branch: Optional[str] = None
    fdate: Optional[str] = None
    tdate: Optional[str] = None
    item: Optional[str] = None
    jstatus: Optional[str] = None

class OrderResponse(BaseModel):
    package_no1: str
    alu1: str
    created_date1: str
    ship_date1: str
    bt_primary_phone_no1: str
    bt_first_name1: str
    employee1_login_name1: str
    order_qty1: float
    Org_price1: float
    price1: float
    disc_amt1: float
    iprice1: float
    idisc_amt1: float
    order_status1: str
    due_amt1: float
    so_deposit_amt_paid1: float
    cgc1: float
    doc_no1: float


@app.post("/api/log/getjoborder", response_model=List[OrderResponse])
async def get_job_order(request: OrderRequest):
    dsn_tns = cx_Oracle.makedsn("127.0.0.1", "1521", service_name="rproods")
    connection = cx_Oracle.connect(user="reportuser", password="report", dsn=dsn_tns)

    selQryBuilder = """
    WITH recent_vi AS (
      SELECT vi.item_sid,
             vi.vou_sid,
             vi.item_note1,
             vi.created_datetime,
             ROW_NUMBER() OVER (PARTITION BY vi.item_sid, vi.item_note1 ORDER BY vi.created_datetime DESC) AS rn
      FROM   rps.vou_item vi
    )
    SELECT dso.package_no,
           dso.alu,
           TRUNC(dso1.created_datetime),
           dso1.udf4_string,
           dso1.bt_primary_phone_no,
           dso1.bt_first_name,
           dso.employee1_login_name,
           dso.qty,
           NVL(dso.orig_price * dso.qty, 0),
           NVL(dso.price * dso.qty, 0),
           NVL(dso.disc_amt, 0),
           NVL(dreg.price * dreg.qty, 0) AS Invoice_Price,
           NVL(dreg.disc_amt * dreg.qty, 0) AS Invoice_Disc,
           CASE
             WHEN dso.package_no = dreg.package_no AND dso1.so_cancel_flag = 0 THEN 'Delivered'
             WHEN dso.package_no = dreg.package_no AND dso1.so_cancel_flag = 1 THEN 'Delivered - SO Cancelled'
             WHEN EXISTS (SELECT 1 FROM rps.document d WHERE d.ref_order_sid = dso1.sid) AND dso1.so_cancel_flag = 0 THEN 'Delivered'
             WHEN EXISTS (SELECT 1 FROM rps.document d WHERE d.ref_order_sid = dso1.sid) AND dso1.so_cancel_flag = 1 THEN 'Delivered - SO Cancelled'
             WHEN dso1.so_cancel_flag = 1 THEN 'Cancelled Order'
             WHEN vo.status = 3 AND vo.vou_type = 0 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 0 THEN 'Pending for Receiving'
             WHEN vo.status = 4 AND vo.vou_type = 1 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 0 THEN 'Return to Vendor'
             WHEN vo.status = 4 AND vo.vou_type = 1 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 1 THEN 'Return to Vendor Cancelled Order'
             WHEN vo.status = 4 AND vo.vou_type = 0 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 0 THEN 'Pending for Delivery'
             WHEN vo.status = 4 AND vo.vou_type = 0 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 1 THEN 'After Received Cancelled Order'
             ELSE 'Work Order Raised'
           END AS order_status,
           DECODE(dreg.sid, NULL, NVL((NVL(dso.price * dso.qty, 0) - NVL(dso1.total_deposit_taken, 0)), 0), '0') AS Due,
           DECODE(dreg.sid, NULL, NVL(dso1.total_deposit_taken, 0), NVL(dso.price * dso.qty, 0)) AS SO_DEPOSIT_AMT_PAID,
           NVL((SELECT amount FROM rps.tender WHERE tender_name = 'Central GiftCard' AND doc_sid = dso1.sid), 0) CGC,
           dso1.order_doc_no 
    FROM   rps.document_item dso
           LEFT JOIN rps.document_item dreg
                  ON ( To_char(dso.doc_sid) = To_char(dreg.udf5_string)
                       AND dreg.invn_sbs_item_sid = dso.invn_sbs_item_sid
                       AND dso.package_no = dreg.package_no )
           LEFT JOIN rps.document dso1
                  ON dso1.sid = dso.doc_sid
           LEFT JOIN rps.document dreg1
                  ON dreg1.sid = dreg.doc_sid and dreg1.status = 4
           LEFT JOIN recent_vi vi
                  ON ( vi.item_sid = dso.invn_sbs_item_sid
                       AND vi.item_note1 = dso.package_no
                       AND vi.rn = 1 ) 
           LEFT JOIN rps.voucher vo ON vo.sid = vi.vou_sid
    WHERE (dso1.receipt_type = 2 OR dso1.receipt_type IS NULL)
    AND dso1.status = 4
    AND dso1.order_doc_no IS NOT NULL
    and dso1.store_no not in (48, 65)
    """

    filters = []
    if request.JDEsearch:
        filters.append(f"dso.package_no = '{request.JDEsearch}'")
    if request.branch:
        filters.append(f"dso1.store_name = '{request.branch}'")
    if request.cust:
        filters.append(f"dso1.bt_primary_phone_no = '{request.cust}'")
    if request.emp:
        filters.append(f"dso.employee1_login_name = '{request.emp}'")
    if request.item:
        filters.append(f"dso.alu = '{request.item}'")
    if request.fdate:
        filters.append(f"TRUNC(dso1.created_datetime) >= TO_DATE('{request.fdate}', 'YYYY-MM-DD')")
    if request.tdate:
        filters.append(f"TRUNC(dso1.created_datetime) <= TO_DATE('{request.tdate}', 'YYYY-MM-DD')")

    if filters:
        selQryBuilder += " AND " + " AND ".join(filters)

    try:
        cursor = connection.cursor()
        cursor.execute(selQryBuilder)
        rows = cursor.fetchall()

        response = []
        for row in rows:
            response.append(OrderResponse(
                package_no1=row[0],
                alu1=row[1],
                created_date1=row[2].strftime('%Y-%m-%d'),
                ship_date1=row[3] or "0",
                bt_primary_phone_no1=row[4] or "0",
                bt_first_name1=row[5] or "0",
                employee1_login_name1=row[6] or "0",
                order_qty1=row[7] or 0.0,
                Org_price1=row[8] or 0.0,
                price1=row[9] or 0.0,
                disc_amt1=row[10] or 0.0,
                iprice1=row[11] or 0.0,
                idisc_amt1=row[12] or 0.0,
                order_status1=row[13] or "",
                due_amt1=row[14] or 0.0,
                so_deposit_amt_paid1=row[15] or 0.0,
                cgc1=row[16] or 0.0,
                doc_no1=row[17] or 0.0
            ))

        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {str(e)}")

    finally:
        cursor.close()
        connection.close()



def clean_text(text):
    if isinstance(text, str):
        return text.replace("", "'").replace("\x92", "'")
    return text

@app.post("/api/log/getjoborder/export")
async def get_job_order_export(request: OrderRequest):

    dsn_tns = cx_Oracle.makedsn("127.0.0.1", "1521", service_name="rproods")
    connection = cx_Oracle.connect(user="reportuser", password="report", dsn=dsn_tns)

    selQryBuilder = """
    WITH recent_vi AS (
      SELECT vi.item_sid,
             vi.vou_sid,
             vi.item_note1,
             vi.created_datetime,
             ROW_NUMBER() OVER (PARTITION BY vi.item_sid, vi.item_note1 ORDER BY vi.created_datetime DESC) AS rn
      FROM   rps.vou_item vi
    )
    SELECT dso.package_no,
           dso.alu,
           TRUNC(dso1.created_datetime),
           dso1.udf4_string,
           dso1.bt_primary_phone_no,
           dso1.bt_first_name,
           dso.employee1_login_name,
           dso.qty,
           NVL(dso.orig_price * dso.qty, 0),
           NVL(dso.price * dso.qty, 0),
           NVL(dso.disc_amt, 0),
           NVL(dreg.price * dreg.qty, 0) AS Invoice_Price,
           NVL(dreg.disc_amt * dreg.qty, 0) AS Invoice_Disc,
           CASE
             WHEN dso.package_no = dreg.package_no AND dso1.so_cancel_flag = 0 THEN 'Delivered'
             WHEN dso.package_no = dreg.package_no AND dso1.so_cancel_flag = 1 THEN 'Delivered - SO Cancelled'
             WHEN EXISTS (SELECT 1 FROM rps.document d WHERE d.ref_order_sid = dso1.sid) AND dso1.so_cancel_flag = 0 THEN 'Delivered'
             WHEN EXISTS (SELECT 1 FROM rps.document d WHERE d.ref_order_sid = dso1.sid) AND dso1.so_cancel_flag = 1 THEN 'Delivered - SO Cancelled'
             WHEN dso1.so_cancel_flag = 1 THEN 'Cancelled Order'
             WHEN vo.status = 3 AND vo.vou_type = 0 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 0 THEN 'Pending for Receiving'
             WHEN vo.status = 4 AND vo.vou_type = 1 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 0 THEN 'Return to Vendor'
             WHEN vo.status = 4 AND vo.vou_type = 1 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 1 THEN 'Return to Vendor Cancelled Order'
             WHEN vo.status = 4 AND vo.vou_type = 0 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 0 THEN 'Pending for Delivery'
             WHEN vo.status = 4 AND vo.vou_type = 0 AND vi.item_note1 = dso.package_no AND dso1.so_cancel_flag = 1 THEN 'After Received Cancelled Order'
             ELSE 'Work Order Raised'
           END AS order_status,
           DECODE(dreg.sid, NULL, NVL((NVL(dso.price * dso.qty, 0) - NVL(dso1.total_deposit_taken, 0)), 0), '0') AS Due,
           DECODE(dreg.sid, NULL, NVL(dso1.total_deposit_taken, 0), NVL(dso.price * dso.qty, 0)) AS SO_DEPOSIT_AMT_PAID,
           NVL((SELECT amount FROM rps.tender WHERE tender_name = 'Central GiftCard' AND doc_sid = dso1.sid), 0) CGC,
           dso1.order_doc_no 
    FROM   rps.document_item dso
           LEFT JOIN rps.document_item dreg
                  ON ( To_char(dso.doc_sid) = To_char(dreg.udf5_string)
                       AND dreg.invn_sbs_item_sid = dso.invn_sbs_item_sid
                       AND dso.package_no = dreg.package_no )
           LEFT JOIN rps.document dso1
                  ON dso1.sid = dso.doc_sid
           LEFT JOIN rps.document dreg1
                  ON dreg1.sid = dreg.doc_sid and dreg1.status = 4
           LEFT JOIN recent_vi vi
                  ON ( vi.item_sid = dso.invn_sbs_item_sid
                       AND vi.item_note1 = dso.package_no
                       AND vi.rn = 1 ) 
           LEFT JOIN rps.voucher vo ON vo.sid = vi.vou_sid
    WHERE (dso1.receipt_type = 2 OR dso1.receipt_type IS NULL)
    AND dso1.status = 4
    AND dso1.order_doc_no IS NOT NULL
    and dso1.store_no not in (48, 65)
    """

    filters = []
    if request.JDEsearch:
        filters.append(f"dso.package_no = '{request.JDEsearch}'")
    if request.branch:
        filters.append(f"dso1.store_name = '{request.branch}'")
    if request.cust:
        filters.append(f"dso1.bt_primary_phone_no = '{request.cust}'")
    if request.emp:
        filters.append(f"dso.employee1_login_name = '{request.emp}'")
    if request.item:
        filters.append(f"dso.alu = '{request.item}'")
    if request.fdate:
        filters.append(f"TRUNC(dso1.created_datetime) >= TO_DATE('{request.fdate}', 'YYYY-MM-DD')")
    if request.tdate:
        filters.append(f"TRUNC(dso1.created_datetime) <= TO_DATE('{request.tdate}', 'YYYY-MM-DD')")

    if filters:
        selQryBuilder += " AND " + " AND ".join(filters)

    try:
        cursor = connection.cursor()
        cursor.execute(selQryBuilder)
        rows = cursor.fetchall()

        print("Query executed successfully. Rows fetched:", len(rows))


        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        df = df.applymap(clean_text) 

        for index, row in df.iterrows():
            if row['ORDER_STATUS'] == 'Work Order Raised':
                package_no = row['PACKAGE_NO']
                print(f"Fetching SOAP status for package number: {package_no}")
                soap_status = fetch_job_order_status(package_no)
                print(soap_status)
                edited_status = clean_text(soap_status)
                print(f"Package Number: {package_no}, Status: {edited_status}")
                df.at[index, "ORDER_STATUS"] = edited_status

        excel_path = "job_order_export.xlsx"
        df.to_excel(excel_path, index=False) 

        return FileResponse(path=excel_path, filename="job_order_export.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        print("Error encountered:", str(e)) 
        raise HTTPException(status_code=500, detail=f"Error executing query: {str(e)}")

    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)