#!/usr/bin/env python3
"""
ODCANIT Import API Server
==========================
שרת API שמתחבר למסד הנתונים של ODCANIT ומייבא נתונים למערכת הניהול הפיננסי

התקנה:
pip install flask pyodbc flask-cors

הרצה:
python odcanit_api_server.py

השרת ירוץ על http://localhost:5000
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import pyodbc
import json

app = Flask(__name__)
CORS(app)  # מאפשר קריאות מדפדפן

# פרטי חיבור ל-ODCANIT SQL Server
ODCANIT_CONFIG = {
    'server': '172.16.19.2\\ODCANIT',
    'database': 'odlight',
    'username': 'OdcanitAPI',
    'password': '392913',
    'driver': '{ODBC Driver 18 for SQL Server}'  # או '{SQL Server}'
}


def get_connection():
    """יצירת חיבור למסד הנתונים"""
    try:
        connection_string = (
            f"DRIVER={ODCANIT_CONFIG['driver']};"
            f"SERVER={ODCANIT_CONFIG['server']};"
            f"DATABASE={ODCANIT_CONFIG['database']};"
            f"UID={ODCANIT_CONFIG['username']};"
            f"PWD={ODCANIT_CONFIG['password']};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(connection_string, timeout=10)
        return conn
    except Exception as e:
        print(f"שגיאה בחיבור: {e}")
        raise


def execute_query(query):
    """הרצת שאילתה והחזרת תוצאות"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        
        # המרת התוצאות למילון
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return results
    except Exception as e:
        print(f"שגיאה בהרצת שאילתה: {e}")
        raise
    finally:
        if conn:
            conn.close()


@app.route('/api/odcanit-import', methods=['POST'])
def import_data():
    """
    ייבוא נתונים מ-ODCANIT
    
    Body:
    {
        "server": "172.16.19.2\\ODCANIT",
        "database": "odlight",
        "username": "OdcanitAPI",
        "password": "392913",
        "queries": {
            "billing": "SELECT ...",
            "income": "SELECT ...",
            "expenses": "SELECT ..."
        }
    }
    """
    try:
        data = request.json
        queries = data.get('queries', {})
        
        results = {}
        
        # ריצת כל השאילתות באופן דינמי
        for query_name, query_sql in queries.items():
            try:
                query_results = execute_query(query_sql)
                
                # אם השאילתה מחזירה שורה אחת - החזר כאובייקט
                # אם מחזירה יותר - החזר כמערך
                # זה שומר על תאימות לאחור עם קוד קיים
                if query_name in ['billing', 'income', 'expensesTotal', 'workDays']:
                    # שאילתות סיכום - שורה אחת
                    results[query_name] = query_results[0] if query_results else {}
                else:
                    # שאילתות פירוט - מערך
                    results[query_name] = query_results if query_results else []
                    
            except Exception as e:
                print(f"שגיאה בשאילתה '{query_name}': {e}")
                results[query_name] = [] if query_name not in ['billing', 'income', 'expensesTotal'] else {}
        
        return jsonify({
            'success': True,
            'data': results
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/odcanit-test', methods=['GET'])
def test_connection():
    """בדיקת חיבור למסד הנתונים"""
    try:
        conn = get_connection()
        conn.close()
        return jsonify({
            'success': True,
            'message': 'חיבור למסד הנתונים הצליח!'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/open-invoices', methods=['GET'])
def get_open_invoices():
    """
    קבלת חשבונות פתוחים
    
    Returns:
    {
        "success": true,
        "data": {
            "totalOpenInvoices": 45,
            "totalOutstandingBalance": 125000.50,  // חשבונות פתוחים (Balance + VAT)
            "totalBalance": 100000.00,              // יתרה ללא מע"מ
            "totalVat": 25000.50,                   // מע"מ
            "totalPaid": 50000.00,
            "avgDaysOverdue": 15.5
        }
    }
    """
    try:
        query = """
            SELECT 
                COUNT(DISTINCT IDinvoice) AS TotalOpenInvoices,
                SUM(Balance) AS TotalBalance,
                SUM(VatAmount) AS TotalVat,
                SUM(Balance) + SUM(VatAmount) AS TotalOutstandingBalance,
                SUM(PaidIncome) AS TotalPaid,
                AVG(DaysNotPaid) AS AvgDaysOverdue
            FROM vwExportToOuterSystems_InvoiceByCategoryAndVat
            WHERE InvStatusDesc = N'פתוח'
        """
        
        results = execute_query(query)
        
        if results and len(results) > 0:
            data = results[0]
            # המרת None ל-0 עבור שדות מספריים
            for key in data:
                if data[key] is None:
                    data[key] = 0
            
            return jsonify({
                'success': True,
                'data': data
            })
        else:
            return jsonify({
                'success': True,
                'data': {
                    'TotalOpenInvoices': 0,
                    'TotalBalance': 0,
                    'TotalVat': 0,
                    'TotalOutstandingBalance': 0,
                    'TotalPaid': 0,
                    'AvgDaysOverdue': 0
                }
            })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/open-invoices/list', methods=['GET'])
def get_open_invoices_list():
    """
    קבלת רשימת חשבונות פתוחים מפורטת
    
    Query Parameters:
    - client_id: סינון לפי לקוח
    - file_id: סינון לפי תיק
    - overdue_only: true/false - רק חשבונות באיחור
    - limit: מספר רשומות מקסימלי (ברירת מחדל: 100)
    
    Returns: רשימת חשבונות פתוחים
    """
    try:
        # קבלת פרמטרים מה-query string
        client_id = request.args.get('client_id')
        file_id = request.args.get('file_id')
        overdue_only = request.args.get('overdue_only', 'false').lower() == 'true'
        limit = int(request.args.get('limit', 100))
        
        # בניית תנאי WHERE
        where_conditions = ["InvStatusDesc = N'פתוח'"]
        
        if client_id:
            where_conditions.append(f"SideCounter = {client_id}")
        if file_id:
            where_conditions.append(f"TikCounter = {file_id}")
        if overdue_only:
            where_conditions.append("DaysNotPaid > 0")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            SELECT TOP {limit}
                IDinvoice AS InvoiceId,
                Dated AS InvoiceDate,
                SideCounter AS ClientId,
                ClientVisualID AS ClientNumber,
                TikCounter AS FileId,
                TikVisualID AS FileNumber,
                InvStatusDesc AS StatusDescription,
                CategoryDesc AS CategoryDescription,
                Amount AS AmountBeforeVat,
                VatAmount AS VatAmount,
                Amount + VatAmount AS TotalAmount,
                Balance AS BalanceBeforeVat,
                Balance + VatAmount AS TotalBalance,
                PaidIncome AS PaidAmount,
                FutureIncome AS UnpaidAmount,
                DaysNotPaid AS DaysOverdue,
                NotPaidCategory AS OverdueCategory,
                paymentDate AS PaymentDueDate,
                Remark AS Notes,
                tsCreateDate AS CreatedDate
            FROM vwExportToOuterSystems_InvoiceByCategoryAndVat
            WHERE {where_clause}
            ORDER BY IDinvoice DESC
        """
        
        results = execute_query(query)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/odcanit-query', methods=['POST'])
def custom_query():
    """
    הרצת שאילתה מותאמת אישית
    
    Body:
    {
        "query": "SELECT * FROM vwExportToOuterSystems_Billing WHERE ..."
    }
    """
    try:
        data = request.json
        query = data.get('query')
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'חסרה שאילתה'
            }), 400
        
        results = execute_query(query)
        
        return jsonify({
            'success': True,
            'data': results,
            'count': len(results)
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 ODCANIT Import API Server")
    print("=" * 60)
    print(f"Server: {ODCANIT_CONFIG['server']}")
    print(f"Database: {ODCANIT_CONFIG['database']}")
    print(f"Username: {ODCANIT_CONFIG['username']}")
    print()
    print("בודק חיבור למסד הנתונים...")
    
    try:
        conn = get_connection()
        print("✅ חיבור למסד הנתונים הצליח!")
        conn.close()
    except Exception as e:
        print(f"❌ שגיאה בחיבור: {e}")
        print()
        print("נא לוודא:")
        print("1. שרת SQL Server פועל")
        print("2. פרטי החיבור נכונים")
        print("3. ODBC Driver 17 for SQL Server מותקן")
        print("   להתקנה: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
    
    print()
    print("=" * 60)
    print("השרת מאזין על http://localhost:5000")
    print("=" * 60)
    print()
    print("נקודות קצה זמינות:")
    print("  POST /api/odcanit-import      - ייבוא נתונים")
    print("  GET  /api/odcanit-test        - בדיקת חיבור")
    print("  GET  /api/open-invoices       - סיכום חשבונות פתוחים")
    print("  GET  /api/open-invoices/list  - רשימת חשבונות פתוחים")
    print("  POST /api/odcanit-query       - שאילתה מותאמת")
    print()
    
    app.run(debug=True, host='0.0.0.0', port=5000)
