# === FLASK APP: app.py ===
from flask import Flask, request, jsonify
import pyodbc
import requests
from flask_cors import CORS
from datetime import datetime
from dateutil import parser
import pytz

app = Flask(__name__)
CORS(app)

DB_CONNECTION_STRING = r'DRIVER={SQL Server};SERVER=DESKTOP-9IJ4MU3;DATABASE=Emag-Product-Scrapper;UID=alexandru1199;PWD=FHAL4Gen'

from contextlib import contextmanager

@contextmanager
def get_connection():
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        yield conn
    finally:
        conn.close()
def convert_to_bucharest_time(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S') if dt else None
@app.route('/produse', methods=['GET'])
def get_produse():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ID, ProductName, [Index], Page, Categorie, RawPosition, Stock, Price,ReviewCount FROM Produse")
                produse = [
                    {
                        "ID": str(row[0]),
                        "ProductName": row[1] or "",
                        "Index": row[2] if row[2] is not None else -1,
                        "Page": row[3] if row[3] is not None else -1,
                        "Categorie": row[4] or "",
                        "RawPosition": row[5] if row[5] is not None else -1,
                        "Stock": row[6] if row[6] is not None else -1,
                        "Price": row[7] if row[7] is not None else -1,
                        "ReviewCount":row[8] if row[8] is not None else -1
                    }
                    for row in cursor.fetchall()
                ]
        return jsonify(produse), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Eroare DB: {str(e)}"}), 500
@app.route('/produse/<product_id>/audit', methods=['GET'])
def get_audit_for_product(product_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT ProductName, Image, Categorie, Price, ReviewCount
                    FROM Produse
                    WHERE ID = ?
                """, product_id)
                produs_row = cursor.fetchone()
                if not produs_row:
                    return jsonify({"error": "Produsul nu a fost găsit"}), 404

                product_info = {
                    "ProductID": product_id,
                    "ProductName": produs_row[0],
                    "Image": produs_row[1],
                    "Categorie": produs_row[2],
                    "Price": produs_row[3],
                    "ReviewCount": produs_row[4]
                }

                cursor.execute("""
                    SELECT OldIndex, NewIndex, OldPage, NewPage,
                           OldRawPosition, NewRawPosition, OldStock, NewStock, TimeStamp
                    FROM AuditLog
                    WHERE ProductID = ?
                    ORDER BY TimeStamp DESC
                """, product_id)
                logs = []
                for row in cursor.fetchall():
                    logs.append({
                        "OldIndex": row[0],
                        "NewIndex": row[1],
                        "OldPage": row[2],
                        "NewPage": row[3],
                        "OldRawPosition": row[4],
                        "NewRawPosition": row[5],
                        "OldStock": row[6],
                        "NewStock": row[7],
                        "TimeStamp": convert_to_bucharest_time(row[8])
                    })
        return jsonify({"Product": product_info, "AuditLog": logs}), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Audit fetch error: {str(e)}"}), 500

@app.route('/procesare-json-bulk', methods=['POST'])
def insert_products_bulk():
    if not request.is_json:
        return jsonify({"error": "Conținutul trebuie să fie JSON"}), 400

    products = request.get_json()
    if not isinstance(products, list):
        return jsonify({"error": "Se așteaptă o listă de produse"}), 400

    inserted_ids = []
    audit_log = []

    try:
        with get_connection() as conn:
            all_ids = [str(p.get("ID", "")).strip() for p in products if p.get("ID")]
            placeholders = ','.join(['?'] * len(all_ids)) if all_ids else '(NULL)'
            existing = {}

            with conn.cursor() as preload_cursor:
                if all_ids:
                    preload_cursor.execute(f"""
                        SELECT ID, [Index], Page, RawPosition, Stock
                        FROM Produse
                        WHERE ID IN ({placeholders})
                    """, all_ids)
                    for row in preload_cursor.fetchall():
                        existing[str(row[0])] = {
                            "Index": row[1],
                            "Page": row[2],
                            "RawPosition": row[3],
                            "Stock": row[4]
                        }

            with conn.cursor() as cursor:
                for prod in products:
                    pid = str(prod.get("ID", "")).strip()
                    offer_id = str(prod.get("OfferID", "")).strip()
                    name = prod.get("ProductName", "")
                    image = prod.get("Image")
                    index = prod.get("Index")
                    page = prod.get("Page")
                    ts_raw = prod.get("TimeStamp")
                    ts = parser.isoparse(ts_raw).astimezone(pytz.timezone("Europe/Bucharest")).strftime(
                        '%Y-%m-%d %H:%M:%S') if ts_raw else None
                    cat = prod.get("Categorie")
                    raw_pos = prod.get("RawPosition") or prod.get("NewRawPosition")
                    stock = prod.get("Stock")
                    price = float(prod.get("Price"))
                    reviewCount=int(prod.get("ReviewCount"))
                    # 🔥 Nou: extragem și old/new stock explicit din JSON
                    old_stock = prod.get("OldStock")
                    new_stock = prod.get("NewStock") or stock

                    if not all([pid, name, image, index is not None, page is not None, ts, cat]):
                        inserted_ids.append(None)
                        continue

                    if pid in existing:
                        old = existing[pid]

                        same_index = old["Index"] == index
                        same_page = old["Page"] == page
                        same_raw = old.get("RawPosition") == raw_pos
                        same_stock = old.get("Stock") == new_stock

                        if same_index and same_page and same_raw and same_stock:
                            inserted_ids.append(pid)
                            continue

                        cursor.execute("""
                                  UPDATE Produse
                                  SET OfferID=?, ProductName=?, Image=?, [Index]=?, Page=?, TimeStamp=?, Categorie=?, RawPosition=?, Stock=?, Price=?
                                  WHERE ID=?
                              """, (offer_id, name, image, index, page, ts, cat, raw_pos, new_stock, price, pid))

                        index_changed = not same_index
                        page_changed = not same_page
                        raw_changed = not same_raw
                        stock_changed = old.get("Stock") != new_stock

                        if page_changed or (
                                index_changed and page <= 2 and abs(old["Index"] - index) >= 5
                        ) or (
                                raw_changed and page <= 2 and abs((old.get("RawPosition") or 0) - raw_pos) >= 5
                        ):
                            with conn.cursor() as check_cursor:
                                check_cursor.execute("""
                                          SELECT TOP 1 NewIndex, NewPage, NewRawPosition
                                          FROM AuditLog
                                          WHERE ProductID = ?
                                          ORDER BY TimeStamp DESC
                                      """, (pid,))
                                last_log = check_cursor.fetchone()

                            if last_log:
                                last_index, last_page, last_raw = last_log
                                if last_index == index and last_page == page and last_raw == raw_pos:
                                    inserted_ids.append(pid)
                                    continue

                            audit_log.append({
                                "ProductID": pid,
                                "OldIndex": old["Index"],
                                "NewIndex": index,
                                "OldPage": old["Page"],
                                "NewPage": page,
                                "OldRawPosition": old.get("RawPosition"),
                                "NewRawPosition": raw_pos,
                                "OldStock": old_stock if old_stock is not None else old.get("Stock"),
                                "NewStock": new_stock,
                                "TimeStamp": ts
                            })
                    else:
                        cursor.execute("""
                                  INSERT INTO Produse (ID, OfferID, ProductName, Image, [Index], Page, TimeStamp, Categorie, RawPosition, Stock, Price,ReviewCount)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
                              """, (pid, offer_id, name, image, index, page, ts, cat, raw_pos, new_stock, price,reviewCount))

                    inserted_ids.append(pid)
            conn.commit()

            if audit_log:
                try:
                    requests.post("http://localhost:5000/audit-log", json=audit_log)
                except Exception as e:
                    app.logger.warning(f"[AUDIT FAIL] {e}")

        return jsonify({"ids": inserted_ids}), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/audit-log/by-category', methods=['GET'])
def get_audit_logs_by_category():
    try:
        categorie = request.args.get("categorie", "").strip()
        limit = int(request.args.get("limit", 10))
        offset = int(request.args.get("offset", 0))

        if not categorie:
            return jsonify({"error": "Categorie necesară"}), 400

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT p.ID, p.ProductName, p.Image, p.Categorie, p.Price,p.ReviewCount
                    FROM Produse p
                    INNER JOIN AuditLog a ON p.ID = a.ProductID
                    WHERE p.Categorie = ?
                """, (categorie,))
                all_products = cursor.fetchall()

                result = []
                for prod in all_products:
                    pid = str(prod[0])
                    pname = prod[1]
                    image = prod[2]
                    cat = prod[3]
                    price = prod[4]
                    reviewCount=prod[5]
                    cursor.execute("""
                        SELECT OldIndex, NewIndex, OldPage, NewPage,
                               OldRawPosition, NewRawPosition, OldStock, NewStock, TimeStamp
                        FROM AuditLog
                        WHERE ProductID = ?
                        ORDER BY TimeStamp DESC
                    """, (pid,))
                    raw_logs = cursor.fetchall()

                    logs = []
                    relevance = 0

                    for row in raw_logs:
                        old_index = row[0] or 0
                        new_index = row[1] or 0
                        old_page = row[2] or 0
                        new_page = row[3] or 0
                        old_raw = row[4]
                        new_raw = row[5]
                        old_stock = row[6]
                        new_stock = row[7]
                        ts = row[8]

                        logs.append({
                            "OldIndex": old_index,
                            "NewIndex": new_index,
                            "OldPage": old_page,
                            "NewPage": new_page,
                            "OldRawPosition": old_raw,
                            "NewRawPosition": new_raw,
                            "OldStock": old_stock,
                            "NewStock": new_stock,
                            "TimeStamp": convert_to_bucharest_time(ts),
                            "Price": price,
                            "ReviewCount":reviewCount
                        })

                        if old_page > 2 and new_page <= 2:
                            relevance += 100
                        elif old_page == 1 and new_page == 1 and abs(old_index - new_index) >= 5:
                            relevance += 50
                        elif old_page == 2 and new_page == 2 and abs(old_index - new_index) >= 5:
                            relevance += 25

                    if logs:
                        result.append({
                            "ProductID": pid,
                            "ProductName": pname,
                            "Image": image,
                            "Categorie": cat,
                            "AuditLog": logs,
                            "Relevance": relevance
                        })

                result.sort(key=lambda x: -x["Relevance"])
                total_count = sum(len(p["AuditLog"]) for p in result)
                paginated = result[offset:offset + limit]

                return jsonify({"data": paginated, "count": total_count}), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Audit fetch error: {str(e)}"}), 500

@app.route('/categorii/audit', methods=['GET'])
def get_distinct_audit_categories():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT p.Categorie
                    FROM Produse p
                    INNER JOIN AuditLog a ON p.ID = a.ProductID
                    WHERE p.Categorie IS NOT NULL AND p.Categorie != ''
                """)
                categories = [row[0] for row in cursor.fetchall()]
                return jsonify(categories), 200
    except Exception as e:
        return jsonify({"error": f"Categorie fetch error: {str(e)}"}), 500





@app.route('/categorii/audit-counts', methods=['GET'])
def get_audit_counts_per_category():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT p.Categorie, COUNT(*) AS TotalChanges
                    FROM Produse p
                    INNER JOIN AuditLog a ON p.ID = a.ProductID
                    WHERE p.Categorie IS NOT NULL AND p.Categorie != ''
                    GROUP BY p.Categorie
                """)
                results = [{"Categorie": row[0], "Count": row[1]} for row in cursor.fetchall()]
        return jsonify(results), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Audit count error: {str(e)}"}), 500

@app.route('/audit-log/all', methods=['GET'])
def get_all_audit_logs_grouped():
    try:
        limit = int(request.args.get("limit", 10))
        offset = int(request.args.get("offset", 0))

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT p.ID, p.ProductName, p.Image, p.Categorie, p.Price,p.ReviewCount
                    FROM Produse p
                    INNER JOIN AuditLog a ON p.ID = a.ProductID
                """)
                products = cursor.fetchall()

            result = []

            with conn.cursor() as cursor:
                for prod in products:
                    pid = str(prod[0])
                    pname = prod[1]
                    image = prod[2]
                    categorie = prod[3]
                    price = prod[4]
                    reviewCount=prod[5]

                    cursor.execute("""
                        SELECT OldIndex, NewIndex, OldPage, NewPage,
                               OldRawPosition, NewRawPosition, OldStock, NewStock, TimeStamp
                        FROM AuditLog
                        WHERE ProductID = ?
                        ORDER BY TimeStamp DESC
                    """, pid)

                    logs = []
                    relevance = 0

                    for row in cursor.fetchall():
                        old_index = row[0] or 0
                        new_index = row[1] or 0
                        old_page = row[2] or 0
                        new_page = row[3] or 0
                        old_raw = row[4]
                        new_raw = row[5]
                        old_stock = row[6]
                        new_stock = row[7]
                        ts = row[8]

                        logs.append({
                            "OldIndex": old_index,
                            "NewIndex": new_index,
                            "OldPage": old_page,
                            "NewPage": new_page,
                            "OldRawPosition": old_raw,
                            "NewRawPosition": new_raw,
                            "OldStock": old_stock,
                            "NewStock": new_stock,
                            "TimeStamp": convert_to_bucharest_time(ts),
                            "Price": price,
                            "ReviewCount":reviewCount
                        })

                        if old_page > 2 and new_page == 1:
                            relevance += 150
                        elif old_page > 2 and new_page == 2:
                            relevance += 100
                        elif old_page == 2 and new_page == 1:
                            relevance += 75
                        elif old_page == 1 and new_page == 1 and abs(old_index - new_index) >= 5:
                            relevance += 50
                        elif old_page == 2 and new_page == 2 and abs(old_index - new_index) >= 5:
                            relevance += 25

                    if logs and relevance > 0:
                        result.append({
                            "ProductID": pid,
                            "ProductName": pname,
                            "Image": image,
                            "Categorie": categorie,
                            "AuditLog": logs,
                            "Relevance": relevance
                        })

            result.sort(key=lambda x: -x["Relevance"])
            total_count = len(result)
            paginated = result[offset:offset + limit]

            return jsonify({"data": paginated, "count": total_count}), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Audit fetch error: {str(e)}"}), 500

@app.route('/audit-log', methods=['POST'])
def insert_audit_log():
    if not request.is_json:
        return jsonify({"error": "Conținutul trebuie să fie JSON"}), 400

    audit_entries = request.get_json()
    if not isinstance(audit_entries, list):
        return jsonify({"error": "Se așteaptă o listă de intrări de audit"}), 400

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for entry in audit_entries:
                    product_id = entry.get("ProductID")
                    old_index = entry.get("OldIndex")
                    new_index = entry.get("NewIndex")
                    old_page = entry.get("OldPage")
                    new_page = entry.get("NewPage")
                    old_raw_pos = entry.get("OldRawPosition")
                    new_raw_pos = entry.get("NewRawPosition")
                    old_stock = entry.get("OldStock")
                    new_stock = entry.get("NewStock")
                    ts_str = entry.get("TimeStamp")

                    if not product_id or not ts_str:
                        continue

                    ts = parser.isoparse(ts_str).astimezone(
                        pytz.timezone("Europe/Bucharest")
                    ).strftime('%Y-%m-%d %H:%M:%S')

                    cursor.execute("""
                        INSERT INTO AuditLog (
                            ProductID, OldIndex, NewIndex, OldPage, NewPage,
                            OldRawPosition, NewRawPosition, OldStock, NewStock, TimeStamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        product_id, old_index, new_index, old_page, new_page,
                        old_raw_pos, new_raw_pos, old_stock, new_stock, ts
                    ))
            conn.commit()

        return jsonify({"status": "Audit log inserat"}), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Eroare la inserarea audit log: {str(e)}"}), 500
@app.route('/produse/nume', methods=['GET'])
def get_all_product_names():
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ID, ProductName FROM Produse ORDER BY ProductName ASC")
                produse = [{"ID": str(row[0]), "ProductName": row[1]} for row in cursor.fetchall()]
        return jsonify(produse), 200
    except Exception as e:
        return jsonify({"error": f"Eroare la autocomplete: {str(e)}"}), 500
if __name__ == '__main__':
    app.run(debug=True)