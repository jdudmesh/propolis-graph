import mysql.connector

users = {}

db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="CKYwALUCTIOnEsiNGTRoTiO",
  database="notthetalk"
)

curs = db.cursor()
curs.execute("select id, description from folder")
res = curs.fetchall()
for row in res:
  id = row[0]
  val = row[1]
  print(f"MERGE (:Tag{{extId='{id}', value='{val}'}})")

curs = db.cursor()
curs.execute("select id, email, username from user limit 10")
res = curs.fetchall()
for row in res:
  id = row[0]
  email = row[1]
  handle = row[2]
  print(f"MERGE (:Identity:User{{extId='{id}', email='{email}', handle='{handle}'}})")

curs.execute("select id, title, header, folder_id, user_id, created_date from discussion limit 10")
res = curs.fetchall()
for row in res:
  id = row[0]
  val = row[1] + "\n\n" + row[2]
  tag_id = row[3]
  user_id = row[4]
  created_at = row[5]

  val = val.replace("\n", "\\n").replace("\t", "\\t").replace("'", "\'").replace("\"", "\\\"")
  print(f"""MERGE (:Identity:User{{extId='{user_id}'}})-[posted{{created_at:'{created_at}'}}]->(:Post{{extId='{id}', value='{val}'}})-[is_tagged_with]->(:Tag{{extId: '{tag_id}'}})""")


curs.execute("select id, text, user_id, created_date, discussion_id from post limit 10")
res = curs.fetchall()
for row in res:
  id = row[0]
  val = row[1]
  user_id = row[2]
  created_at = row[3]
  in_reply_to_id = row[4]

  val = val.replace("\n", "\\n").replace("\t", "\\t").replace("'", "\'").replace("\"", "\\\"")
  print(f"""MERGE (:Identity:User{{extId='{user_id}'}})-[posted{{created_at:'{created_at}'}}]->(:Post{{extId='{id}', inReplyToExtId='{in_reply_to_id}', value='{val}'}})""")
