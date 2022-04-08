import feedparser
import pymongo
import sys
import string
import sqlite3
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("matty", "Bravo-201"));
con = sqlite3.connect('contracts-finder-archive.db')
cursor = con.cursor()
for row in cursor.execute("SELECT * FROM notice_detail;"):
  db_local = pymongo.MongoClient("127.0.0.1", 27017)
  industry = db_local.industry
  industry.industry
  session = driver.session();
  session.run("MERGE (CONTACT: CONTACT {email: {email}})", {'email': row[6]});
  session.run("MERGE (TENDER: TENDER {title: {title}, contact: {contact}, description: {description}, contact: {contact}})", {"title": row[2], "contact": row[6], "description": row[3]});
  session.run("MATCH (TENDER) WHERE TENDER.contact CONTAINS '"+str(row[6])+"' MATCH (CONTACT) WHERE CONTACT.email CONTAINS '"+str(row[6])+"' MERGE (CONTACT)-[EMAIL:EMAIL]-(TENDER)");
  session.close();
  print("printed")
