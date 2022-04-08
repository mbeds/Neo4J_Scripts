import feedparser
import sys
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("matty", "Bravo-201"));

d = feedparser.parse('https://nhssbs.eu-supply.com/ctm/rss/Rss.ashx?days=30&b=NHSSBS')

for post in d.entries:
    session = driver.session();
    session.run("MERGE (CONTACT: CONTACT {email: {email}})", {'email': post.contactperson["email"]});
    session.run(
        "MERGE (TENDER: TENDER {title: {title}, link: {link}, description: {description}, email: {email}})",
        {"title": post.title, "link": post.link, "description": post.detaileddescription, "email": post.contactperson["email"]});
    session.run("MATCH (TENDER) WHERE TENDER.email CONTAINS '" + post.contactperson["email"] + "' MATCH (CONTACT) WHERE CONTACT.email CONTAINS '" + post.contactperson["email"] + "' MERGE (TENDER)-[link:link]-(CONTACT)");
    session.close();
    print(post.contactperson["email"] + ": "+ post.title + ": " + post.link + ": " + post.detaileddescription + ": " +post.description)
