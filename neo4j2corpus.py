import os
from neo4j import GraphDatabase
import nltk
nltk.download('punkt')

# Connect to Neo4j database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "BRavo-201"))
session = driver.session()

# Define the directory containing text files
directory = "data"

# Iterate over all text files in the directory
for filename in os.listdir(directory):
    with open(os.path.join(directory, filename), "r") as file:
        text = file.read()
        # Tokenize the text
        tokens = nltk.word_tokenize(text)
        # Iterate over all tokens and create a new node for each token
        # and a relationship between the token node and the text file node
        for token in tokens:
            session.run("CREATE (t:Token {value: $value})-[:PART_OF]->(f:Text {name: $name, text: $text})", value=token, name=filename, text=text)

# Close Neo4j session
session.close

