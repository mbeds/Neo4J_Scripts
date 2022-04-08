import csv
import sys

from neo4j.v1 import GraphDatabase, basic_auth

with open(sys.argv[1], 'r') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:

    application_received = row["application_received"];
    address = row['address'];
    proposal = row['proposal'];
    status = row['status'];    
    #application_type = row['application_type'];
    case_officer = row['case_officer'];
    parish = row['parish'];
    ward = row['ward'];
    applicant_name = row['applicant_name'];
    applicant_address = row['applicant_address'];
    agent_name = row['agent_name'];
    agent_company_name = row['agent_company_name'];
    agent_address = row['agent_address'];
    agent_email = row['agent_address'];
    agent_phone = row['agent_phone'];

    driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=basic_auth("neo4j", "Bravo-201"));
    
    session = driver.session();

    session.run("MERGE (:AGENT { \
        agent_name: {agent_name}})", \
        {'agent_name': agent_name})

    session.run("MERGE (:PLAN { \
        agent_company_name: {agent_company_name}, \
        address: {address}, \
        proposal: {proposal}, \
        status: {status}, \
        case_officer: {case_officer}, \
        parish:{parish}, \
        ward: {ward}, \
        applicant_name: {applicant_name}, \
        applicant_address: {applicant_address}, \
        agent_name: {agent_name}, \
        agent_address: {agent_address}, \
        agent_phone: {agent_phone}, \
        agent_email: {agent_email}, \
        agent_company_name: {agent_company_name}})", \

        {"address": address, \
         "proposal": proposal, \
         "status": status,\
         "case_officer": case_officer, \
         "parish": parish, \
         "ward": ward, \
         "applicant_name": applicant_name, \
         "applicant_address": applicant_address, \
         "agent_name": agent_name, \
         "agent_phone": agent_phone, \
         "agent_email": agent_email, \
         "agent_company_name": agent_company_name, \
         "agent_address": agent_address})

    session.run('MATCH (AGENT) where AGENT.agent_name = "'+agent_name+'" MATCH (PLAN) where PLAN.agent_name = "'+agent_name+'" MERGE (AGENT)-[Link:Link]-(PLAN)');
    session.close();
    print(proposal)
