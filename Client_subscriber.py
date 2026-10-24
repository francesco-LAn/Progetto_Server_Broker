import asyncio
import argparse

parser = argparse.ArgumentParser(description="Client subscriber: Ricevi messaggi dai pubblisher sottoscrivendoti ad un topic")

#*Impostazione dei valori richiesti:
parser.add_argument("broker",help="indirizzo IP del broker")
parser.add_argument("topic",help="topic a cui si riferisce il messaggio")

parser.parse_args()

PORTA = 5000
broker = args.broker
topic = args.topic