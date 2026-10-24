import asyncio
import argparse

parser = argparse.ArgumentParser(description="Client publisher: invia un messaggio a un topic del broker")      #Creo l'oggetto argparse per leggere la linea di comando

#*Settaggio dei valori richiesti all'avvio del programma:
parser.add_argument("broker", help="indirizzo IP del broker")               #Il broker, cioè l'indirizzo          
parser.add_argument("topic", help="topic a cui si riferisce il messaggio")  # Il topic di riferimento per l'invio
parser.add_argument("messaggio", help="testo del messaggio (tra virgolette se contiene spazi)") #Il messaggio da inviare
#Con -h ottengo le descrizioni qui scritte in help

args = parser.parse_args()      #Faccio partire l'esecuzione del parser

PORTA = 5000
broker = args.broker
topic = args.topic
messaggio = argsm.messaggio

async def main(broker,topic,messaggio):
    reader, writer = asyncio.open_connection(broker,PORTA)

    writer.write( (f"pub:{topic}:{messaggio}\n").encode() )
    await writer.drain()

    writer.close()


asyncio.run(main(broker,topic,messaggio))
