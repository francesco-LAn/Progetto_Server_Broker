import asyncio
import argparse,json

parser = argparse.ArgumentParser(description="Client subscriber: Ricevi messaggi dai pubblisher sottoscrivendoti ad un topic")

#*Impostazione dei valori richiesti:
parser.add_argument("broker",help="indirizzo IP del broker")    
parser.add_argument("topic",nargs = "+",help="topic a cui si riferisce il messaggio")   #Tramite narg posso far inserire più valori
                                                                                        #in questo modo il subscribing avviene su più topic
args = parser.parse_args()

PORTA = 5000
broker = args.broker
topics = args.topic

async def main(broker,topics):
    reader, writer = await asyncio.open_connection(broker,PORTA)

    try: 
        for t in topics:
            writer.write( (f"sub:{t}\n").encode() )     #Invio uno alla volta la sottoscrizzione ai vari topic
            await writer.drain()

        while True:
            dati = await reader.readline()
            if dati:
                messaggio_pronto = json.loads(dati.decode())
                print(f"MESSAGGIO RICEVUTO!: {messaggio_pronto["messaggio"]} ottenuto dal TOPIC:{messaggio_pronto["topic"]} ")

            else:
                break
        
    except (ConnectionError, KeyboardInterrupt):
        pass

    finally:
        writer.close()

try:
    asyncio.run(main(broker, topics))
except KeyboardInterrupt:
    print("Connessione chiusa")