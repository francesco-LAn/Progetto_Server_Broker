import asyncio,json


class Broker:
    def __init__(self,ip,port):
        self.ip = ip    
        self.port = port
        self.topics = {}            #Lista per tenere conto dei topic e quali client sono sottoscritti struttura: {"topic":topic,"messaggio":messaggio}
        self.subscriber = {}        #Lista per sapere a quali topic è sottoscritto ogni client, è l'opposto di quella sopram, ma utile per eliminare velocemente alla disconnessione

    async def gestione_messaggi(self,reader,writer):
        ip, porta = writer.get_extra_info("peername")   #Ottengo informazioni per identificare il singolo client
        try:
            while True:
                dati = await reader.readline()           #Ricevo dati, sia quelli dal pub che sub
                
                if dati:                                #Se il dato ricevuto non è vuoto    
                    messaggio_scomposto = (dati.decode()).split(":")

                    if messaggio_scomposto[0] == "sub":
                        await self.messaggio_x_sub(messaggio_scomposto,writer)

                    elif messaggio_scomposto[0] == "pub":
                        await self.messaggio_x_pub(messaggio_scomposto)
                    

                else:
                    break                               #Altrimenti esco ed eseguo il blocco finally

        except (ConnectionError, KeyboardInterrupt):
            pass

        finally:
            print(f"Client disconnesso: {porta}")
            #ELIMINAZIONE CLIENT DALLE LISTE --- SENZA BLOCCHI O CONTROLLI COME NEI THREAD, QUI NON HO RISCHI SE POSIZIONO BENE AWAIT
            try:
                lista_topic_iscritti = self.subscriber[writer]      #Ottengo la lista dei topic a cui è iscitto questo client, può dare errore se il clinet non si è mai SUBSCRIPTATAO: KeyError
                for topic in lista_topic_iscritti:                  #Scorro i topic ottenuti
                    self.topics[topic].remove(writer)               #Rimuovo da questa lista l'oggetto writer

                del self.subscriber[writer]                         #Elimino anche la chiave dall'altro dizionario, ovviamente dopo averlo usato                

            except KeyError:
                pass
 
            writer.close()                                          #ALLA FINE CHIUDO PROPRIO IL SOCKET


    async def messaggio_x_sub(self,lista_messaggio,writer_client):          #Metodo che assegna al topic il socket del client connesso, che lo ha richiesto
        topic = lista_messaggio[1].strip()          #*Rimuovo \n
        if topic in self.topics:                    #Se il topic esiste già
            if writer_client not in self.topics[topic]:     #!Se il writer non è già nella lista (evita duplicati)
                self.topics[topic].append(writer_client)  #Aggiungo alla lista di socket per quel topic, il writer del client attuale che lo ha richiesto 
                                                        #Devo passarlo come argomento al metodo, perché di istanza di Broker ce nè una sola e non posso quindi salvare
                                                        # writer come attributo suo                                                        
                
        else:   
            self.topics[topic] = [writer_client]    #Altrimenti creo la nuova lista per il topic    


        if writer_client in self.subscriber:        #Se questo writer è già tra le chiavi del dizionari
            self.subscriber[writer_client].append(topic)   #Aggiungo il topic alla lista di quelli a cui è connesso questo client

        else:
            self.subscriber[writer_client] = [topic]        #Altrimenti la creo


    async def messaggio_x_pub(self,lista_messaggio):
        topic = lista_messaggio[1]
        #Il messsaggio avrà ancora \n alla fine, va tolto per il confronto
        topic.strip()
        print(self.topics)
        print(topic)

        #Il messaggio inviato ha una struttura precisa, una stringa fata come un dizionario
        messaggio_pronto = json.dumps({"topic":topic,"messaggio":lista_messaggio[2]})      
        
        
        for topic_lista in self.topics:
            if topic == topic_lista:        #*Quando trovo la corrispondenza tra i topic per i quali c'è almeno un client sottoscritto
                for client in self.topics[topic_lista]:  #Scorro la lista di client subscriptati a quel TOPICs
                    try:
                        print("Inivo messaggio")
                        client.write((messaggio_pronto+"\n").encode()) #Client corrisponde al writer_client, che si trova dentro alle liste grazie alla funzione di sub
                        await client.drain()

                    except ConnectionError:
                        print("errore nell'invio")
                        pass



    async def run(self):
        server = await asyncio.start_server(self.gestione_messaggi,self.ip,self.port)
        print("Server partito")
        async with server:
            await server.serve_forever()


# MAIN
broker = Broker('127.0.0.1',5000)
try:
    asyncio.run(broker.run())

except KeyboardInterrupt:
    print("Server chiuso")