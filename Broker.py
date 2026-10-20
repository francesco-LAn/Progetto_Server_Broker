import asyncio


class Broker(asyncio):
    def __init__(self,ip,port):
        self.ip = ip    
        self.port = port
        self.topics = {}

    async def gestione_messaggi(self,reader,writer):
        dati = self.reader.readline()                   #Ricevo dati, sia quelli dal pub che sub




    async def run(self):
        server = await asyncio.start_server(self.gestione_messaggi,self.ip,self.port)
        async with server:
            await server.serve_forever()


# MAIN
broker = Broker('',5000)
asyncio.run(broker.run())