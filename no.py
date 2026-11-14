import time
import random
import asyncio
import argparse
import grpc
import no_pb2
import no_pb2_grpc

from concurrent import futures
from enum import StrEnum

DEBUG = True
QTD_NOS = 4
QUORUM = 3
TEMPO_HEARTBEAT = 0.1 if not DEBUG else 5
TEMPO_ELEICAO = 0.3 if not DEBUG else 10

estado = StrEnum('estado', ['seguidor', 'candidato', 'lider'])

class No(no_pb2_grpc.NoServicer):
    def __init__(self, id):
        self.id = id
        self.lider_id = None
        self.termo_atual = 0
        self.indice_log = 0
        self.indice_commit = 0
        self.votos_recebidos = 0
        self.votou = False

        self.nos = {id: no_pb2_grpc.NoStub(channel) for id, channel in channels.items()}
        self.heartbeat_task = None
        self.eleicao_task = None
        self.eleicao_event = asyncio.Event()

        self.VirarSeguidor(termo=0)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.eleicao_task:
            self.eleicao_task.cancel()

    async def RequestVote(self, request, context):
        if request.termo < self.termo_atual:
            return no_pb2.VoteResponse(id=self.id, termo=self.termo_atual, voto=False)

        # Termo mudou: pode votar novamente
        if request.termo > self.termo_atual:
            self.votou = False

        if not self.votou:
            self.VirarSeguidor(request.termo, request.id)
            self.votou = True
            return no_pb2.VoteResponse(id=self.id, termo=self.termo_atual, voto=True)
        
        return no_pb2.VoteResponse(id=self.id, termo=self.termo_atual, voto=False)

    async def Heartbeat(self, request, context):
        self.VerificarTermo(request.id, request.termo)

        # Implementação do método Heartbeat
        
        return no_pb2.HeartbeatResponse(
            id=self.id,
            termo=self.termo_atual,
            indice_log=self.indice_log
        )

    def VirarLider(self):
        self.estado_atual = estado.lider
        self.lider_id = None

        self.EncerrarEleicaoLoop()

        print(f"\nNó {self.id} se tornou líder no termo {self.termo_atual}")

    def VirarSeguidor(self, termo, lider_id=None):
        self.estado_atual = estado.seguidor
        self.lider_id = lider_id
        self.termo_atual = termo

        self.IniciarHearbeatLoop()
        self.IniciarEleicaoLoop()

        print(f"\nNó {self.id} se tornou seguidor no termo {self.termo_atual}")

    async def IniciarEleicao(self):
        print("\nEleição iniciou")
        self.estado_atual = estado.candidato
        self.termo_atual += 1
        self.votos_recebidos = 1
        self.votou = True

        self.EncerrarHeartbeatLoop()

        for id, no in self.nos.items():
            try:
                response = await no.RequestVote(no_pb2.VoteRequest(
                    id=self.id,
                    termo=self.termo_atual,
                    indice_log=self.indice_log
                ))

                if (self.VerificarTermo(response.id, response.termo)):
                    return

                if response.voto:
                    self.votos_recebidos += 1
            except grpc.RpcError as e:
                print(f"Erro ao solicitar voto para o nó {id}")

        if self.votos_recebidos >= QUORUM:
            print("\nEleito como líder!")
            self.VirarLider()
        else:
            print("\nNão obteve votos suficientes.")

    async def HeartbeatLoop(self):
        print("\nHeartbeat iniciou")
        exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider_id)
        
        while True:
            await asyncio.sleep(TEMPO_HEARTBEAT)

            try:
                if self.estado_atual == estado.seguidor and self.lider_id is not None:
                    response = await self.nos[self.lider_id].Heartbeat(no_pb2.HeartbeatRequest(
                        id=self.id,
                        termo=self.termo_atual,
                        indice_log=self.indice_log
                    ))

                    # Sinaliza que o líder está ativo
                    self.eleicao_event.set()
                    
                    self.VerificarTermo(response.id, response.termo)

                    # Atualizar logs            
                    exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider_id)
            except grpc.RpcError as e:
                print(f"\nErro ao enviar heartbeat para o líder")

    async def EleicaoLoop(self):
        while True:
            timeout = TEMPO_ELEICAO + random.uniform(0, 0.1)

            try:
                # Aguarda o timeout, reinicia o temporizador se receber heartbeat do líder
                await asyncio.wait_for(self.eleicao_event.wait(), timeout=timeout)
                self.eleicao_event.clear()
            except asyncio.TimeoutError:
                # Se o temporizador expirar, inicia a eleição
                await self.IniciarEleicao()
                exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider_id)
    
    def IniciarHearbeatLoop(self):
        if self.heartbeat_task is None or self.heartbeat_task.done():
            self.heartbeat_task = asyncio.create_task(self.HeartbeatLoop())

    def EncerrarHeartbeatLoop(self):
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
    
    def IniciarEleicaoLoop(self):
        if self.eleicao_task is None or self.eleicao_task.done():
            self.eleicao_event.clear()
            self.eleicao_task = asyncio.create_task(self.EleicaoLoop())

    def EncerrarEleicaoLoop(self):
        if self.eleicao_task and not self.eleicao_task.done():
            self.eleicao_task.cancel()

    def VerificarTermo(self, id, termo):
        if termo > self.termo_atual:
            print(f"\nTermo maior do nó {id}. Voltando a ser seguidor.")
            self.VirarSeguidor(termo, id)
            return True
        
        return False

def exibir_estado(estado_atual, termo_atual, indice_commit, lider=None):
    print(f"===============================")
    print(time.strftime("%H:%M:%S", time.localtime()))
    print(f"Papel: {estado_atual}")
    print(f"Termo: {termo_atual}")
    print(f"Índice de Commit: {indice_commit}")
    print(f"Líder: {lider}")

def calcular_porta(id):
    return 50051 + id

channels = {}

parser = argparse.ArgumentParser(description="No")
parser.add_argument("--id", type=int, required=True, help="ID do nó")

async def main():
    args = parser.parse_args()

    if args.id < 0 or args.id >= QTD_NOS:
        print(f"ID do nó deve estar entre 0 e {QTD_NOS - 1}")
        exit(1)

    for id in range(QTD_NOS):
        if id != args.id:
            channels[id] = grpc.aio.insecure_channel(f"localhost:{calcular_porta(id)}")

    server = grpc.aio.server()
    no_pb2_grpc.add_NoServicer_to_server(No(id=args.id), server)
    server.add_insecure_port(f"[::]:{calcular_porta(args.id)}")
    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        print("Servidor interrompido")
        await server.stop(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Encerrando")