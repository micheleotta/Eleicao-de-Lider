import time
import random
import asyncio
import argparse
from urllib import request
import grpc
import no_pb2
import no_pb2_grpc

from concurrent import futures
from enum import StrEnum

DEBUG = True
QTD_NOS = 4
QUORUM = 3

TEMPO_HEARTBEAT = 0.5 if not DEBUG else 2
TEMPO_ELEICAO = 0.3 if not DEBUG else 10

# Líder mantém um Heartbeat Timeout para cada réplica
HEARTBEAT_TIMEOUT = 0.5 if not DEBUG else 3.5

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
        self.voted_for = None

        self.nos = {id: no_pb2_grpc.NoStub(channel) for id, channel in channels.items()}
        self.heartbeat_task = None
        self.eleicao_task = None
        self.monitor_task = None
        self.heartbeat_event = asyncio.Event()
        
        # Monitoramento
        self.ultimo_fetch = {}                              # Último FetchRequest recebido de cada réplica
        self.indices_replicas = {id: 0 for id in channels}  # Último índice de log informado por cada réplica
        self.replicas_inativas = set()

        self.VirarSeguidor(termo=0)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        if self.eleicao_task:
            self.eleicao_task.cancel()
        if self.monitor_task:
            self.monitor_task.cancel()

    async def RequestVote(self, request, context):
        '''Solicitação de Voto'''
        
        if request.termo < self.termo_atual:
            return no_pb2.VoteResponse(id=self.id, termo=self.termo_atual, voto=False)

        # Voto único por termo
        if request.termo > self.termo_atual:
            # self.votou = False
            self.VirarSeguidor(request.termo)

        # Um nó só concede seu voto a um candidato se o log do candidato
        # for tão ou mais atualizado que o seu próprio log
        if (not self.votou or self.voted_for == request.id) and request.indice_log >= self.indice_log:
            # self.VirarSeguidor(request.termo, request.id) # mudei
            self.voted_for = request.id
            self.votou = True
            self.VirarSeguidor(request.termo, request.id)
            print(f"\n[Eleição] Votou para o nó {request.id} no termo {request.termo}")
            return no_pb2.VoteResponse(id=self.id, termo=self.termo_atual, voto=True)
        
        return no_pb2.VoteResponse(id=self.id, termo=self.termo_atual, voto=False)

    async def Heartbeat(self, request, context):
        '''Recebe FetchRequest/Heartbeat de uma réplica'''
        
        # Se recebeu termo obsoleto -> rejeita
        if request.termo < self.termo_atual:
            return no_pb2.HeartbeatResponse(
                id=self.id,
                termo=self.termo_atual,
                indice_log=self.indice_log,
                sucesso=False, # rejeita a requisição
                lider_id=self.lider_id
            )
        
        # Se recebeu termo maior -> vira seguidor
        if self.VerificarTermo(request.id, request.termo):
            return no_pb2.HeartbeatResponse(
                id=self.id,
                termo=self.termo_atual,
                indice_log=self.indice_log,
                sucesso=False,
                lider_id=None
            )
        
        # Apenas líder atende FetchRequests
        if self.estado_atual != estado.lider:
            return no_pb2.HeartbeatResponse(
                id=self.id,
                termo=self.termo_atual,
                indice_log=self.indice_log,
                sucesso=False,
                lider_id=self.lider_id
            )
        
        # Se lider:
        # Registra a atividade da réplica
        self.ultimo_fetch[request.id] = time.monotonic()
        # Registra o índice de log informado
        self.indices_replicas[request.id] = request.indice_log
        print(f"[♡] Heartbeat recebido -> {request.id}")
        return no_pb2.HeartbeatResponse(
            id=self.id,
            termo=self.termo_atual,
            indice_log=self.indice_log,
            sucesso=True,
            lider_id=self.id
        )

    def VirarLider(self):
        self.estado_atual = estado.lider
        self.lider_id = self.id

        self.EncerrarEleicaoLoop()
        self.ultimo_fetch.clear()
        self.replicas_inativas = set()

        self.IniciarMonitoramentoLoop()
        print(f"\nNó {self.id} se tornou líder no termo {self.termo_atual}")

    def VirarSeguidor(self, termo, lider_id=None):
        if termo > self.termo_atual:
            self.voted_for = None
            self.votou = False
        
        self.estado_atual = estado.seguidor
        self.lider_id = lider_id
        self.termo_atual = termo

        self.EncerrarMonitoramentoLoop()
        self.heartbeat_event.clear()
        self.IniciarHeartbeatLoop()
        self.IniciarEleicaoLoop()

        if self.lider_id:
            print(f"\nNó {self.id} se tornou seguidor de {self.lider_id} no termo {self.termo_atual}")
        else:
            print(f"Nó {self.id} se tornou seguidor")

    async def IniciarEleicao(self):
        print(f"\n[Eleição] {self.id} iniciou uma eleição")
        self.estado_atual = estado.candidato
        self.lider_id = None
        self.termo_atual += 1
        self.voted_for = self.id
        self.votos_recebidos = 1
        self.votou = True
        
        termo_eleicao = self.termo_atual

        for id, no in self.nos.items():
            try:
                response = await no.RequestVote(
                    no_pb2.VoteRequest(
                        id=self.id,
                        termo=termo_eleicao,
                        indice_log=self.indice_log
                    ))

                # Atualização de termo se resposta tiver termo maior  -> precisa desse?
                if (self.VerificarTermo(response.id, response.termo)):
                    return
                
                # Ignora respostas de uma eleição anterior
                if self.termo_atual != termo_eleicao:
                    return

                if response.voto:
                    self.votos_recebidos += 1
                    print(f"[Eleição] Nó {self.id} recebeu voto de {id} (total: {self.votos_recebidos})")
            
            except grpc.RpcError as e:
                print(f"Erro ao solicitar voto para o nó {id}: {e}")

        # Verifica se ainda é candidato, ou teve atualização
        if self.estado_atual == estado.candidato and self.termo_atual == termo_eleicao:
            if self.votos_recebidos >= QUORUM:
                print("\n[Eleição] Eleito como líder!")
                self.VirarLider()
            else:
                print(f"\n[Eleição] Não obteve votos suficientes: {self.votos_recebidos}/{QUORUM}")

    async def HeartbeatLoop(self):
        print("\n[♡] Heartbeat iniciou")
        exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider_id)
        
        while True:
            await asyncio.sleep(TEMPO_HEARTBEAT)

            try:
                if self.estado_atual == estado.seguidor and self.lider_id is not None:
                    response = await self.nos[self.lider_id].Heartbeat(
                        no_pb2.HeartbeatRequest(
                            id=self.id,
                            termo=self.termo_atual,
                            indice_log=self.indice_log
                        ))
                    
                    # Resposta com termo maior
                    if self.VerificarTermo(response.id, response.termo):
                        continue
                    
                    # Líder ativo se resposta aceita
                    if(response.sucesso and response.termo == self.termo_atual):
                        # Sinaliza que o líder está ativo
                        self.heartbeat_event.set()
                        print(f"[♡] Líder ativo: {self.lider_id}")

                    # Atualizar logs
                    exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider_id)
            except grpc.RpcError as e:
                print(f"\nErro ao enviar heartbeat para o líder {self.lider_id}: {e}")

    async def EleicaoLoop(self):
        while True:
            # Temporizadores de eleição aleatórios -> evitar que enviem ao mesmo tempo
            timeout = TEMPO_ELEICAO + random.uniform(0, 0.2)

            try:
                # Aguarda o timeout, reinicia o temporizador se receber heartbeat do líder
                await asyncio.wait_for(self.heartbeat_event.wait(), timeout=timeout)
                # Se recebeu resposta válida do líder, reinicia o temporizador
                self.heartbeat_event.clear()
            
            except asyncio.TimeoutError:
                if self.estado_atual != estado.lider:
                    # Se o temporizador expirar, inicia a eleição
                    await self.IniciarEleicao()
                    exibir_estado(self.estado_atual, self.termo_atual, self.indice_commit, self.lider_id)
    
    def IniciarHeartbeatLoop(self):
        if self.heartbeat_task is None or self.heartbeat_task.done():
            self.heartbeat_task = asyncio.create_task(self.HeartbeatLoop())

    def EncerrarHeartbeatLoop(self):
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
    
    def IniciarEleicaoLoop(self):
        if self.eleicao_task is None or self.eleicao_task.done():
            self.heartbeat_event.clear()
            self.eleicao_task = asyncio.create_task(self.EleicaoLoop())

    def EncerrarEleicaoLoop(self):
        if self.eleicao_task and not self.eleicao_task.done():
            self.eleicao_task.cancel()

    def VerificarTermo(self, id, termo):
        '''Verificação do termo. Retorna True se houve atualização'''
        if termo > self.termo_atual:
            print(f"\nTermo maior do nó {id}. Voltando a ser seguidor.")
            self.VirarSeguidor(termo)
            return True
        
        return False
    
    def ReplicasAtivas(self):
        '''Retorna as réplicas que enviaram FetchRequest dentro do Heartbeat Timeout'''
        
        agora = time.monotonic()
        replicas_ativas = []

        for id, ultimo_fetch in self.ultimo_fetch.items():
            if agora - ultimo_fetch <= HEARTBEAT_TIMEOUT:
                replicas_ativas.append(id)

        return replicas_ativas

    def TemQuorum(self):
        '''Verifica se o líder possui quórum de nós ativos'''
        
        if self.estado_atual == estado.lider:
            # O próprio líder conta como um nó ativo -> ?
            nos_ativos = 1 + len(self.ReplicasAtivas())
            return nos_ativos >= QUORUM
        return False
    
    async def MonitorarReplicasLoop(self):
        '''Monitoramento de inatividade das réplicas pelo líder'''
        print("\n[MONITOR] Monitoramento de réplicas iniciado")

        while self.estado_atual == estado.lider:
            await asyncio.sleep(TEMPO_HEARTBEAT)
            replicas_ativas = set(self.ReplicasAtivas())
            replicas_inativas = set(self.nos.keys()) - replicas_ativas

            # Exibe somente quando houver alteração
            if replicas_inativas != self.replicas_inativas:
                self.replicas_inativas = replicas_inativas
                print(f"\nRéplicas ativas: {sorted(replicas_ativas)}")
                print(f"Réplicas inativas: {sorted(replicas_inativas)}")

                if not self.TemQuorum():
                    print("\n[ALERTA] Líder sem quórum!")

    def IniciarMonitoramentoLoop(self):
        if self.monitor_task is None or self.monitor_task.done():
            self.monitor_task = asyncio.create_task(self.MonitorarReplicasLoop())
    
    def EncerrarMonitoramentoLoop(self):
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
    
    
    # Operações com o cliente
    async def ClientWrite(self, request, context):
        if self.estado_atual != estado.lider:
            return no_pb2.WriteResponse(
                sucesso = False,
                lider_id = self.lider_id,
                mensagem = f"falha ao gravar dado -> o lider atual é {self.lider_id}"
            )
        
        if not self.TemQuorum():
            return no_pb2.WriteResponse(
                sucesso = False,
                lider_id = self.lider_id,
                mensagem = "falha ao gravar dado -> não há quorum suficiente"
            )
        
        # TODO: Implementar gravação de dados no log do líder e replicação para os seguidores
        
        # se tudo certo
        return no_pb2.WriteResponse(
            sucesso = True,
            lider_id = self.lider_id,
            mensagem = "dado gravado com sucesso!"
        )
    
    async def ClientRead(self, request, context):
        # TODO: Implementar leitura de dados
        return no_pb2.ReadResponse(
            sucesso = True,
            log_index = 0,
            termo = 0,
            dado = "",
            commit_index = 0
        )


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