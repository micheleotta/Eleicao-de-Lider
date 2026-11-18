import time
import random
import asyncio
import argparse
import grpc
import no_pb2
import no_pb2_grpc
import bd.bd as bd

from concurrent import futures
from enum import StrEnum

DEBUG = True
QTD_NOS = 4
QUORUM = 3

TEMPO_FETCH = 0.5 if not DEBUG else 2
ELEICAO_TIMEOUT = 1.5 if not DEBUG else 10

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
        self.voted_for = None

        self.nos = {id: no_pb2_grpc.NoStub(channel) for id, channel in channels.items()}
        self.fetch_task = None
        self.eleicao_task = None
        self.monitor_task = None
        self.sucesso_fetch = asyncio.Event()
        
        # Monitoramento
        self.ultimo_fetch = {}                              # Último FetchRequest recebido de cada réplica
        self.indices_replicas = {id: 0 for id in channels}  # Último índice de log informado por cada réplica
        self.replicas_inativas = set()

        self.IniciarBD()
        self.VirarSeguidor(termo=self.termo_atual if self.termo_atual else 0)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        bd.update_estado(self.id, self.termo_atual, self.voted_for)
        if self.fetch_task:
            self.fetch_task.cancel()
        if self.eleicao_task:
            self.eleicao_task.cancel()
        if self.monitor_task:
            self.monitor_task.cancel()

    def IniciarBD(self):
        '''Inicia os bancos de dados do nó'''
        bd.initialize_bd(self.id)
        estado = bd.get_estado(self.id)
        self.indice_log = bd.get_last_log_index(self.id)
        self.indice_commit = bd.get_last_commit_index(self.id)

        if (estado):
            print(f"\n\nEstado recuperado: \n- termo: {estado.get('termo')}\n- voted_for: {estado.get('voted_for')}")
            self.termo_atual = estado.get("termo")
            self.voted_for = estado.get("voted_for")

    async def RequestVote(self, request, context):
        '''Solicitação de Voto'''
        
        if request.termo < self.termo_atual:
            return no_pb2.VoteResponse(id=self.lider_id, termo=self.termo_atual, voto=False)

        # Voto único por termo
        if request.termo > self.termo_atual:
            self.VirarSeguidor(request.termo)

        # Um nó só concede seu voto a um candidato se o log do candidato
        # for tão ou mais atualizado que o seu próprio log
        if (not self.voted_for or self.voted_for == request.id) and request.indice_log >= self.indice_log:
            self.voted_for = request.id
            self.VirarSeguidor(request.termo, request.id)
            self.sucesso_fetch.set() # Conceder voto reinicia temporizador de eleição
            bd.update_estado(self.id, self.termo_atual, self.voted_for)
            print(f"\n[Eleição] Votou para o nó {request.id} no termo {request.termo}")
            return no_pb2.VoteResponse(id=self.lider_id, termo=self.termo_atual, voto=True)
        
        return no_pb2.VoteResponse(id=self.lider_id, termo=self.termo_atual, voto=False)

    async def Fetch(self, request, context):
        '''Recebe FetchRequest de uma réplica'''
        
        # Se recebeu termo obsoleto -> rejeita
        if request.termo < self.termo_atual:
            return no_pb2.FetchResponse(
                id = self.id,
                termo = self.termo_atual,
                indice_log = self.indice_log,
                sucesso = False, # rejeita a requisição
                lider_id = self.lider_id,
                commit_index = self.indice_commit
            )
        
        # Se recebeu termo maior -> vira seguidor
        if self.VerificarTermo(request.id, request.termo):
            return no_pb2.FetchResponse(
                id = self.id,
                termo = self.termo_atual,
                indice_log = self.indice_log,
                sucesso = False,
                lider_id = None,
                commit_index = self.indice_commit
            )
        
        # Apenas líder atende FetchRequests
        if self.estado_atual != estado.lider:
            return no_pb2.FetchResponse(
                id = self.id,
                termo = self.termo_atual,
                indice_log = self.indice_log,
                sucesso = False,
                lider_id = self.lider_id,
                commit_index = self.indice_commit
            )
        
        # Se lider:
        # Registra a atividade da réplica
        self.ultimo_fetch[request.id] = time.monotonic()
        # Registra o índice de log informado
        self.indices_replicas[request.id] = request.indice_log
        # Envia log completo para permitir comparação
        logs = bd.get_log_entries(self.id, 1)
        
        print(f"[♡] Heartbeat recebido -> {request.id}")
        return no_pb2.FetchResponse(
            id = self.id,
            termo = self.termo_atual,
            indice_log = self.indice_log,
            sucesso = True,
            lider_id = self.id,
            commit_index = self.indice_commit,
            logs = logs
        )

    def VirarLider(self):
        self.estado_atual = estado.lider
        self.lider_id = self.id

        self.EncerrarEleicaoLoop()
        self.ultimo_fetch.clear()
        self.replicas_inativas = set()

        self.IniciarMonitoramentoLoop()

        # Persiste o estado do nó
        bd.update_estado(self.id, self.termo_atual, self.voted_for)

        print(f"\nNó {self.id} se tornou líder no termo {self.termo_atual}")

    def VirarSeguidor(self, termo, lider_id=None):
        if termo > self.termo_atual:
            self.voted_for = None
        
        self.estado_atual = estado.seguidor
        self.lider_id = lider_id
        self.termo_atual = termo

        self.EncerrarMonitoramentoLoop()
        self.sucesso_fetch.set()
        self.IniciarFetchLoop()
        self.IniciarEleicaoLoop()

        # Persiste o estado do nó
        bd.update_estado(self.id, self.termo_atual, self.voted_for)

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
        
        termo_eleicao = self.termo_atual

        for id, no in self.nos.items():
            try:
                response = await no.RequestVote(
                    no_pb2.VoteRequest(
                        id=self.id,
                        termo=termo_eleicao,
                        indice_log=self.indice_log
                    ))

                # Se resposta tiver termo maior, se torna seguidor
                if (self.VerificarTermo(response.id, response.termo)):
                    return
                
                # Ignora respostas de uma eleição anterior
                if self.termo_atual != termo_eleicao:
                    return

                if response.voto:
                    self.votos_recebidos += 1
                    print(f"[Eleição] Nó {self.id} recebeu voto de {id} (total: {self.votos_recebidos})")
            
            except grpc.RpcError as e:
                if (e.code() == grpc.StatusCode.UNAVAILABLE):
                    print(f"Erro ao solicitar voto para o nó {id}: indisponível")
                else:
                    print(f"Erro ao solicitar voto para o nó {id}: {e}")

        # Verifica se ainda é candidato, ou teve atualização
        if self.estado_atual == estado.candidato and self.termo_atual == termo_eleicao:
            if self.votos_recebidos >= QUORUM:
                print("\n[Eleição] Eleito como líder!")
                self.VirarLider()
            else:
                print(f"\n[Eleição] Não obteve votos suficientes: {self.votos_recebidos}/{QUORUM}")

    async def FetchLoop(self):
        '''Réplica buscar novos dados, confirmar o recebimento de logs e avisar que está viva (heartbeat)'''
        print("\n[♡] Heartbeat iniciou")
        exibir_estado(self.estado_atual, self.termo_atual, self.indice_log, self.indice_commit, self.lider_id)
        
        while True:
            await asyncio.sleep(TEMPO_FETCH)

            try:
                if self.estado_atual == estado.seguidor and self.lider_id is not None:
                    response = await self.nos[self.lider_id].Fetch(
                        no_pb2.FetchRequest(
                            id=self.id,
                            termo=self.termo_atual,
                            indice_log=self.indice_log
                        ))
                    
                    # Resposta com termo maior
                    if self.VerificarTermo(response.id, response.termo):
                        continue
                    
                    # Líder ativo se resposta aceita
                    if response.sucesso and response.termo == self.termo_atual:
                        # Sinaliza que o líder está ativo
                        self.sucesso_fetch.set()
                        print(f"\n[♡] Líder ativo: {self.lider_id}")
                        
                        # Busca ativamente entradas no líder atual
                        for log in response.logs:
                            log_local = bd.get_log_entry(self.id, log.indice)
                            # Verifica possíveis conflitos
                            if log_local is not None:
                                if log_local.termo != log.termo or log_local.dado != log.dado:
                                    if log.indice <= self.indice_commit:
                                        raise Exception("\nDivergência em entrada já comitada!")
                                    print(
                                        f"\n[RESSINCRONIZAÇÃO] Conflito no índice {log.indice}"
                                        f"\n- Log local: termo = {log_local.termo}, dado = {log_local.dado}"
                                        f"\n- Log líder: termo = {log.termo}, dado = {log.dado}\n"
                                    )
                                
                                    # Trunca a partir do índice conflitante
                                    bd.truncate_logs_from(self.id, log.indice)
                                    print(f"\n[RESSINCRONIZAÇÃO] Trucamento e correção de log -> log locais a partir do índice {log.indice} removidos\n")
                                
                                    # Adiciona logs corretos do líder
                                    bd.insert_log_entries(self.id, [log])
                            
                            else:
                                # Adiciona logs que ainda não foram registrados do líder
                                bd.insert_log_entries(self.id, [log])
                        
                        
                        # Verifica se há entradas locais excedentes que não existem mais no log do líder
                        ultimo_log_lider = response.indice_log
                        ultimo_log_local = bd.get_last_log_index(self.id)
                        
                        if ultimo_log_local > ultimo_log_lider:
                            primeiro_excedente = ultimo_log_lider + 1
                            if primeiro_excedente <= self.indice_commit:
                                raise Exception("O líder possui log menor que o índice já comitado localmente!")

                            bd.truncate_logs_from(self.id, primeiro_excedente)

                            print(
                                f"\n[RECONCILIAÇÃO] Removendo entradas excedentes"
                                f"\n- Primeiro índice removido: {primeiro_excedente}"
                                f"\n- Último índice do líder: {ultimo_log_lider}\n"
                            )
                        
                        # Atualiza índice local
                        self.indice_log = bd.get_last_log_index(self.id)
                        
                        # Comita logs quando confirmado
                        if response.commit_index > self.indice_commit:
                            bd.commit_logs(self.id, response.commit_index)
                            self.indice_commit = bd.get_last_commit_index(self.id)

                    # Atualizar logs
                    exibir_estado(self.estado_atual, self.termo_atual, self.indice_log, self.indice_commit, self.lider_id)
            
            except grpc.RpcError as e:
                if (e.code() == grpc.StatusCode.UNAVAILABLE):
                    print(f"\nErro ao enviar heartbeat para o líder {self.lider_id}: indisponível")
                else:
                    print(f"\nErro ao enviar heartbeat para o líder {self.lider_id}: {e}")

    async def EleicaoLoop(self):
        while True:
            # Temporizadores de eleição aleatórios -> evitar que enviem ao mesmo tempo
            timeout = ELEICAO_TIMEOUT + random.uniform(0, 0.5)

            try:
                # Aguarda o timeout, reinicia o temporizador se receber heartbeat do líder
                await asyncio.wait_for(self.sucesso_fetch.wait(), timeout=timeout)
                # Se recebeu resposta válida do líder, reinicia o temporizador
                self.sucesso_fetch.clear()
            
            except asyncio.TimeoutError:
                if self.estado_atual != estado.lider:
                    # Se o temporizador expirar, inicia a eleição
                    await self.IniciarEleicao()
                    exibir_estado(self.estado_atual, self.termo_atual, self.indice_log, self.indice_commit, self.lider_id)
    
    def IniciarFetchLoop(self):
        if self.fetch_task is None or self.fetch_task.done():
            self.fetch_task = asyncio.create_task(self.FetchLoop())

    def EncerrarFetchLoop(self):
        if self.fetch_task and not self.fetch_task.done():
            self.fetch_task.cancel()
    
    def IniciarEleicaoLoop(self):
        if self.eleicao_task is None or self.eleicao_task.done():
            self.sucesso_fetch.clear()
            self.eleicao_task = asyncio.create_task(self.EleicaoLoop())

    def EncerrarEleicaoLoop(self):
        if self.eleicao_task and not self.eleicao_task.done():
            self.eleicao_task.cancel()

    def VerificarTermo(self, id, termo):
        '''Verificação do termo. Retorna True se houve atualização'''
        if termo > self.termo_atual:
            print(f"\nTermo maior do nó {id}. Voltando a ser seguidor.")
            self.VirarSeguidor(termo, id)
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
            nos_ativos = 1 + len(self.ReplicasAtivas())
            return nos_ativos >= QUORUM
        return False
    
    async def MonitorarReplicasLoop(self):
        '''Monitoramento de inatividade das réplicas pelo líder'''
        print("\n[MONITOR] Monitoramento de réplicas iniciado")

        while self.estado_atual == estado.lider:
            await asyncio.sleep(TEMPO_FETCH)
            replicas_ativas = set(self.ReplicasAtivas())
            replicas_inativas = set(self.nos.keys()) - replicas_ativas

            # Exibe somente quando houver alteração
            if replicas_inativas != self.replicas_inativas:
                self.replicas_inativas = replicas_inativas
                print(f"\nRéplicas ativas: {sorted(replicas_ativas)}")
                print(f"Réplicas inativas: {sorted(replicas_inativas)}\n")

                if not self.TemQuorum():
                    print("\n[ALERTA] Líder sem quórum!\n")

    def IniciarMonitoramentoLoop(self):
        if self.monitor_task is None or self.monitor_task.done():
            self.monitor_task = asyncio.create_task(self.MonitorarReplicasLoop())
    
    def EncerrarMonitoramentoLoop(self):
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
    
    
    # Operações com o cliente =====================================================================
    async def ClientWrite(self, request, context):
        
        # Apenas o lider pode realizar escrita de dados
        if self.estado_atual != estado.lider:
            print("\n[ClientWrite] Não sou o líder!")
            return no_pb2.WriteResponse(
                sucesso = False,
                lider_id = self.lider_id if self.lider_id is not None else -1,
                mensagem = f"falha ao gravar dado -> o lider atual é {self.lider_id}"
            )
        
        # Verifica o Quorum
        if not self.TemQuorum():
            print("\n[ClientWrite] Não há quorum suficiente, rejeitando")
            return no_pb2.WriteResponse(
                sucesso = False,
                lider_id = self.lider_id,
                mensagem = "falha ao gravar dado -> não há quorum suficiente"
            )
        
        # Cria nova entrada no log local (não comitada)
        try:
            bd.insert_log_entries(
                self.id,
                [
                    no_pb2.Log(
                        indice = self.indice_log + 1,
                        termo = self.termo_atual,
                        dado = request.dado
                    )
                ]
            )
            self.indice_log += 1
            print(f"\n[ClientWrite] Novo log: \n- índice log = {self.indice_log}\n- termo = {self.termo_atual}\n- dado = {request.dado}")
            
            if request.crash:
                print(f"\n[ClientWrite] CRASH!!!")
                exit(1)
        
        except Exception as e:
            print(f"\n[ClientWrite] Erro ao adicionar novo log: {e}")
            return no_pb2.WriteResponse(
                sucesso = False,
                lider_id = self.id,
                mensagem = "falha ao persistir nova entrada do log"
            )
        
        # Aguarda confirmação das réplicas
        inicio = asyncio.get_running_loop().time()
        timeout = ELEICAO_TIMEOUT
        
        while asyncio.get_running_loop().time() - inicio < timeout:
            
            # Nó pode perder a liderança
            if self.estado_atual != estado.lider:
                print("\n[ClientWrite] Não sou o líder!")
                return no_pb2.WriteResponse(
                    sucesso = False,
                    lider_id = self.lider_id if self.lider_id is not None else -1,
                    mensagem = f"falha ao gravar dado -> liderança perdida"
                )
            
            # Conta somente réplicas ativas que armazenaram a entrada
            replicas_confirmadas = sum(1 for id in self.ReplicasAtivas() if self.indices_replicas.get(id, 0) >= self.indice_log)
            nos_confirmados = 1 + replicas_confirmadas # Lider também conta no quorum
            
            if nos_confirmados >= QUORUM:
                # Avança índice de commit apenas ao obter confirmação do quórum
                self.indice_commit = self.indice_log
                # Persiste e aplica entradas comitadas no banco final
                if bd.commit_logs(self.id, self.indice_commit):
                    print(f"\n[ClientWrite] Commit realizado!\n- indice de commit = {self.indice_commit}\n- nós confirmados = {nos_confirmados}/{QTD_NOS}")
                    return no_pb2.WriteResponse(
                        sucesso = True,
                        lider_id = self.lider_id,
                        mensagem = f"dado gravado com sucesso! índice = {self.indice_commit}"
                    )
                else:
                    return no_pb2.WriteResponse(
                        sucesso = False,
                        lider_id = self.lider_id if self.lider_id is not None else -1,
                        mensagem = f"falha ao gravar dado"
                    )
            
            # Aguarda novos FetchRequests
            await asyncio.sleep(0.05)
        
        # Timeout: entrada fica no log, mas não é comitada
        print(f"\n[ClientWrite] Timeout aguardando quorum\n- índice pendente = {self.indice_log}\n- confirmações = {nos_confirmados}/{QUORUM}")
        return no_pb2.WriteResponse(
            sucesso = False,
            lider_id = self.id,
            mensagem = f"falha ao gravar dado -> timeour aguardando confirmação do quorum"
        )
    
    
    async def ClientRead(self, request, context):
        '''Responde a consultas do cliente utilizando apenas dados comitados'''
        try:
            # Se commit específico solicitado
            commit_index = request.commit_index
            if 0 < commit_index <= bd.get_last_commit_index(self.id):
                commit = bd.get_commit(self.id, commit_index)
                if commit is not None:
                    print(f"\n[ClientRead] Enviando commit {commit_index}: \n- índice = {commit.indice}\n- termo = {commit.termo}\n- dado = {commit.dado!r}")
                    return no_pb2.ReadResponse(
                        sucesso = True,
                        commit_index = commit.indice,
                        termo = commit.termo,
                        dado = commit.dado,
                    )
            
            # Se índice inválido, retorna o último commit
            last_commit = bd.get_last_commit(self.id)
            print(f"\n[ClientRead] Enviando último commit: \n- índice = {last_commit.indice}\n- termo = {last_commit.termo}\n- dado = {last_commit.dado!r}")
            return no_pb2.ReadResponse(
                sucesso = True,
                commit_index = last_commit.indice,
                termo = last_commit.termo,
                dado = last_commit.dado,
            )
        
        except Exception as e:
            print(f"\n[ClientRead] Erro ao ler último commit: {e}")
            return no_pb2.ReadResponse(
                sucesso = False,
                commit_index = 0,
                termo = 0,
                dado = ""
            )


def exibir_estado(estado_atual, termo_atual, indice_log, indice_commit, lider=None):
    print(f"===============================")
    print(time.strftime("%H:%M:%S", time.localtime()))
    print(f"Papel: {estado_atual}")
    print(f"Termo: {termo_atual}")
    print(f"Índice de Log: {indice_log}")
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